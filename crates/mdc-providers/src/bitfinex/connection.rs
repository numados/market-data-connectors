//! Low-level WebSocket connection actor, channel dispatching, and self-healing resync loop for Bitfinex.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::MdcError;
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, RunId, Symbol, SymbolTag, UnixNanos, VenueId,
};
use mdc_core::sink::{BookKind, EventContext, RawLevelText, StreamingSink};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::bitfinex::checksum::{BitfinexChecksumBook, ChecksumVerdict};
use crate::bitfinex::config::BitfinexConfig;
use crate::bitfinex::control::Command;
use crate::bitfinex::fsm::{ChannelFsm, PollOutcome};
use crate::bitfinex::normalize::{normalize_book_level, normalize_trade};
use crate::bitfinex::raw::{BitfinexConnectionScratch, RawChannelFrame, RawFrame, RawPayload};

/// Builds the subscription JSON string for a given channel and symbol.
fn build_book_subscribe(symbol: &str, depth: usize) -> String {
    serde_json::json!({
        "event": "subscribe",
        "channel": "book",
        "symbol": symbol,
        "prec": "P0",
        "freq": "F0",
        "len": depth
    })
    .to_string()
}

/// Builds the unsubscribe JSON string for a channel ID.
fn build_unsubscribe(chan_id: u32) -> String {
    serde_json::json!({
        "event": "unsubscribe",
        "chanId": chan_id
    })
    .to_string()
}

/// Builds the trades channel subscription JSON string.
fn build_trades_subscribe(symbol: &str) -> String {
    serde_json::json!({
        "event": "subscribe",
        "channel": "trades",
        "symbol": symbol
    })
    .to_string()
}

/// Runs the Bitfinex connection worker loop with automatic reconnect, heartbeat monitoring,
/// and channel-id lifecycle handling.
pub async fn run_connection_loop<S: StreamingSink>(
    config: BitfinexConfig,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) -> Result<(), MdcError> {
    let run_id = RunId::from_bytes([0x62, 0x66, 0x78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut connection_id: u32 = 0;
    let mut delivery_ordinal: u64 = 0;
    let mut reconnect_attempt: u32 = 0;

    let symbol_list = config.symbols.clone();
    let tag_map = config.symbol_tags.clone();

    let mut fsms: HashMap<Symbol, ChannelFsm> = HashMap::new();
    let mut chan_to_symbol: HashMap<u32, Symbol> = HashMap::new();
    let mut symbol_to_chan: HashMap<Symbol, u32> = HashMap::new();
    let mut books: HashMap<u32, BitfinexChecksumBook> = HashMap::new();
    let mut scratch = BitfinexConnectionScratch::new();

    'reconnect_loop: loop {
        if cancel.is_cancelled() {
            break 'reconnect_loop;
        }

        if reconnect_attempt > 0 {
            let backoff_factor = 2u64.saturating_pow(reconnect_attempt.saturating_sub(1));
            let delay = config
                .reconnect_backoff_initial
                .saturating_mul(backoff_factor as u32)
                .min(config.reconnect_backoff_max);
            info!(
                connection_id,
                attempt = reconnect_attempt,
                delay_ms = delay.as_millis(),
                "Backing off before Bitfinex reconnect"
            );

            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Resume) => {
                            reconnect_attempt = 0;
                        }
                        Some(Command::Pause) => {
                            // Wait in pause until Resume
                            'paused_wait: loop {
                                tokio::select! {
                                    _ = cancel.cancelled() => break 'reconnect_loop,
                                    resume_cmd = command_rx.recv() => {
                                        if matches!(resume_cmd, Some(Command::Resume)) {
                                            reconnect_attempt = 0;
                                            break 'paused_wait;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }

        connection_id = connection_id.wrapping_add(1);
        reconnect_attempt = reconnect_attempt.saturating_add(1);

        let ws_url = match url::Url::parse(&config.ws_url) {
            Ok(u) => u,
            Err(e) => {
                error!(connection_id, error = %e, "Invalid Bitfinex WebSocket URL");
                return Err(MdcError::Transport(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    e.to_string(),
                ))));
            }
        };

        info!(connection_id, url = %ws_url, "Connecting to Bitfinex WebSocket v2");

        let connect_res = tokio_tungstenite::connect_async(ws_url.as_str()).await;
        let (mut ws_stream, _) = match connect_res {
            Ok(pair) => pair,
            Err(e) => {
                warn!(connection_id, error = %e, "Failed to connect to Bitfinex WebSocket");
                continue 'reconnect_loop;
            }
        };

        info!(connection_id, "Connected to Bitfinex WebSocket v2");
        reconnect_attempt = 0;

        let now = Instant::now();
        let mut watchdog = SilenceWatchdog::new(
            config.watchdog_ping_after,
            config.watchdog_reconnect_after,
            now,
        );

        // Clear all connection-scoped maps
        chan_to_symbol.clear();
        symbol_to_chan.clear();
        books.clear();
        fsms.clear();
        scratch.clear();

        // Initialize FSMs and emit ResyncStarted for all configured symbols
        for sym in &symbol_list {
            fsms.insert(
                sym.clone(),
                ChannelFsm::new(now, config.resubscribe_timeout, config.quarantine_duration),
            );

            let tag = *tag_map.get(sym).unwrap_or(&0);
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Bitfinex,
                symbol: sym,
                tag,
                exchange_ts: None,
                received_ts: crate::now_nanos(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, u64::from(connection_id), delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
        }

        // Send conf handshake message: TIMESTAMP (32768) + OB_CHECKSUM (131072) = 163840
        const CONF_FLAGS: u64 = 163840;
        let conf_msg = serde_json::json!({
            "event": "conf",
            "flags": CONF_FLAGS
        })
        .to_string();

        if let Err(e) = ws_stream.send(Message::Text(conf_msg)).await {
            warn!(connection_id, error = %e, "Failed to send conf handshake message");
            continue 'reconnect_loop;
        }

        // Send subscribe requests for all symbols
        for sym in &symbol_list {
            if config.streams.depth {
                let sub_msg = build_book_subscribe(sym.as_str(), config.orderbook_depth);
                if let Err(e) = ws_stream.send(Message::Text(sub_msg)).await {
                    warn!(connection_id, %sym, error = %e, "Failed to send book subscribe message");
                    continue 'reconnect_loop;
                }
            }
            if config.streams.trades {
                let trade_msg = build_trades_subscribe(sym.as_str());
                if let Err(e) = ws_stream.send(Message::Text(trade_msg)).await {
                    warn!(connection_id, %sym, error = %e, "Failed to send trades subscribe message");
                    continue 'reconnect_loop;
                }
            }
        }

        let mut timer = tokio::time::interval(Duration::from_millis(100));
        let mut pending_resubscribe: HashSet<Symbol> = HashSet::new();

        'message_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(connection_id, "Bitfinex connection worker cancelled");
                    let _ = ws_stream.close(None).await;
                    break 'reconnect_loop;
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Pause) => {
                            info!(connection_id, "Pausing Bitfinex connector");
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitfinex,
                                    symbol: sym,
                                    tag,
                                    exchange_ts: None,
                                    received_ts: crate::now_nanos(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(
                                        run_id,
                                        u64::from(connection_id),
                                        delivery_ordinal,
                                    ),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(
                                    &ctx,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                );
                            }
                            let _ = ws_stream.close(None).await;

                            // Wait for resume command or cancellation
                            loop {
                                tokio::select! {
                                    _ = cancel.cancelled() => break 'reconnect_loop,
                                    resume_cmd = command_rx.recv() => {
                                        if matches!(resume_cmd, Some(Command::Resume)) {
                                            info!(connection_id, "Resuming Bitfinex connector");
                                            continue 'reconnect_loop;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested Bitfinex reconnect");
                            let _ = ws_stream.close(None).await;
                            break 'message_loop;
                        }
                        Some(Command::RequestResync(sym_opt)) => {
                            if let Some(target_sym) = sym_opt {
                                pending_resubscribe.insert(target_sym);
                            } else {
                                for sym in &symbol_list {
                                    pending_resubscribe.insert(sym.clone());
                                }
                            }
                        }
                        Some(Command::Resume) => {
                            // Already active
                        }
                        None => {
                            // Control channel closed
                        }
                    }
                }
                _ = timer.tick() => {
                    let now = Instant::now();

                    // Check silence watchdog
                    match watchdog.poll(now) {
                        WatchdogAction::SendPing => {
                            trace!(connection_id, "Silence watchdog sending WebSocket Ping");
                            if let Err(e) = ws_stream.send(Message::Ping(vec![])).await {
                                warn!(connection_id, error = %e, "Failed to send WS Ping");
                                break 'message_loop;
                            }
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            warn!(
                                connection_id,
                                ?silent_for,
                                "Silence watchdog triggered reconnect"
                            );
                            break 'message_loop;
                        }
                        WatchdogAction::Healthy => {}
                    }

                    // Check channel leak guard: if active channels > 2x configured symbols, force reconnect
                    if !symbol_list.is_empty() && chan_to_symbol.len() > symbol_list.len() * 2 {
                        warn!(
                            connection_id,
                            channels = chan_to_symbol.len(),
                            expected = symbol_list.len(),
                            "Channel leak detected: active channels exceeded 2x symbols, reconnecting"
                        );
                        break 'message_loop;
                    }

                    // Poll FSM timers per symbol
                    for sym in &symbol_list {
                        if let Some(fsm) = fsms.get_mut(sym) {
                            match fsm.poll(now) {
                                PollOutcome::TimeoutQuarantined { until } => {
                                    warn!(connection_id, %sym, ?until, "Symbol snapshot timed out, entering quarantine");
                                    let tag = *tag_map.get(sym).unwrap_or(&0);
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::Bitfinex,
                                        symbol: sym,
                                        tag,
                                        exchange_ts: None,
                                        received_ts: crate::now_nanos(),
                                        connection_id,
                                        delivery: DeliveryStamp::new(
                                            run_id,
                                            u64::from(connection_id),
                                            delivery_ordinal,
                                        ),
                                        source_sequence: None,
                                    };
                                    let _ = sink.alert(
                                        &ctx,
                                        IntegrityAlert::Quarantined {
                                            retry_in_ms: 30_000,
                                        },
                                    );
                                }
                                PollOutcome::QuarantineExpiredTriggerResubscribe => {
                                    info!(connection_id, %sym, "Quarantine expired, re-attempting subscribe");
                                    pending_resubscribe.insert(sym.clone());
                                }
                                PollOutcome::None => {}
                            }
                        }
                    }

                    // Flush pending resubscriptions
                    if !pending_resubscribe.is_empty() {
                        let to_resubscribe: Vec<Symbol> = pending_resubscribe.drain().collect();
                        for sym in to_resubscribe {
                            if let Some(old_chan_id) = symbol_to_chan.get(&sym).copied() {
                                let un_msg = build_unsubscribe(old_chan_id);
                                if let Err(e) = ws_stream.send(Message::Text(un_msg)).await {
                                    warn!(connection_id, %sym, error = %e, "Failed to send unsubscribe during resync; reconnecting");
                                    break 'message_loop;
                                }
                            }

                            if let Some(fsm) = fsms.get_mut(&sym) {
                                fsm.reset_awaiting_snapshot(now);
                            }

                            let tag = *tag_map.get(&sym).unwrap_or(&0);
                            delivery_ordinal += 1;
                            let ctx = EventContext {
                                venue: VenueId::Bitfinex,
                                symbol: &sym,
                                tag,
                                exchange_ts: None,
                                received_ts: crate::now_nanos(),
                                connection_id,
                                delivery: DeliveryStamp::new(
                                    run_id,
                                    u64::from(connection_id),
                                    delivery_ordinal,
                                ),
                                source_sequence: None,
                            };
                            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);

                            let sub_msg = build_book_subscribe(sym.as_str(), config.orderbook_depth);
                            if let Err(e) = ws_stream.send(Message::Text(sub_msg)).await {
                                warn!(connection_id, %sym, error = %e, "Failed to send subscribe during resync; reconnecting");
                                break 'message_loop;
                            }
                        }
                    }
                }
                msg_opt = ws_stream.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "Bitfinex WebSocket error");
                            break 'message_loop;
                        }
                        None => {
                            info!(connection_id, "Bitfinex WebSocket closed by remote server");
                            break 'message_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let received_ts = crate::now_nanos();
                            watchdog.on_data_frame(Instant::now());

                            if let Err(should_reconnect) = handle_frame_text(
                                &text,
                                received_ts,
                                connection_id,
                                run_id,
                                &mut delivery_ordinal,
                                &symbol_list,
                                &tag_map,
                                &mut fsms,
                                &mut chan_to_symbol,
                                &mut symbol_to_chan,
                                &mut books,
                                &mut pending_resubscribe,
                                config.orderbook_depth,
                                config.streams.trades,
                                &mut ws_stream,
                                &mut sink,
                            ).await {
                                if should_reconnect {
                                    break 'message_loop;
                                }
                            }
                        }
                        Message::Binary(bin) => {
                            let received_ts = crate::now_nanos();
                            watchdog.on_data_frame(Instant::now());

                            if let Ok(text) = std::str::from_utf8(&bin) {
                                if let Err(should_reconnect) = handle_frame_text(
                                    text,
                                    received_ts,
                                    connection_id,
                                    run_id,
                                    &mut delivery_ordinal,
                                    &symbol_list,
                                    &tag_map,
                                    &mut fsms,
                                    &mut chan_to_symbol,
                                    &mut symbol_to_chan,
                                    &mut books,
                                    &mut pending_resubscribe,
                                    config.orderbook_depth,
                                    config.streams.trades,
                                    &mut ws_stream,
                                    &mut sink,
                                ).await {
                                    if should_reconnect {
                                        break 'message_loop;
                                    }
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_stream.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(_) => {
                            // Pong does not reset data frame silence watchdog
                        }
                        Message::Close(frame) => {
                            info!(connection_id, ?frame, "Received Close frame from Bitfinex");
                            break 'message_loop;
                        }
                        _ => {}
                    }
                }
            }
        }

        // On socket disconnection, emit ConnectionLost for all active symbols
        for sym in &symbol_list {
            let tag = *tag_map.get(sym).unwrap_or(&0);
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Bitfinex,
                symbol: sym,
                tag,
                exchange_ts: None,
                received_ts: crate::now_nanos(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, u64::from(connection_id), delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(
                &ctx,
                IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::TransportError,
                },
            );
        }
    }

    Ok(())
}

/// Dispatches an incoming text message frame to events, channels, or book updates.
/// Returns `Err(true)` if the connection loop should immediately break and reconnect.
#[allow(clippy::too_many_arguments)]
async fn handle_frame_text<S: StreamingSink, W: SinkExt<Message> + Unpin>(
    text: &str,
    received_ts: UnixNanos,
    connection_id: u32,
    run_id: RunId,
    delivery_ordinal: &mut u64,
    symbol_list: &[Symbol],
    tag_map: &HashMap<Symbol, SymbolTag>,
    fsms: &mut HashMap<Symbol, ChannelFsm>,
    chan_to_symbol: &mut HashMap<u32, Symbol>,
    symbol_to_chan: &mut HashMap<Symbol, u32>,
    books: &mut HashMap<u32, BitfinexChecksumBook>,
    _pending_resubscribe: &mut HashSet<Symbol>,
    orderbook_depth: usize,
    trades_enabled: bool,
    ws_stream: &mut W,
    sink: &mut S,
) -> Result<(), bool> {
    let frame = match RawFrame::parse(text) {
        Ok(f) => f,
        Err(e) => {
            debug!(connection_id, error = %e, raw = %text, "Failed to parse Bitfinex frame; skipping");
            return Ok(());
        }
    };

    match frame {
        RawFrame::Event(event) => match event {
            crate::bitfinex::raw::RawEvent::Info { version, server_id } => {
                info!(connection_id, ?version, ?server_id, "Bitfinex server info");
            }
            crate::bitfinex::raw::RawEvent::Conf { status, flags } => {
                info!(connection_id, ?status, ?flags, "Bitfinex conf response");
            }
            crate::bitfinex::raw::RawEvent::Subscribed {
                channel,
                chan_id,
                symbol,
                pair,
                ..
            } => {
                let resolved_sym = symbol
                    .or(pair)
                    .and_then(|raw_sym| {
                        symbol_list.iter().find(|s| {
                            s.as_str() == raw_sym
                                || s.as_str().trim_start_matches('t') == raw_sym
                                || raw_sym.trim_start_matches('t') == s.as_str()
                        })
                    })
                    .cloned();

                if let Some(sym) = resolved_sym {
                    info!(connection_id, chan_id, channel, %sym, "Subscribed to channel");
                    chan_to_symbol.insert(chan_id, sym.clone());

                    if channel == "book" {
                        symbol_to_chan.insert(sym.clone(), chan_id);
                        books.insert(chan_id, BitfinexChecksumBook::new(orderbook_depth.min(25)));
                    }
                } else {
                    warn!(
                        connection_id,
                        chan_id,
                        channel,
                        ?symbol,
                        ?pair,
                        "Subscribed to unknown symbol; unsubscribing"
                    );
                    let un_msg = build_unsubscribe(chan_id);
                    let _ = ws_stream.send(Message::Text(un_msg)).await;
                }
            }
            crate::bitfinex::raw::RawEvent::Unsubscribed { chan_id, status } => {
                info!(connection_id, chan_id, ?status, "Unsubscribed from channel");
                chan_to_symbol.remove(&chan_id);
                books.remove(&chan_id);
            }
            crate::bitfinex::raw::RawEvent::Error { msg, code } => {
                warn!(connection_id, code, msg, "Bitfinex error notification");
            }
            crate::bitfinex::raw::RawEvent::Other => {}
        },
        RawFrame::Channel(RawChannelFrame {
            chan_id,
            payload,
            ts,
        }) => {
            let sym = match chan_to_symbol.get(&chan_id) {
                Some(s) => s.clone(),
                None => {
                    trace!(
                        connection_id,
                        chan_id, "Received channel data for unmapped channel"
                    );
                    return Ok(());
                }
            };

            let tag = *tag_map.get(&sym).unwrap_or(&0);
            let exchange_ts = ts.map(UnixNanos::from_millis);

            match payload {
                RawPayload::Heartbeat => {
                    // Already counted towards silence watchdog
                }
                RawPayload::Checksum(cs) => {
                    if let Some(book) = books.get_mut(&chan_id) {
                        let verdict = book.verify(cs);
                        match verdict {
                            ChecksumVerdict::Valid => {
                                trace!(connection_id, chan_id, %sym, cs, "Checksum verified valid");
                            }
                            ChecksumVerdict::Mismatch {
                                expected,
                                calculated,
                            } => {
                                warn!(
                                    connection_id,
                                    chan_id,
                                    %sym,
                                    expected,
                                    calculated,
                                    "Bitfinex CRC32 checksum mismatch; triggering channel resubscribe"
                                );

                                *delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitfinex,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts,
                                    received_ts,
                                    connection_id,
                                    delivery: DeliveryStamp::new(
                                        run_id,
                                        u64::from(connection_id),
                                        *delivery_ordinal,
                                    ),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(
                                    &ctx,
                                    IntegrityAlert::ChecksumMismatch {
                                        expected: expected as u32,
                                        calculated: calculated as u32,
                                    },
                                );

                                if let Some(fsm) = fsms.get_mut(&sym) {
                                    fsm.on_checksum_mismatch(Instant::now());
                                }

                                *delivery_ordinal += 1;
                                let ctx_resync = EventContext {
                                    venue: VenueId::Bitfinex,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts,
                                    received_ts,
                                    connection_id,
                                    delivery: DeliveryStamp::new(
                                        run_id,
                                        u64::from(connection_id),
                                        *delivery_ordinal,
                                    ),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx_resync, IntegrityAlert::ResyncStarted);

                                // Resubscribe channel in-place
                                let un_msg = build_unsubscribe(chan_id);
                                if ws_stream.send(Message::Text(un_msg)).await.is_err() {
                                    return Err(true);
                                }
                                let sub_msg = build_book_subscribe(sym.as_str(), orderbook_depth);
                                if ws_stream.send(Message::Text(sub_msg)).await.is_err() {
                                    return Err(true);
                                }
                            }
                        }
                    }
                }
                RawPayload::BookSnapshot(levels) => {
                    if let Some(book) = books.get_mut(&chan_id) {
                        book.clear();
                        for l in &levels {
                            let _ = book.apply(l);
                        }

                        if let Some(fsm) = fsms.get_mut(&sym) {
                            fsm.on_snapshot();
                        }

                        *delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Bitfinex,
                            symbol: &sym,
                            tag,
                            exchange_ts,
                            received_ts,
                            connection_id,
                            delivery: DeliveryStamp::new(
                                run_id,
                                u64::from(connection_id),
                                *delivery_ordinal,
                            ),
                            source_sequence: None,
                        };

                        let _ = sink.alert(
                            &ctx,
                            IntegrityAlert::ResyncCompleted {
                                last_update_id: None,
                            },
                        );

                        if sink.begin_book(&ctx, BookKind::Snapshot).is_ok() {
                            for l in &levels {
                                if let Ok((side, pl)) = normalize_book_level(l) {
                                    let raw_text = RawLevelText {
                                        price: l.price_str(),
                                        volume: l.amount_str(),
                                    };
                                    let _ = sink.level_raw(side, pl.price, pl.volume, raw_text);
                                }
                            }
                            let _ = sink.end_book(&ctx, None);
                        }
                    }
                }
                RawPayload::BookUpdate(level) => {
                    if let Some(book) = books.get_mut(&chan_id) {
                        let _ = book.apply(&level);
                    }

                    let is_in_sync = fsms.get(&sym).is_some_and(|f| f.is_in_sync());
                    if is_in_sync {
                        *delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Bitfinex,
                            symbol: &sym,
                            tag,
                            exchange_ts,
                            received_ts,
                            connection_id,
                            delivery: DeliveryStamp::new(
                                run_id,
                                u64::from(connection_id),
                                *delivery_ordinal,
                            ),
                            source_sequence: None,
                        };

                        if sink.begin_book(&ctx, BookKind::Delta).is_ok() {
                            if let Ok((side, pl)) = normalize_book_level(&level) {
                                let raw_text = RawLevelText {
                                    price: level.price_str(),
                                    volume: level.amount_str(),
                                };
                                let _ = sink.level_raw(side, pl.price, pl.volume, raw_text);
                            }
                            let _ = sink.end_book(&ctx, None);
                        }
                    } else {
                        trace!(connection_id, %sym, "Dropping book update while not in sync");
                    }
                }
                RawPayload::Trade { trade, .. } => {
                    if trades_enabled {
                        if let Ok(t) = normalize_trade(&trade, received_ts) {
                            *delivery_ordinal += 1;
                            let ctx = EventContext {
                                venue: VenueId::Bitfinex,
                                symbol: &sym,
                                tag,
                                exchange_ts,
                                received_ts,
                                connection_id,
                                delivery: DeliveryStamp::new(
                                    run_id,
                                    u64::from(connection_id),
                                    *delivery_ordinal,
                                ),
                                source_sequence: None,
                            };
                            let _ = sink.trade(&ctx, &t);
                        }
                    }
                }
                RawPayload::TradeSnapshot { trades, .. } => {
                    if trades_enabled {
                        for trade in &trades {
                            if let Ok(t) = normalize_trade(trade, received_ts) {
                                *delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitfinex,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts,
                                    received_ts,
                                    connection_id,
                                    delivery: DeliveryStamp::new(
                                        run_id,
                                        u64::from(connection_id),
                                        *delivery_ordinal,
                                    ),
                                    source_sequence: None,
                                };
                                let _ = sink.trade(&ctx, &t);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
