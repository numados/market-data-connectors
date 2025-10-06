//! Bitstamp WebSocket per-connection worker task and protocol loop.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, RunId, Side, Symbol, SymbolTag, UnixNanos,
    VenueId,
};
use mdc_core::resync::ResyncFailure;
use mdc_core::sink::{BookKind, EventContext, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::bitstamp::config::BitstampConfig;
use crate::bitstamp::control::Command;
use crate::bitstamp::fsm::{DiffDecision, SymbolFsm, SyncState};
use crate::bitstamp::normalize::normalize_trade;
use crate::bitstamp::raw::{
    BitstampConnectionScratch, BitstampDiffFrameSeed, RawEnvelope, RawTrade, SubscribeMessage,
};
use crate::bitstamp::rest::{BitstampRestClient, RestError};

/// Builds the channel name for diff order book updates (e.g. `"diff_order_book_btcusd"`).
pub fn build_diff_order_book_channel(symbol: &str) -> String {
    format!("diff_order_book_{}", symbol.to_lowercase())
}

/// Builds the channel name for public live trades (e.g. `"live_trades_btcusd"`).
pub fn build_live_trades_channel(symbol: &str) -> String {
    format!("live_trades_{}", symbol.to_lowercase())
}

/// Builds a JSON subscribe message frame for a target channel.
pub fn build_subscribe_frame(channel: &str) -> String {
    let req = SubscribeMessage::new(channel);
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Runs a single Bitstamp WebSocket connection worker managing a slice of symbols.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: BitstampConfig,
    rest_client: BitstampRestClient,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0xb1, 0x75, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let mut fsms: HashMap<Symbol, SymbolFsm> = symbols
        .iter()
        .map(|(s, _)| {
            (
                s.clone(),
                SymbolFsm::new(s.clone(), config.snapshot_retry_cooldown),
            )
        })
        .collect();

    let mut scratch = BitstampConnectionScratch::with_capacity(512);

    // Fast lookup maps for symbol matching
    let mut channel_to_symbol: HashMap<String, Symbol> = HashMap::new();
    let symbol_tags: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();

    for (s, _) in &symbols {
        let sym_lower = s.as_str().to_lowercase();
        if config.streams.depth {
            channel_to_symbol.insert(format!("diff_order_book_{sym_lower}"), s.clone());
        }
        if config.streams.trades {
            channel_to_symbol.insert(format!("live_trades_{sym_lower}"), s.clone());
        }
    }

    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(10);

    'reconnect_loop: loop {
        if cancel.is_cancelled() {
            break 'reconnect_loop;
        }

        info!(
            connection_id,
            url = %config.ws_url,
            "Connecting to Bitstamp WebSocket"
        );

        let ws_stream = match connect_async(config.ws_url.as_str()).await {
            Ok((stream, _resp)) => {
                info!(connection_id, "Connected to Bitstamp WebSocket");
                backoff = Duration::from_millis(500);
                stream
            }
            Err(e) => {
                warn!(
                    connection_id,
                    error = %e,
                    retry_in = ?backoff,
                    "Failed to connect to Bitstamp WebSocket"
                );
                tokio::select! {
                    _ = cancel.cancelled() => break 'reconnect_loop,
                    _ = tokio::time::sleep(backoff) => {
                        backoff = (backoff * 2).min(max_backoff);
                        continue 'reconnect_loop;
                    }
                }
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Subscribe to channels and initialize symbols to AwaitingSnapshot
        let mut snapshot_queue: VecDeque<Symbol> = VecDeque::new();

        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Bitstamp,
                symbol: sym,
                tag: *tag,
                exchange_ts: None,
                received_ts: UnixNanos::now(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                source_sequence: None,
            };

            if let Some(fsm) = fsms.get_mut(sym) {
                fsm.trigger_resync(Instant::now());
            }

            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);

            let sym_lower = sym.as_str().to_lowercase();

            if config.streams.depth {
                let channel = build_diff_order_book_channel(&sym_lower);
                let frame = build_subscribe_frame(&channel);
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        connection_id,
                        symbol = %sym,
                        error = %e,
                        "Failed to send Bitstamp diff_order_book subscribe frame"
                    );
                }
                snapshot_queue.push_back(sym.clone());
            }

            if config.streams.trades {
                let channel = build_live_trades_channel(&sym_lower);
                let frame = build_subscribe_frame(&channel);
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        connection_id,
                        symbol = %sym,
                        error = %e,
                        "Failed to send Bitstamp live_trades subscribe frame"
                    );
                }
            }
        }

        // Connection watchdog: Bitstamp has no ping frames; timeout after 45s of silence
        let now_inst = Instant::now();
        let mut watchdog =
            SilenceWatchdog::new(Duration::from_secs(30), Duration::from_secs(45), now_inst);
        let mut watchdog_ticker = tokio::time::interval(Duration::from_secs(1));
        watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // At most one snapshot in flight per connection
        let mut snapshot_task: Option<
            tokio::task::JoinHandle<(
                Symbol,
                Result<crate::bitstamp::raw::RawOrderBookSnapshot, RestError>,
            )>,
        > = None;

        'frame_loop: loop {
            // Check if we can dispatch a pending snapshot request
            if snapshot_task.is_none() && !snapshot_queue.is_empty() {
                let now = Instant::now();
                let mut found_idx = None;

                for (idx, sym) in snapshot_queue.iter().enumerate() {
                    if let Some(fsm) = fsms.get_mut(sym) {
                        if fsm.can_request_snapshot(now) {
                            found_idx = Some(idx);
                            break;
                        }
                    }
                }

                if let Some(idx) = found_idx {
                    if let Some(sym) = snapshot_queue.remove(idx) {
                        if let Some(fsm) = fsms.get_mut(&sym) {
                            fsm.mark_snapshot_requested(now);
                        }

                        let client = rest_client.clone();
                        let sym_clone = sym.clone();
                        let pair_lower = sym.as_str().to_lowercase();

                        snapshot_task = Some(tokio::spawn(async move {
                            let res = client.fetch_order_book(&pair_lower, Instant::now()).await;
                            (sym_clone, res)
                        }));
                    }
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => {
                    for (sym, tag) in &symbols {
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Bitstamp,
                            symbol: sym,
                            tag: *tag,
                            exchange_ts: None,
                            received_ts: UnixNanos::now(),
                            connection_id,
                            delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                            source_sequence: None,
                        };
                        let _ = sink.alert(
                            &ctx,
                            IntegrityAlert::ConnectionLost {
                                reason: DisconnectKind::Shutdown,
                            },
                        );
                    }
                    let _ = ws_tx.close().await;
                    break 'reconnect_loop;
                }

                // Silence watchdog ticks
                _ = watchdog_ticker.tick() => {
                    let now = Instant::now();
                    match watchdog.poll(now) {
                        WatchdogAction::Healthy | WatchdogAction::SendPing => {}
                        WatchdogAction::Reconnect { .. } => {
                            warn!(
                                connection_id,
                                "Silence watchdog timeout on Bitstamp feed; forcing reconnect"
                            );
                            break 'frame_loop;
                        }
                    }
                }

                // Operator commands
                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::ResyncSymbol(sym)) => {
                            if let Some(fsm) = fsms.get_mut(&sym) {
                                let now = Instant::now();
                                fsm.trigger_resync(now);

                                let tag = symbol_tags.get(&sym).copied().unwrap_or_default();
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitstamp,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);

                                if !snapshot_queue.contains(&sym) {
                                    snapshot_queue.push_back(sym);
                                }
                            }
                        }
                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            info!(connection_id, "Operator paused Bitstamp connection");
                            for (sym, tag) in &symbols {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitstamp,
                                    symbol: sym,
                                    tag: *tag,
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(
                                    &ctx,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                );
                            }
                            // Wait for resume
                            while let Some(c) = command_rx.recv().await {
                                if c == Command::Resume {
                                    break;
                                }
                            }
                        }
                        Some(Command::Resume) => {}
                        None => break 'frame_loop,
                    }
                }

                // In-flight REST snapshot completion
                Some(task_res) = async {
                    match &mut snapshot_task {
                        Some(handle) => Some(handle.await),
                        None => std::future::pending().await,
                    }
                } => {
                    snapshot_task = None;
                    let now = Instant::now();

                    if let Ok((sym, res)) = task_res {
                        match res {
                            Ok(snap) => {
                                let tag = symbol_tags.get(&sym).copied().unwrap_or_default();
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Bitstamp,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts: Some(UnixNanos::from_micros(snap.microtimestamp)),
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };

                                let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                for level in &snap.bids {
                                    let _ = sink.level(Side::Bid, level[0], level[1]);
                                }
                                for level in &snap.asks {
                                    let _ = sink.level(Side::Ask, level[0], level[1]);
                                }
                                let _ = sink.end_book(&ctx, None);

                                let _ = sink.alert(
                                    &ctx,
                                    IntegrityAlert::ResyncCompleted {
                                        last_update_id: None,
                                    },
                                );

                                if let Some(fsm) = fsms.get_mut(&sym) {
                                    fsm.on_snapshot(snap.microtimestamp, now);
                                }
                            }
                            Err(e) => {
                                warn!(
                                    connection_id,
                                    symbol = %sym,
                                    error = %e,
                                    "Failed to fetch Bitstamp REST snapshot"
                                );
                                let failure = match &e {
                                    RestError::RateLimited(status) | RestError::HttpStatus(status) => {
                                        ResyncFailure::HttpError(*status)
                                    }
                                    RestError::Transport(s) => ResyncFailure::Transport(s.clone()),
                                    RestError::Parse(s) => ResyncFailure::Parse(s.clone()),
                                    RestError::Embargoed(_) => ResyncFailure::HttpError(429),
                                };

                                if let Some(fsm) = fsms.get_mut(&sym) {
                                    fsm.on_snapshot_failed(&failure, now);
                                }

                                if !snapshot_queue.contains(&sym) {
                                    snapshot_queue.push_back(sym);
                                }
                            }
                        }
                    }
                }

                // WebSocket frames
                msg_opt = ws_rx.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "Bitstamp WebSocket stream error");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "Bitstamp WebSocket connection closed by peer");
                            break 'frame_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let now = Instant::now();
                            watchdog.on_data_frame(now);

                            // Quick check for server request reconnect
                            if text.contains("bts:request_reconnect") {
                                info!(
                                    connection_id,
                                    "Bitstamp server requested reconnect (bts:request_reconnect)"
                                );
                                for (sym, tag) in &symbols {
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::Bitstamp,
                                        symbol: sym,
                                        tag: *tag,
                                        exchange_ts: None,
                                        received_ts: UnixNanos::now(),
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: None,
                                    };
                                    let _ = sink.alert(
                                        &ctx,
                                        IntegrityAlert::ConnectionLost {
                                            reason: DisconnectKind::VenueRequested,
                                        },
                                    );
                                }
                                break 'frame_loop;
                            }

                            if text.contains("bts:subscription_succeeded") {
                                debug!(connection_id, "Bitstamp subscription confirmed");
                                continue;
                            }

                            // Try parsing top-level envelope
                            let env: RawEnvelope<'_> = match serde_json::from_str(&text) {
                                Ok(e) => e,
                                Err(_) => continue,
                            };

                            if env.event == "data" {
                                let sym_opt = channel_to_symbol.get(env.channel);
                                if let Some(sym) = sym_opt {
                                    if let Some(fsm) = fsms.get_mut(sym) {
                                        // Zero-allocation single-pass parse into scratch
                                        let mut de = serde_json::Deserializer::from_str(&text);
                                        let header = match BitstampDiffFrameSeed::new(&mut scratch).deserialize(&mut de) {
                                            Ok(h) => h,
                                            Err(e) => {
                                                warn!(
                                                    connection_id,
                                                    symbol = %sym,
                                                    error = %e,
                                                    "Failed to parse Bitstamp diff_order_book frame"
                                                );
                                                continue;
                                            }
                                        };

                                        match fsm.check_diff(header.microtimestamp) {
                                            DiffDecision::Emit => {
                                                let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                delivery_ordinal += 1;
                                                let ctx = EventContext {
                                                    venue: VenueId::Bitstamp,
                                                    symbol: sym,
                                                    tag,
                                                    exchange_ts: Some(UnixNanos::from_micros(header.microtimestamp)),
                                                    received_ts: UnixNanos::now(),
                                                    connection_id,
                                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                    source_sequence: None,
                                                };

                                                let _ = sink.begin_book(&ctx, BookKind::Delta);
                                                for level in &scratch.bids {
                                                    let _ = sink.level(Side::Bid, level.price, level.volume);
                                                }
                                                for level in &scratch.asks {
                                                    let _ = sink.level(Side::Ask, level.price, level.volume);
                                                }
                                                let _ = sink.end_book(&ctx, None);
                                            }
                                            DiffDecision::Drop => {
                                                if fsm.state() == SyncState::AwaitingSnapshot
                                                    && !snapshot_queue.contains(sym)
                                                {
                                                    snapshot_queue.push_back(sym.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            } else if env.event == "trade" && config.streams.trades {
                                if let Some(sym) = channel_to_symbol.get(env.channel) {
                                    if let Some(data_val) = env.data {
                                        if let Ok(raw_trade) = serde_json::from_str::<RawTrade>(data_val.get()) {
                                            if let Ok((_trade_ts, canonical_trade)) = normalize_trade(&raw_trade) {
                                                let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                delivery_ordinal += 1;
                                                let ctx = EventContext {
                                                    venue: VenueId::Bitstamp,
                                                    symbol: sym,
                                                    tag,
                                                    exchange_ts: Some(UnixNanos::from_micros(raw_trade.microtimestamp)),
                                                    received_ts: UnixNanos::now(),
                                                    connection_id,
                                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                    source_sequence: None,
                                                };
                                                let _ = sink.trade(&ctx, &canonical_trade);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_tx.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            info!(connection_id, "Bitstamp close frame received");
                            break 'frame_loop;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
