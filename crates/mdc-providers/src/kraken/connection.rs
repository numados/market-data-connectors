//! Kraken WebSocket per-bucket connection worker task.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::{
    BookKind, ChecksumPolicy, DisconnectKind, IntegrityAlert,
    PriceLevel, RawLevelText, RunId, Side, SilenceWatchdog, StreamingSink, Symbol, SymbolTag,
    UnixNanos, VenueId, WatchdogAction,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::sink_util::ConnectionSink;

use crate::kraken::checksum::{ChecksumVerdict, KrakenChecksumBook};
use crate::kraken::config::KrakenConfig;
use crate::kraken::control::Command;
use crate::kraken::fsm::{PollOutcome, SnapshotOutcome, SymbolFsm, UpdateOutcome};
use crate::kraken::normalize::{normalize_trade, parse_rfc3339_nanos};
use crate::kraken::raw::{RawBookData, RawLevel, RawMessage, RawMethodAck};

/// Chunks a slice of symbols into batches of at most `batch_size`.
pub fn chunk_symbols(symbols: &[Symbol], batch_size: usize) -> Vec<Vec<String>> {
    if batch_size == 0 {
        return Vec::new();
    }
    symbols
        .chunks(batch_size)
        .map(|chunk| chunk.iter().map(|s| s.as_str().to_owned()).collect())
        .collect()
}

/// Builds the subscribe JSON message for a batch of symbols and channel.
pub fn build_subscribe_payload(symbols: &[String], channel: &str, depth: usize) -> String {
    if channel == "book" {
        serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": symbols,
                "depth": depth
            }
        })
        .to_string()
    } else {
        serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": channel,
                "symbol": symbols
            }
        })
        .to_string()
    }
}

/// Builds the unsubscribe JSON message for a batch of symbols and channel.
pub fn build_unsubscribe_payload(symbols: &[String], channel: &str, depth: usize) -> String {
    if channel == "book" {
        serde_json::json!({
            "method": "unsubscribe",
            "params": {
                "channel": "book",
                "symbol": symbols,
                "depth": depth
            }
        })
        .to_string()
    } else {
        serde_json::json!({
            "method": "unsubscribe",
            "params": {
                "channel": channel,
                "symbol": symbols
            }
        })
        .to_string()
    }
}

/// Runs a single Kraken WebSocket connection task managing up to `symbols_per_connection` symbols.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: KrakenConfig,
    sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    run_id: RunId,
) {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id);

    let tag_map: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();
    let symbol_list: Vec<Symbol> = symbols.iter().map(|(s, _)| s.clone()).collect();

    let mut fsms: HashMap<Symbol, SymbolFsm> = HashMap::new();
    let mut books: HashMap<Symbol, KrakenChecksumBook> = HashMap::new();

    let mut reconnect_attempt = 0u32;
    let mut is_paused = false;

    // Pre-allocated scratch levels to avoid heap allocation on updates
    let mut bids_scratch: Vec<PriceLevel> = Vec::with_capacity(config.depth);
    let mut asks_scratch: Vec<PriceLevel> = Vec::with_capacity(config.depth);

    'reconnect_loop: loop {
        if cancel.is_cancelled() || sink.is_failed() {
            break 'reconnect_loop;
        }

        while is_paused {
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    if let Some(Command::Resume) = cmd {
                        is_paused = false;
                        break;
                    }
                }
            }
        }

        sink.next_generation();

        if reconnect_attempt > 0 {
            let mult = 1u32.checked_shl(reconnect_attempt.min(4) - 1).unwrap_or(16);
            let backoff = (config.initial_backoff * mult).min(config.max_backoff);
            info!(
                connection_id,
                ?backoff,
                reconnect_attempt,
                "Reconnecting Kraken WebSocket with backoff"
            );
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Pause) => { is_paused = true; continue 'reconnect_loop; }
                        Some(Command::Reconnect(_)) => { reconnect_attempt = 0; }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        reconnect_attempt += 1;
        let ws_url = config.ws_base_url.clone();
        info!(connection_id, url = %ws_url, "Connecting to Kraken WebSocket v2");

        let connect_res = tokio_tungstenite::connect_async(ws_url.as_str()).await;
        let (mut ws_stream, _) = match connect_res {
            Ok(pair) => pair,
            Err(e) => {
                warn!(connection_id, error = %e, "Failed to connect to Kraken WebSocket");
                continue 'reconnect_loop;
            }
        };

        info!(connection_id, "Connected to Kraken WebSocket v2");
        reconnect_attempt = 0;

        let now = Instant::now();
        let mut watchdog = SilenceWatchdog::new(
            config.watchdog_ping_after,
            config.watchdog_reconnect_after,
            now,
        );

        // Reset FSMs and books, emit ResyncStarted for all symbols
        fsms.clear();
        books.clear();
        for sym in &symbol_list {
            fsms.insert(
                sym.clone(),
                SymbolFsm::new(now, config.resubscribe_timeout, config.quarantine_duration),
            );
            books.insert(sym.clone(), KrakenChecksumBook::new(config.depth));

            let tag = *tag_map.get(sym).unwrap_or(&0);
            if sink
                .emit_alert(
                    VenueId::Kraken,
                    sym,
                    tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ResyncStarted,
                )
                .is_err()
            {
                break 'reconnect_loop;
            }
        }

        // Send initial subscriptions chunked at resubscribe_batch_size
        let chunks = chunk_symbols(&symbol_list, config.resubscribe_batch_size);
        for chunk in &chunks {
            if config.streams.depth {
                let sub_payload = build_subscribe_payload(chunk, "book", config.depth);
                if let Err(e) = ws_stream.send(Message::Text(sub_payload)).await {
                    warn!(connection_id, error = %e, "Failed to send book subscribe message");
                    continue 'reconnect_loop;
                }
            }
            if config.streams.trade {
                let trade_payload = build_subscribe_payload(chunk, "trade", config.depth);
                if let Err(e) = ws_stream.send(Message::Text(trade_payload)).await {
                    warn!(connection_id, error = %e, "Failed to send trade subscribe message");
                    continue 'reconnect_loop;
                }
            }
        }

        let mut timer = tokio::time::interval(Duration::from_millis(100));
        let mut pending_resubscribe: HashSet<Symbol> = HashSet::new();

        'message_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(connection_id, "Kraken connection worker cancelled");
                    let _ = ws_stream.close(None).await;
                    break 'reconnect_loop;
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Pause) => {
                            info!(connection_id, "Pausing Kraken connector");
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                let _ = sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                );
                            }
                            let _ = ws_stream.close(None).await;
                            is_paused = true;
                            continue 'reconnect_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        Some(Command::Reconnect(reason)) => {
                            info!(connection_id, %reason, "Operator forced reconnect");
                            let _ = ws_stream.close(None).await;
                            continue 'reconnect_loop;
                        }
                        Some(Command::Resync(symbol)) => {
                            if let Some(fsm) = fsms.get_mut(&symbol) {
                                fsm.request_resync(Instant::now());
                                pending_resubscribe.insert(symbol.clone());
                                let tag = *tag_map.get(&symbol).unwrap_or(&0);
                                if sink
                                    .emit_alert(
                                        VenueId::Kraken,
                                        &symbol,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ResyncStarted,
                                    )
                                    .is_err()
                                {
                                    break 'message_loop;
                                }
                            }
                        }
                        Some(Command::ResyncAll) => {
                            let now = Instant::now();
                            for sym in &symbol_list {
                                if let Some(fsm) = fsms.get_mut(sym) {
                                    fsm.request_resync(now);
                                    pending_resubscribe.insert(sym.clone());
                                    let tag = *tag_map.get(sym).unwrap_or(&0);
                                    if sink
                                        .emit_alert(
                                            VenueId::Kraken,
                                            sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            IntegrityAlert::ResyncStarted,
                                        )
                                        .is_err()
                                    {
                                        break 'message_loop;
                                    }
                                }
                            }
                        }
                        None => {
                            break 'reconnect_loop;
                        }
                    }
                }
                _ = timer.tick() => {
                    let now = Instant::now();

                    // Check silence watchdog
                    match watchdog.poll(now) {
                        WatchdogAction::SendPing => {
                            debug!(connection_id, "Watchdog sending ping");
                            if let Err(e) = ws_stream.send(Message::Ping(vec![])).await {
                                warn!(connection_id, error = %e, "Failed to send watchdog ping");
                                break 'message_loop;
                            }
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            warn!(connection_id, ?silent_for, "Silence watchdog timeout; forcing reconnect");
                            let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                let _ = sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::SilenceTimeout { elapsed_ms },
                                );
                                let _ = sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout,
                                    },
                                );
                            }
                            break 'message_loop;
                        }
                        WatchdogAction::Healthy => {}
                    }

                    // Poll FSM timers for each symbol
                    for (sym, fsm) in &mut fsms {
                        match fsm.poll(now) {
                            PollOutcome::TimeoutQuarantined { .. } => {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                if sink
                                    .emit_alert(
                                        VenueId::Kraken,
                                        sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::Quarantined {
                                            retry_in_ms: config.quarantine_duration.as_millis().min(u64::MAX as u128) as u64,
                                        },
                                    )
                                    .is_err()
                                {
                                    break 'message_loop;
                                }
                            }
                            PollOutcome::QuarantineExpiredTriggerResubscribe => {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                if sink
                                    .emit_alert(
                                        VenueId::Kraken,
                                        sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ResyncStarted,
                                    )
                                    .is_err()
                                {
                                    break 'message_loop;
                                }
                                pending_resubscribe.insert(sym.clone());
                            }
                            PollOutcome::None => {}
                        }
                    }

                    // Flush pending resubscriptions in batches
                    if !pending_resubscribe.is_empty() {
                        let to_resubscribe: Vec<Symbol> = pending_resubscribe.drain().collect();
                        let batches = chunk_symbols(&to_resubscribe, config.resubscribe_batch_size);
                        for batch in batches {
                            let un_payload = build_unsubscribe_payload(&batch, "book", config.depth);
                            let sub_payload = build_subscribe_payload(&batch, "book", config.depth);
                            if let Err(e) = ws_stream.send(Message::Text(un_payload)).await {
                                warn!(connection_id, error = %e, "Failed to send batch unsubscribe");
                                break 'message_loop;
                            }
                            if let Err(e) = ws_stream.send(Message::Text(sub_payload)).await {
                                warn!(connection_id, error = %e, "Failed to send batch subscribe");
                                break 'message_loop;
                            }
                        }
                    }
                }
                msg_opt = ws_stream.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "WebSocket error");
                            break 'message_loop;
                        }
                        None => {
                            info!(connection_id, "WebSocket stream closed by remote server");
                            break 'message_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let received_ts = crate::now_nanos();
                            watchdog.on_data_frame(Instant::now());

                            handle_frame_text(
                                &text,
                                config.checksum_policy,
                                received_ts,
                                connection_id,
                                &tag_map,
                                &mut fsms,
                                &mut books,
                                &mut pending_resubscribe,
                                &mut bids_scratch,
                                &mut asks_scratch,
                                &mut sink,
                            );
                        }
                        Message::Binary(bin) => {
                            let received_ts = crate::now_nanos();
                            watchdog.on_data_frame(Instant::now());

                            if let Ok(text) = std::str::from_utf8(&bin) {
                                handle_frame_text(
                                    text,
                                    config.checksum_policy,
                                    received_ts,
                                    connection_id,
                                    &tag_map,
                                    &mut fsms,
                                    &mut books,
                                    &mut pending_resubscribe,
                                    &mut bids_scratch,
                                    &mut asks_scratch,
                                    &mut sink,
                                );
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_stream.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(_) => {
                            // Inbound pong does not reset silence watchdog by architectural invariant
                        }
                        Message::Close(frame) => {
                            info!(connection_id, ?frame, "Received Close frame from Kraken");
                            break 'message_loop;
                        }
                        _ => {}
                    }
                }
            }

            if sink.is_failed() {
                break 'reconnect_loop;
            }
        }

        // On socket drop, notify embedder of connection loss for all active symbols if sink not already failed
        if !sink.is_failed() {
            for sym in &symbol_list {
                let tag = *tag_map.get(sym).unwrap_or(&0);
                let _ = sink.emit_alert(
                    VenueId::Kraken,
                    sym,
                    tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ConnectionLost {
                        reason: DisconnectKind::TransportError,
                    },
                );
            }
        }
    }
}

/// Dispatches a single incoming JSON frame across canonical streams and handlers.
#[allow(clippy::too_many_arguments)]
fn handle_frame_text<S: StreamingSink>(
    text: &str,
    checksum_policy: ChecksumPolicy,
    received_ts: UnixNanos,
    connection_id: u32,
    tag_map: &HashMap<Symbol, SymbolTag>,
    fsms: &mut HashMap<Symbol, SymbolFsm>,
    books: &mut HashMap<Symbol, KrakenChecksumBook>,
    pending_resubscribe: &mut HashSet<Symbol>,
    bids_scratch: &mut Vec<PriceLevel>,
    asks_scratch: &mut Vec<PriceLevel>,
    sink: &mut ConnectionSink<S>,
) {
    let raw_msg = match RawMessage::parse(text) {
        Ok(m) => m,
        Err(_) => {
            // Check for method ack e.g. {"method":"subscribe","success":false,...}
            if let Ok(ack) = serde_json::from_str::<RawMethodAck<'_>>(text) {
                if !ack.success {
                    let sym_str = ack
                        .symbol
                        .or_else(|| ack.result.as_ref().and_then(|r| r.symbol));
                    if let Some(s) = sym_str {
                        let sym = Symbol::from(s);
                        warn!(connection_id, symbol = s, error = ?ack.error, "Kraken subscription rejected");
                        if let Some(fsm) = fsms.get_mut(&sym) {
                            fsm.poll(Instant::now() + Duration::from_secs(10));
                        }
                    }
                }
            }
            return;
        }
    };

    match raw_msg {
        RawMessage::Book { kind, data } => {
            for book_data in data {
                let symbol = Symbol::from(book_data.symbol);
                let tag = *tag_map.get(&symbol).unwrap_or(&0);

                let fsm = match fsms.get_mut(&symbol) {
                    Some(f) => f,
                    None => continue,
                };
                let book = match books.get_mut(&symbol) {
                    Some(b) => b,
                    None => continue,
                };

                if kind == "snapshot" {
                    handle_snapshot(
                        &book_data,
                        checksum_policy,
                        &symbol,
                        tag,
                        received_ts,
                        fsm,
                        book,
                        pending_resubscribe,
                        sink,
                    );
                } else if kind == "update" {
                    handle_update(
                        &book_data,
                        checksum_policy,
                        &symbol,
                        tag,
                        received_ts,
                        fsm,
                        book,
                        pending_resubscribe,
                        bids_scratch,
                        asks_scratch,
                        sink,
                    );
                }
            }
        }
        RawMessage::Trade { data } => {
            for raw_trade in data {
                let symbol = Symbol::from(raw_trade.symbol);
                let tag = *tag_map.get(&symbol).unwrap_or(&0);
                if let Ok((exchange_ts, trade)) = normalize_trade(&raw_trade) {
                    let ctx = sink.next_context(
                        VenueId::Kraken,
                        &symbol,
                        tag,
                        Some(exchange_ts),
                        received_ts,
                        None,
                    );
                    if sink.trade(&ctx, &trade).is_err() {
                        return;
                    }
                }
            }
        }
        RawMessage::Heartbeat | RawMessage::Status | RawMessage::Other => {}
    }
}

/// Handles snapshot messages for a symbol.
#[allow(clippy::too_many_arguments)]
fn handle_snapshot<S: StreamingSink>(
    data: &RawBookData<'_>,
    checksum_policy: ChecksumPolicy,
    symbol: &Symbol,
    tag: SymbolTag,
    received_ts: UnixNanos,
    fsm: &mut SymbolFsm,
    book: &mut KrakenChecksumBook,
    pending_resubscribe: &mut HashSet<Symbol>,
    sink: &mut ConnectionSink<S>,
) {
    book.clear();
    if book.apply(&data.asks, &data.bids).is_err() {
        return;
    }

    let is_valid = match book.verify(data.checksum) {
        ChecksumVerdict::Valid => true,
        ChecksumVerdict::Mismatch {
            expected,
            calculated,
        } => {
            warn!(
                symbol = symbol.as_str(),
                expected, calculated, "Kraken snapshot checksum mismatch"
            );
            false
        }
        ChecksumVerdict::NotProvided => match checksum_policy {
            ChecksumPolicy::Required => {
                warn!(
                    symbol = symbol.as_str(),
                    "Missing checksum on Kraken snapshot under Required policy; failing closed"
                );
                false
            }
            ChecksumPolicy::Optional => true,
        },
    };

    match fsm.on_snapshot(is_valid) {
        SnapshotOutcome::Accepted => {
            pending_resubscribe.remove(symbol);
            let ctx = sink.next_context(VenueId::Kraken, symbol, tag, None, received_ts, None);

            if sink.begin_book(&ctx, BookKind::Snapshot).is_err() {
                return;
            }
            for lvl in book.top(Side::Bid, book.depth()) {
                if sink.level(Side::Bid, lvl.price, lvl.volume).is_err() {
                    return;
                }
            }
            for lvl in book.top(Side::Ask, book.depth()) {
                if sink.level(Side::Ask, lvl.price, lvl.volume).is_err() {
                    return;
                }
            }
            if sink.end_book(&ctx, None).is_err() {
                return;
            }
            let _ = sink.emit_alert(
                VenueId::Kraken,
                symbol,
                tag,
                None,
                crate::now_nanos(),
                None,
                IntegrityAlert::ResyncCompleted {
                    last_update_id: None,
                },
            );
        }
        SnapshotOutcome::RejectedBadChecksum => {
            if let Some(expected) = data.checksum {
                let _ = sink.emit_alert(
                    VenueId::Kraken,
                    symbol,
                    tag,
                    None,
                    received_ts,
                    None,
                    IntegrityAlert::ChecksumMismatch {
                        expected,
                        calculated: book.checksum(),
                    },
                );
            }
        }
    }
}

/// Handles incremental book update messages for a symbol.
#[allow(clippy::too_many_arguments)]
fn handle_update<S: StreamingSink>(
    data: &RawBookData<'_>,
    checksum_policy: ChecksumPolicy,
    symbol: &Symbol,
    tag: SymbolTag,
    received_ts: UnixNanos,
    fsm: &mut SymbolFsm,
    book: &mut KrakenChecksumBook,
    pending_resubscribe: &mut HashSet<Symbol>,
    bids_scratch: &mut Vec<PriceLevel>,
    asks_scratch: &mut Vec<PriceLevel>,
    sink: &mut ConnectionSink<S>,
) {
    if !fsm.is_in_sync() {
        return;
    }

    if book.apply(&data.asks, &data.bids).is_err() {
        return;
    }

    let verdict = book.verify(data.checksum);
    let (is_valid, expected, calculated) = match verdict {
        ChecksumVerdict::Valid => (true, 0, 0),
        ChecksumVerdict::Mismatch {
            expected,
            calculated,
        } => (false, expected, calculated),
        ChecksumVerdict::NotProvided => match checksum_policy {
            ChecksumPolicy::Required => {
                warn!(
                    symbol = symbol.as_str(),
                    "Missing checksum on Kraken book update under Required policy; failing closed"
                );
                (false, 0, book.checksum())
            }
            ChecksumPolicy::Optional => (true, 0, 0),
        },
    };

    let now = Instant::now();
    match fsm.on_update(is_valid, expected, calculated, now) {
        UpdateOutcome::Accepted => {
            let exchange_ts = data.timestamp.and_then(|ts| parse_rfc3339_nanos(ts).ok());
            let ctx =
                sink.next_context(VenueId::Kraken, symbol, tag, exchange_ts, received_ts, None);

            bids_scratch.clear();
            populate_scratch_levels(&data.bids, bids_scratch);

            asks_scratch.clear();
            populate_scratch_levels(&data.asks, asks_scratch);

            if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                return;
            }
            for (lvl, raw) in bids_scratch.iter().zip(&data.bids) {
                if sink
                    .level_raw(
                        Side::Bid,
                        lvl.price,
                        lvl.volume,
                        RawLevelText {
                            price: raw.price.get(),
                            volume: raw.qty.get(),
                        },
                    )
                    .is_err()
                {
                    return;
                }
            }
            for (lvl, raw) in asks_scratch.iter().zip(&data.asks) {
                if sink
                    .level_raw(
                        Side::Ask,
                        lvl.price,
                        lvl.volume,
                        RawLevelText {
                            price: raw.price.get(),
                            volume: raw.qty.get(),
                        },
                    )
                    .is_err()
                {
                    return;
                }
            }
            let _ = sink.end_book(&ctx, None);
        }
        UpdateOutcome::MismatchTriggerResync {
            expected,
            calculated,
        } => {
            warn!(
                symbol = symbol.as_str(),
                expected, calculated, "Kraken checksum mismatch; resubscribing"
            );
            let _ = sink.emit_alert(
                VenueId::Kraken,
                symbol,
                tag,
                None,
                received_ts,
                None,
                IntegrityAlert::ChecksumMismatch {
                    expected,
                    calculated,
                },
            );
            let _ = sink.emit_alert(
                VenueId::Kraken,
                symbol,
                tag,
                None,
                received_ts,
                None,
                IntegrityAlert::ResyncStarted,
            );
            pending_resubscribe.insert(symbol.clone());
        }
        UpdateOutcome::DroppedNotSynchronized => {}
    }
}

/// Helper that converts parsed `RawLevel`s into reusable scratch price levels.
fn populate_scratch_levels(raws: &[RawLevel<'_>], out: &mut Vec<PriceLevel>) {
    for raw in raws {
        if let (Ok(p), Ok(v)) = (
            rust_decimal::Decimal::from_str_exact(raw.price.get()),
            rust_decimal::Decimal::from_str_exact(raw.qty.get()),
        ) {
            out.push(PriceLevel::new(p, v));
        }
    }
}
