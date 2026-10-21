//! Kraken WebSocket per-bucket connection worker task.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::resync::EscalatingRejectionPolicy;
use mdc_core::{
    BackoffPolicy, BookKind, ChecksumPolicy, DisconnectKind, ExponentialBackoff, IntegrityAlert,
    PriceLevel, RawLevelText, RunId, Side, SilenceWatchdog, SourceErrorKind, StreamingSink, Symbol,
    SymbolTag, UnixNanos, VenueId, WatchdogAction,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::sink_util::ConnectionSink;

use crate::kraken::checksum::{ChecksumVerdict, KrakenChecksumBook};
use crate::kraken::config::KrakenConfig;
use crate::kraken::control::Command;
use crate::kraken::fsm::{
    PollOutcome, RejectionOutcome, SnapshotOutcome, SymbolFsm, UpdateOutcome,
};
use crate::kraken::normalize::{normalize_trade, parse_rfc3339_nanos};
use crate::kraken::raw::{RawBookData, RawLevel, RawMessage};

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
    generation_allocator: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> Result<(), crate::sink_util::SinkError> {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id)
        .with_generation_allocator(generation_allocator);

    let tag_map: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();
    let symbol_list: Vec<Symbol> = symbols.iter().map(|(s, _)| s.clone()).collect();

    let mut fsms: HashMap<Symbol, SymbolFsm> = HashMap::new();
    let mut books: HashMap<Symbol, KrakenChecksumBook> = HashMap::new();

    let mut reconnect_attempt = 0u32;
    let mut backoff_policy = ExponentialBackoff::new(config.initial_backoff, config.max_backoff);
    let mut is_paused = false;
    let mut deferred_commands: VecDeque<Command> = VecDeque::new();

    // Pre-allocated scratch levels to avoid heap allocation on updates
    let mut bids_scratch: Vec<PriceLevel> = Vec::with_capacity(config.depth);
    let mut asks_scratch: Vec<PriceLevel> = Vec::with_capacity(config.depth);
    let mut evicted_bids: Vec<Decimal> = Vec::with_capacity(16);
    let mut evicted_asks: Vec<Decimal> = Vec::with_capacity(16);

    'reconnect_loop: loop {
        if cancel.is_cancelled() || sink.is_failed() {
            break 'reconnect_loop;
        }

        while is_paused {
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Resume) => {
                            is_paused = false;
                            break;
                        }
                        Some(_) => {}
                        None => break 'reconnect_loop,
                    }
                }
            }
        }

        sink.next_generation();

        if reconnect_attempt > 0 {
            let backoff = backoff_policy.next_delay(reconnect_attempt.saturating_sub(1));
            info!(
                connection_id,
                ?backoff,
                reconnect_attempt,
                "Reconnecting Kraken WebSocket with backoff"
            );
            match wait_backoff(backoff, &mut command_rx, &mut deferred_commands, &cancel).await {
                BackoffExit::Elapsed => {}
                BackoffExit::Reconnect => {
                    reconnect_attempt = 0;
                    backoff_policy.reset();
                }
                BackoffExit::Paused => {
                    is_paused = true;
                    continue 'reconnect_loop;
                }
                BackoffExit::Stopped => break 'reconnect_loop,
            }
        }

        reconnect_attempt += 1;
        let ws_url = config.ws_base_url.clone();
        info!(connection_id, url = %ws_url, "Connecting to Kraken WebSocket v2");

        let connect_res = tokio::select! {
            _ = cancel.cancelled() => break 'reconnect_loop,
            res = crate::ws_util::connect_ws(ws_url.as_str(), config.connection_timeout) => res,
        };
        let (mut ws_stream, _) = match connect_res {
            Ok(pair) => pair,
            Err(e) => {
                warn!(connection_id, error = %e, "Failed to connect to Kraken WebSocket");
                continue 'reconnect_loop;
            }
        };

        info!(connection_id, "Connected to Kraken WebSocket v2");
        reconnect_attempt = 0;
        backoff_policy.reset();

        let now = Instant::now();
        let mut watchdog = SilenceWatchdog::new(
            config.watchdog_ping_after,
            config.watchdog_reconnect_after,
            now,
        );
        let mut disconnect_emitted = false;

        // Reset FSMs and books, emit ResyncStarted for all symbols
        fsms.clear();
        books.clear();
        let checksum_strategy = config
            .checksum_strategy
            .clone()
            .unwrap_or_else(|| std::sync::Arc::new(mdc_core::integrity::KrakenChecksum::new(10)));
        for sym in &symbol_list {
            fsms.insert(
                sym.clone(),
                SymbolFsm::with_rejection_policy(
                    sym.clone(),
                    now,
                    config.resubscribe_timeout,
                    config.quarantine_duration,
                    match &config.rejection_policy {
                        Some(factory) => factory(),
                        None => Box::new(EscalatingRejectionPolicy::new(
                            config.rejection_cooldown,
                            config.quarantine_duration,
                            config.rejection_escalate_after,
                        )),
                    },
                ),
            );
            books.insert(
                sym.clone(),
                KrakenChecksumBook::with_strategy(
                    config.depth,
                    std::sync::Arc::clone(&checksum_strategy),
                ),
            );

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
                cmd = next_command(&mut deferred_commands, &mut command_rx) => {
                    match cmd {
                        Some(Command::Pause) => {
                            info!(connection_id, "Pausing Kraken connector");
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                if sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                ).is_err() {
                                    break 'message_loop;
                                }
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
                            disconnect_emitted = true;
                            let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                if sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::SilenceTimeout { elapsed_ms },
                                ).is_err() {
                                    break 'message_loop;
                                }
                                if sink.emit_alert(
                                    VenueId::Kraken,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout,
                                    },
                                ).is_err() {
                                    break 'message_loop;
                                }
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
                                &mut evicted_bids,
                                &mut evicted_asks,
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
                                    &mut evicted_bids,
                                    &mut evicted_asks,
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
        if !sink.is_failed() && !disconnect_emitted {
            for sym in &symbol_list {
                let tag = *tag_map.get(sym).unwrap_or(&0);
                sink.best_effort_emit_alert(
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

    if sink.is_failed() {
        Err(crate::sink_util::SinkError)
    } else {
        Ok(())
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
    evicted_bids: &mut Vec<Decimal>,
    evicted_asks: &mut Vec<Decimal>,
    sink: &mut ConnectionSink<S>,
) {
    let raw_msg = match RawMessage::parse(text) {
        Ok(m) => m,
        Err(_) => return,
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
                        evicted_bids,
                        evicted_asks,
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
        RawMessage::MethodAck(ack) => {
            if !ack.success {
                let sym_str = ack
                    .symbol
                    .or_else(|| ack.result.as_ref().and_then(|r| r.symbol));
                if let Some(s) = sym_str {
                    let sym = Symbol::from(s);
                    let tag = *tag_map.get(&sym).unwrap_or(&0);
                    warn!(
                        connection_id,
                        symbol = s,
                        error = ?ack.error,
                        "Kraken subscription rejected"
                    );
                    let now = Instant::now();
                    let quarantine_retry_ms = match fsms.get_mut(&sym) {
                        Some(fsm) => rejection_retry_ms(fsm.on_subscription_rejected(now), now),
                        None => Some(30_000),
                    };
                    pending_resubscribe.remove(&sym);
                    let ctx =
                        sink.next_context(VenueId::Kraken, &sym, tag, None, received_ts, None);
                    if sink
                        .alert(
                            &ctx,
                            IntegrityAlert::SourceError {
                                kind: SourceErrorKind::Parse,
                            },
                        )
                        .is_err()
                    {
                        return;
                    }
                    if let Some(retry_in_ms) = quarantine_retry_ms {
                        if sink
                            .alert(&ctx, IntegrityAlert::Quarantined { retry_in_ms })
                            .is_err()
                        {
                            return;
                        }
                    }
                } else {
                    warn!(
                        connection_id,
                        error = ?ack.error,
                        "Kraken general subscription rejected; applying rejection policy to all active symbols"
                    );
                    let now = Instant::now();
                    for (sym, fsm) in fsms.iter_mut() {
                        let quarantine_retry_ms =
                            rejection_retry_ms(fsm.on_subscription_rejected(now), now);
                        pending_resubscribe.remove(sym);
                        let tag = *tag_map.get(sym).unwrap_or(&0);
                        let ctx =
                            sink.next_context(VenueId::Kraken, sym, tag, None, received_ts, None);
                        if sink
                            .alert(
                                &ctx,
                                IntegrityAlert::SourceError {
                                    kind: SourceErrorKind::Transport,
                                },
                            )
                            .is_err()
                        {
                            return;
                        }
                        if let Some(retry_in_ms) = quarantine_retry_ms {
                            if sink
                                .alert(&ctx, IntegrityAlert::Quarantined { retry_in_ms })
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }
        }
        RawMessage::Heartbeat | RawMessage::Status | RawMessage::Other => {}
    }
}

/// Why a reconnect backoff wait ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackoffExit {
    /// The full backoff elapsed.
    Elapsed,
    /// An operator reconnect cut the wait short.
    Reconnect,
    /// An operator pause parked the connection.
    Paused,
    /// Cancellation or a closed command channel ended the connection.
    Stopped,
}

/// Waits out a reconnect backoff while honouring operator commands.
///
/// Rules: `Reconnect` is an operator override and cuts the wait short; `Pause`
/// parks the connection; `Resync`/`ResyncAll` are deferred (coalesced) and
/// executed once the next connection is up, without shortening the wait;
/// `Resume` is a no-op because nothing is paused.
async fn wait_backoff(
    backoff: Duration,
    command_rx: &mut mpsc::Receiver<Command>,
    deferred: &mut VecDeque<Command>,
    cancel: &CancellationToken,
) -> BackoffExit {
    let sleep = tokio::time::sleep(backoff);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return BackoffExit::Stopped,
            _ = &mut sleep => return BackoffExit::Elapsed,
            cmd = command_rx.recv() => match cmd {
                Some(Command::Pause) => return BackoffExit::Paused,
                Some(Command::Reconnect(_)) => return BackoffExit::Reconnect,
                Some(cmd @ (Command::Resync(_) | Command::ResyncAll)) => defer_command(deferred, cmd),
                Some(Command::Resume) => {}
                None => return BackoffExit::Stopped,
            }
        }
    }
}

/// Queues a command for execution after the next connect, coalescing
/// duplicates (`ResyncAll` supersedes every per-symbol resync).
fn defer_command(queue: &mut VecDeque<Command>, cmd: Command) {
    let redundant = match &cmd {
        Command::ResyncAll => {
            queue.clear();
            false
        }
        Command::Resync(sym) => queue.iter().any(|queued| match queued {
            Command::ResyncAll => true,
            Command::Resync(other) => other == sym,
            _ => false,
        }),
        _ => false,
    };
    if !redundant {
        queue.push_back(cmd);
    }
}

/// Next command to execute: deferred ones first (in arrival order), then live.
async fn next_command(
    deferred: &mut VecDeque<Command>,
    command_rx: &mut mpsc::Receiver<Command>,
) -> Option<Command> {
    match deferred.pop_front() {
        Some(cmd) => Some(cmd),
        None => command_rx.recv().await,
    }
}

/// Returns the `Quarantined` alert delay for a rejection outcome. A cooldown
/// is not an alert-worthy quarantine: the symbol simply resubscribes after it.
fn rejection_retry_ms(outcome: RejectionOutcome, now: Instant) -> Option<u64> {
    match outcome {
        RejectionOutcome::CoolingDown { .. } => None,
        RejectionOutcome::Quarantined { until } => Some(
            until
                .saturating_duration_since(now)
                .as_millis()
                .min(u64::MAX as u128) as u64,
        ),
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
    if let Err(e) = book.apply(&data.asks, &data.bids) {
        warn!(
            symbol = symbol.as_str(),
            error = %e,
            "Failed to apply Kraken snapshot levels to shadow book; triggering resync"
        );
        pending_resubscribe.insert(symbol.clone());
        fsm.request_resync(Instant::now());
        let ctx = sink.next_context(VenueId::Kraken, symbol, tag, None, received_ts, None);
        if sink
            .alert(
                &ctx,
                IntegrityAlert::SourceError {
                    kind: SourceErrorKind::Parse,
                },
            )
            .is_err()
        {
            return;
        }
        if sink.alert(&ctx, IntegrityAlert::ResyncStarted).is_err() {
            return;
        }
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
            if sink
                .emit_alert(
                    VenueId::Kraken,
                    symbol,
                    tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ResyncCompleted {
                        last_update_id: None,
                    },
                )
                .is_err()
            {
                return;
            }
        }
        SnapshotOutcome::RejectedBadChecksum => {
            let expected = data.checksum.unwrap_or(0);
            let calculated = book.checksum();
            if sink
                .emit_alert(
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
                )
                .is_err()
            {
                return;
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
    evicted_bids: &mut Vec<Decimal>,
    evicted_asks: &mut Vec<Decimal>,
    sink: &mut ConnectionSink<S>,
) {
    if !fsm.is_in_sync() {
        return;
    }

    if let Err(e) = book.apply_with_evictions(&data.asks, &data.bids, evicted_asks, evicted_bids) {
        warn!(
            symbol = symbol.as_str(),
            error = %e,
            "Failed to apply Kraken update levels to shadow book; triggering resync"
        );
        pending_resubscribe.insert(symbol.clone());
        fsm.request_resync(Instant::now());
        let ctx = sink.next_context(VenueId::Kraken, symbol, tag, None, received_ts, None);
        if sink
            .alert(
                &ctx,
                IntegrityAlert::SourceError {
                    kind: SourceErrorKind::Parse,
                },
            )
            .is_err()
        {
            return;
        }
        if sink.alert(&ctx, IntegrityAlert::ResyncStarted).is_err() {
            return;
        }
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
            for price in evicted_bids.iter() {
                if sink.level(Side::Bid, *price, Decimal::ZERO).is_err() {
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
            for price in evicted_asks.iter() {
                if sink.level(Side::Ask, *price, Decimal::ZERO).is_err() {
                    return;
                }
            }
            if sink.end_book(&ctx, None).is_err() {
                return;
            }
        }
        UpdateOutcome::MismatchTriggerResync {
            expected,
            calculated,
        } => {
            warn!(
                symbol = symbol.as_str(),
                expected, calculated, "Kraken checksum mismatch; resubscribing"
            );
            if sink
                .emit_alert(
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
                )
                .is_err()
            {
                return;
            }
            if sink
                .emit_alert(
                    VenueId::Kraken,
                    symbol,
                    tag,
                    None,
                    received_ts,
                    None,
                    IntegrityAlert::ResyncStarted,
                )
                .is_err()
            {
                return;
            }
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    struct Harness {
        tx: mpsc::Sender<Command>,
        task: tokio::task::JoinHandle<(BackoffExit, VecDeque<Command>)>,
    }

    fn spawn_backoff(backoff: Duration, cancel: CancellationToken) -> Harness {
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            let mut deferred = VecDeque::new();
            let exit = wait_backoff(backoff, &mut rx, &mut deferred, &cancel).await;
            (exit, deferred)
        });
        Harness { tx, task }
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_runs_its_full_duration() {
        let h = spawn_backoff(Duration::from_secs(4), CancellationToken::new());
        tokio::time::advance(Duration::from_millis(3999)).await;
        tokio::task::yield_now().await;
        assert!(!h.task.is_finished(), "must still be waiting 1ms early");
        tokio::time::advance(Duration::from_millis(1)).await;
        let (exit, deferred) = h.task.await.unwrap();
        assert_eq!(exit, BackoffExit::Elapsed);
        assert!(deferred.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn resync_commands_are_deferred_without_shortening_backoff() {
        let h = spawn_backoff(Duration::from_secs(4), CancellationToken::new());
        h.tx.send(Command::Resync(Symbol::from("BTC/USD")))
            .await
            .unwrap();
        h.tx.send(Command::Resync(Symbol::from("BTC/USD")))
            .await
            .unwrap();
        h.tx.send(Command::Resync(Symbol::from("ETH/USD")))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(3999)).await;
        tokio::task::yield_now().await;
        assert!(!h.task.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        let (exit, deferred) = h.task.await.unwrap();
        assert_eq!(exit, BackoffExit::Elapsed);
        assert_eq!(deferred.len(), 2, "duplicate per-symbol resync coalesced");
    }

    #[tokio::test(start_paused = true)]
    async fn resync_all_supersedes_symbol_resyncs() {
        let h = spawn_backoff(Duration::from_secs(1), CancellationToken::new());
        h.tx.send(Command::Resync(Symbol::from("BTC/USD")))
            .await
            .unwrap();
        h.tx.send(Command::ResyncAll).await.unwrap();
        h.tx.send(Command::Resync(Symbol::from("ETH/USD")))
            .await
            .unwrap();
        let (exit, deferred) = h.task.await.unwrap();
        assert_eq!(exit, BackoffExit::Elapsed);
        assert_eq!(deferred.len(), 1);
        assert!(matches!(deferred[0], Command::ResyncAll));
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_cuts_backoff_short_and_keeps_deferred_commands() {
        let h = spawn_backoff(Duration::from_secs(60), CancellationToken::new());
        h.tx.send(Command::ResyncAll).await.unwrap();
        h.tx.send(Command::Reconnect("operator".into()))
            .await
            .unwrap();
        let (exit, deferred) = h.task.await.unwrap();
        assert_eq!(exit, BackoffExit::Reconnect);
        assert_eq!(deferred.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pause_parks_and_cancel_stops() {
        let h = spawn_backoff(Duration::from_secs(60), CancellationToken::new());
        h.tx.send(Command::Pause).await.unwrap();
        assert_eq!(h.task.await.unwrap().0, BackoffExit::Paused);

        let cancel = CancellationToken::new();
        let h = spawn_backoff(Duration::from_secs(60), cancel.clone());
        cancel.cancel();
        assert_eq!(h.task.await.unwrap().0, BackoffExit::Stopped);
    }

    #[tokio::test]
    async fn deferred_commands_are_served_before_live_ones() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(Command::ResyncAll).await.unwrap();
        let mut deferred = VecDeque::new();
        deferred.push_back(Command::Resync(Symbol::from("BTC/USD")));
        assert!(matches!(
            next_command(&mut deferred, &mut rx).await,
            Some(Command::Resync(_))
        ));
        assert!(matches!(
            next_command(&mut deferred, &mut rx).await,
            Some(Command::ResyncAll)
        ));
    }
}
