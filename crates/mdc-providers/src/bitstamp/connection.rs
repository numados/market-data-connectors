//! Bitstamp WebSocket per-connection worker task and protocol loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DisconnectKind, IntegrityAlert, PriceLevel, RunId, Side, SourceErrorKind, Symbol, SymbolTag,
    UnixNanos, VenueId,
};
use mdc_core::resync::ResyncFailure;
use mdc_core::sink::{BookKind, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
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
use crate::sink_util::ConnectionSink;

/// Buffered order book diff held while authoritative snapshot retrieval is in flight.
#[derive(Debug, Clone)]
struct BufferedDiff {
    microtimestamp: u64,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

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
    #[allow(clippy::expect_used)]
    serde_json::to_string(&req).expect("infallible static frame serialization")
}

/// Runs a single Bitstamp WebSocket connection worker managing a slice of symbols.
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "Internal connection worker receives injected configuration, state channels, and policies"
)]
pub async fn run_connection_task<S: StreamingSink>(
    run_id: RunId,
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: BitstampConfig,
    rest_client: BitstampRestClient,
    sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    generation_allocator: Arc<std::sync::atomic::AtomicU64>,
    resync_policy_factory: crate::bitstamp::connector::ResyncPolicyFactory,
) -> Result<(), crate::sink_util::SinkError> {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id)
        .with_generation_allocator(generation_allocator);

    let mut fsms: HashMap<Symbol, SymbolFsm> = symbols
        .iter()
        .map(|(s, _)| {
            (
                s.clone(),
                SymbolFsm::with_policy(s.clone(), (resync_policy_factory)()),
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
    let mut is_paused = false;
    let mut snapshot_tasks = JoinSet::new();

    'reconnect_loop: loop {
        snapshot_tasks.abort_all();
        while snapshot_tasks.join_next().await.is_some() {}

        while is_paused {
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Resume) => {
                            is_paused = false;
                            backoff = Duration::from_millis(500);
                            break;
                        }
                        Some(_) => {}
                        None => break 'reconnect_loop,
                    }
                }
            }
        }

        if cancel.is_cancelled() || sink.is_failed() {
            break 'reconnect_loop;
        }

        info!(
            connection_id,
            url = %config.ws_url,
            "Connecting to Bitstamp WebSocket"
        );

        let ws_res = tokio::select! {
            _ = cancel.cancelled() => break 'reconnect_loop,
            res = crate::ws_util::connect_ws(config.ws_url.as_str(), config.connection_timeout) => res,
        };
        let ws_stream = match ws_res {
            Ok((stream, _resp)) => {
                info!(connection_id, "Connected to Bitstamp WebSocket");
                backoff = Duration::from_millis(500);
                sink.next_generation();
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
        let mut buffered_diffs: HashMap<Symbol, VecDeque<BufferedDiff>> = HashMap::new();
        let mut in_flight_snapshots: HashSet<Symbol> = HashSet::new();
        let mut active_snapshot_ids: HashMap<Symbol, u64> = HashMap::new();
        let mut next_snapshot_id: u64 = 1;

        for (sym, tag) in &symbols {
            if let Some(fsm) = fsms.get_mut(sym) {
                fsm.trigger_resync(Instant::now());
            }

            if sink
                .emit_alert(
                    VenueId::Bitstamp,
                    sym,
                    *tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ResyncStarted,
                )
                .is_err()
            {
                break 'reconnect_loop;
            }

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

        'frame_loop: loop {
            if sink.is_failed() {
                break 'frame_loop;
            }

            // Check if we can dispatch a pending snapshot request
            if snapshot_tasks.is_empty() && !snapshot_queue.is_empty() {
                let now = Instant::now();
                let mut found_idx = None;

                for (idx, sym) in snapshot_queue.iter().enumerate() {
                    if in_flight_snapshots.contains(sym) {
                        continue;
                    }
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
                            fsm.trigger_resync(now);
                            fsm.mark_snapshot_requested(now);
                        }
                        in_flight_snapshots.insert(sym.clone());
                        let req_id = next_snapshot_id;
                        next_snapshot_id = next_snapshot_id.wrapping_add(1);
                        active_snapshot_ids.insert(sym.clone(), req_id);

                        let client = rest_client.clone();
                        let sym_clone = sym.clone();
                        let pair_lower = sym.as_str().to_lowercase();

                        let cancel_snap = cancel.clone();
                        snapshot_tasks.spawn(async move {
                            tokio::select! {
                                _ = cancel_snap.cancelled() => (sym_clone, req_id, Err(RestError::Transport("request cancelled".to_string()))),
                                res = client.fetch_order_book(&pair_lower, Instant::now()) => (sym_clone, req_id, res),
                            }
                        });
                    }
                }
            }

            tokio::select! {
                _ = cancel.cancelled() => {
                    snapshot_tasks.abort_all();
                    while snapshot_tasks.join_next().await.is_some() {}
                     for (sym, tag) in &symbols {
                        sink.best_effort_emit_alert(
                            VenueId::Bitstamp,
                            sym,
                            *tag,
                            None,
                            crate::now_nanos(),
                            None,
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
                        WatchdogAction::Reconnect { silent_for } => {
                            let elapsed_ms = silent_for.as_millis() as u64;
                            warn!(
                                connection_id,
                                elapsed_ms,
                                "Silence watchdog timeout on Bitstamp feed; forcing reconnect"
                            );
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::SilenceTimeout { elapsed_ms },
                                ).is_err() {
                                    break;
                                }
                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout,
                                    },
                                ).is_err() {
                                    break;
                                }
                            }
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
                                if let Some(buf) = buffered_diffs.get_mut(&sym) {
                                    buf.clear();
                                }

                                let tag = symbol_tags.get(&sym).copied().unwrap_or_default();
                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    &sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ResyncStarted,
                                ).is_err() {
                                    break 'frame_loop;
                                }

                                // Any request already in flight predates this
                                // invalidation: orphan its id so the late response
                                // is discarded, and queue a fresh snapshot.
                                active_snapshot_ids.remove(&sym);
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
                                sink.best_effort_emit_alert(
                                    VenueId::Bitstamp,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                );
                            }
                            is_paused = true;
                            break 'frame_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        None => break 'frame_loop,
                    }
                }

                // In-flight REST snapshot completion
                Some(task_res) = snapshot_tasks.join_next(), if !snapshot_tasks.is_empty() => {
                    let now = Instant::now();

                    if let Ok((sym, req_id, res)) = task_res {
                        in_flight_snapshots.remove(&sym);

                        // Discard if superseded by a newer snapshot request for this symbol
                        if active_snapshot_ids.get(&sym) != Some(&req_id) {
                            continue;
                        }

                        match res {
                            Ok(snap) => {
                                if let Some(fsm) = fsms.get(&sym) {
                                    if fsm.state() != SyncState::AwaitingSnapshot {
                                        // Symbol is already synchronized or superseded; discard older snapshot
                                        continue;
                                    }
                                }

                                let tag = symbol_tags.get(&sym).copied().unwrap_or_default();
                                let snap_ts = Some(UnixNanos::from_micros(snap.microtimestamp));
                                let ctx = sink.next_context(
                                    VenueId::Bitstamp,
                                    &sym,
                                    tag,
                                    snap_ts,
                                    crate::now_nanos(),
                                    None,
                                );

                                if sink.begin_book(&ctx, BookKind::Snapshot).is_err() {
                                    break 'frame_loop;
                                }
                                for level in &snap.bids {
                                    if sink.level(Side::Bid, level[0], level[1]).is_err() {
                                        break 'frame_loop;
                                    }
                                }
                                for level in &snap.asks {
                                    if sink.level(Side::Ask, level[0], level[1]).is_err() {
                                        break 'frame_loop;
                                    }
                                }
                                if sink.end_book(&ctx, None).is_err() {
                                    break 'frame_loop;
                                }

                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    &sym,
                                    tag,
                                    snap_ts,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ResyncCompleted {
                                        last_update_id: None,
                                    },
                                ).is_err() {
                                    break 'frame_loop;
                                }

                                if let Some(fsm) = fsms.get_mut(&sym) {
                                    fsm.on_snapshot(snap.microtimestamp, now);
                                }

                                // Replay diffs buffered while snapshot was in flight that are newer than snapshot
                                if let Some(queue) = buffered_diffs.get_mut(&sym) {
                                    while let Some(diff) = queue.pop_front() {
                                        if diff.microtimestamp > snap.microtimestamp {
                                            let diff_ts = Some(UnixNanos::from_micros(diff.microtimestamp));
                                            let ctx = sink.next_context(
                                                VenueId::Bitstamp,
                                                &sym,
                                                tag,
                                                diff_ts,
                                                crate::now_nanos(),
                                                None,
                                            );
                                            if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                                                break 'frame_loop;
                                            }
                                            for level in &diff.bids {
                                                if sink.level(Side::Bid, level.price, level.volume).is_err() {
                                                    break 'frame_loop;
                                                }
                                            }
                                            for level in &diff.asks {
                                                if sink.level(Side::Ask, level.price, level.volume).is_err() {
                                                    break 'frame_loop;
                                                }
                                            }
                                            if sink.end_book(&ctx, None).is_err() {
                                                break 'frame_loop;
                                            }
                                        }
                                    }
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

                                if !snapshot_queue.contains(&sym) && !in_flight_snapshots.contains(&sym) {
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
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::TransportError,
                                    },
                                ).is_err() {
                                    break;
                                }
                            }
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "Bitstamp WebSocket connection closed by peer");
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Bitstamp,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Closed,
                                    },
                                ).is_err() {
                                    break;
                                }
                            }
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
                                    if sink.emit_alert(
                                        VenueId::Bitstamp,
                                        sym,
                                        *tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ConnectionLost {
                                            reason: DisconnectKind::VenueRequested,
                                        },
                                    ).is_err() {
                                        break;
                                    }
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
                                                if fsm.state() != SyncState::AwaitingSnapshot {
                                                    let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                    if sink.emit_alert(
                                                        VenueId::Bitstamp,
                                                        sym,
                                                        tag,
                                                        None,
                                                        crate::now_nanos(),
                                                        None,
                                                        IntegrityAlert::SourceError {
                                                            kind: SourceErrorKind::Parse,
                                                        },
                                                    ).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                    if sink.emit_alert(
                                                        VenueId::Bitstamp,
                                                        sym,
                                                        tag,
                                                        None,
                                                        crate::now_nanos(),
                                                        None,
                                                        IntegrityAlert::ResyncStarted,
                                                    ).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                    fsm.trigger_resync(Instant::now());
                                                    if let Some(buf) = buffered_diffs.get_mut(sym) {
                                                        buf.clear();
                                                    }
                                                    active_snapshot_ids.remove(sym);
                                                    if !snapshot_queue.contains(sym) {
                                                        snapshot_queue.push_back(sym.clone());
                                                    }
                                                }
                                                continue;
                                            }
                                        };

                                        match fsm.check_diff(header.microtimestamp) {
                                            DiffDecision::Emit => {
                                                let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                let diff_ts = Some(UnixNanos::from_micros(header.microtimestamp));
                                                let ctx = sink.next_context(
                                                    VenueId::Bitstamp,
                                                    sym,
                                                    tag,
                                                    diff_ts,
                                                    crate::now_nanos(),
                                                    None,
                                                );

                                                if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                                                    break 'frame_loop;
                                                }
                                                for level in &scratch.bids {
                                                    if sink.level(Side::Bid, level.price, level.volume).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                }
                                                for level in &scratch.asks {
                                                    if sink.level(Side::Ask, level.price, level.volume).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                }
                                                if sink.end_book(&ctx, None).is_err() {
                                                    break 'frame_loop;
                                                }
                                            }
                                            DiffDecision::Buffer => {
                                                let queue = buffered_diffs
                                                    .entry(sym.clone())
                                                    .or_insert_with(|| VecDeque::with_capacity(128));
                                                if queue.len() >= config.buffer_capacity {
                                                    warn!(
                                                        connection_id,
                                                        symbol = %sym,
                                                        capacity = config.buffer_capacity,
                                                        "Bitstamp diff buffer overflow; triggering resync"
                                                    );
                                                    queue.clear();
                                                    let now = Instant::now();
                                                    if let Some(fsm) = fsms.get_mut(sym) {
                                                        fsm.trigger_resync(now);
                                                    }
                                                    let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                    if sink.emit_alert(
                                                        VenueId::Bitstamp,
                                                        sym,
                                                        tag,
                                                        None,
                                                        crate::now_nanos(),
                                                        None,
                                                        IntegrityAlert::ResyncStarted,
                                                    ).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                    active_snapshot_ids.remove(sym);
                                                    if !snapshot_queue.contains(sym) {
                                                        snapshot_queue.push_back(sym.clone());
                                                    }
                                                } else {
                                                    queue.push_back(BufferedDiff {
                                                        microtimestamp: header.microtimestamp,
                                                        bids: scratch.bids.clone(),
                                                        asks: scratch.asks.clone(),
                                                    });
                                                    if !snapshot_queue.contains(sym) && !in_flight_snapshots.contains(sym) {
                                                        snapshot_queue.push_back(sym.clone());
                                                    }
                                                }
                                            }
                                            DiffDecision::Drop => {}
                                        }
                                    }
                                }
                            } else if env.event == "trade" && config.streams.trades {
                                if let Some(sym) = channel_to_symbol.get(env.channel) {
                                    if let Some(data_val) = env.data {
                                        if let Ok(raw_trade) = serde_json::from_str::<RawTrade>(data_val.get()) {
                                            if let Ok((_trade_ts, canonical_trade)) = normalize_trade(&raw_trade) {
                                                let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                let trade_ts = Some(UnixNanos::from_micros(raw_trade.microtimestamp));
                                                let ctx = sink.next_context(
                                                    VenueId::Bitstamp,
                                                    sym,
                                                    tag,
                                                    trade_ts,
                                                    crate::now_nanos(),
                                                    None,
                                                );
                                                if sink.trade(&ctx, &canonical_trade).is_err() {
                                                    break 'frame_loop;
                                                }
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
                            for (sym, tag) in &symbols {
                                if sink
                                    .emit_alert(
                                        VenueId::Bitstamp,
                                        sym,
                                        *tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ConnectionLost {
                                            reason: DisconnectKind::Closed,
                                        },
                                    )
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            break 'frame_loop;
                        }
                        _ => {}
                    }
                }
            }
        }

        snapshot_tasks.abort_all();
        while snapshot_tasks.join_next().await.is_some() {}

        if sink.is_failed() {
            break 'reconnect_loop;
        }
    }

    snapshot_tasks.abort_all();
    while snapshot_tasks.join_next().await.is_some() {}

    if sink.is_failed() {
        Err(crate::sink_util::SinkError)
    } else {
        Ok(())
    }
}
