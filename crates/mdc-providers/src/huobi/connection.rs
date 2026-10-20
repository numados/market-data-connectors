//! Huobi (HTX) WebSocket connection worker and frame protocol loop.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, L2Delta, MarketEvent, RunId, SequenceRange,
    Side, SourceErrorKind, Symbol, SymbolTag, UnixNanos, VenueId,
};
use mdc_core::resync::{DeltaDecision, SnapshotOutcome};
use mdc_core::sink::{BookKind, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::huobi::config::HuobiConfig;
use crate::huobi::control::Command;
use crate::huobi::fsm::SymbolFsm;
use crate::huobi::gzip::GzipDecoder;
use crate::huobi::normalize::normalize_trade;
use crate::huobi::raw::{
    HuobiConnectionScratch, HuobiMessageKind, MbpSeed, RawEnvelope, RawTradeTick, build_pong_frame,
    build_req_snapshot_frame, build_sub_mbp_frame, build_sub_trades_frame,
    extract_symbol_from_channel,
};
use crate::sink_util::ConnectionSink;

/// Runs the Huobi connection task until cancelled.
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: HuobiConfig,
    sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    run_id: RunId,
    generation_allocator: std::sync::Arc<std::sync::atomic::AtomicU64>,
    policy_factory: crate::huobi::connector::ResyncPolicyFactory,
) -> Result<(), crate::sink_util::SinkError> {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id)
        .with_generation_allocator(generation_allocator);

    let mut fsms: HashMap<Symbol, SymbolFsm> = symbols
        .iter()
        .map(|(s, tag)| {
            (
                s.clone(),
                SymbolFsm::with_policy(s.clone(), *tag, &config, (policy_factory)()),
            )
        })
        .collect();

    let mut scratch = HuobiConnectionScratch::with_capacity(1_024);
    let mut gzip_decoder = GzipDecoder::with_capacity(64 * 1024);
    let mut reconnect_attempt = 0u32;
    let mut is_paused = false;

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
                        Some(Command::Reconnect) => {
                            is_paused = false;
                            break;
                        }
                        Some(Command::ResyncSymbol(_)) | Some(Command::Pause) => {}
                        None => break 'reconnect_loop,
                    }
                }
            }
        }

        if reconnect_attempt > 0 {
            let backoff_secs = (1u64 << reconnect_attempt.saturating_sub(1).min(5)).min(30);
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
            }
        }
        reconnect_attempt = reconnect_attempt.saturating_add(1);

        info!(
            connection_id,
            url = %config.ws_url,
            attempt = reconnect_attempt,
            "Connecting to Huobi WebSocket"
        );

        let ws_res = tokio::select! {
            _ = cancel.cancelled() => break 'reconnect_loop,
            res = crate::ws_util::connect_ws(config.ws_url.as_str(), config.connection_timeout) => res,
        };
        let ws_stream = match ws_res {
            Ok((stream, _response)) => {
                reconnect_attempt = 0;
                stream
            }
            Err(e) => {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to connect to Huobi WebSocket; retrying"
                );
                continue 'reconnect_loop;
            }
        };

        sink.next_generation();
        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Queue all symbols for snapshot requests and reset FSMs
        let mut pending_snapshots: VecDeque<Symbol> = VecDeque::new();
        let mut active_snapshot_req: Option<(Symbol, String, Instant)> = None;
        let mut snapshot_counter = 0u64;

        for (sym, tag) in &symbols {
            if let Some(fsm) = fsms.get_mut(sym) {
                fsm.reset(&config);
            }
            pending_snapshots.push_back(sym.clone());

            if sink
                .emit_alert(
                    VenueId::Huobi,
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
        }

        // Send initial subscriptions
        for (sym, _) in &symbols {
            if config.streams.mbp {
                let frame = build_sub_mbp_frame(
                    sym.as_str(),
                    config.mbp_levels,
                    &format!("sub-mbp-{}", sym.as_str()),
                );
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        connection_id,
                        symbol = %sym,
                        error = %e,
                        "Failed to send MBP subscribe frame"
                    );
                    break;
                }
            }

            if config.streams.trades {
                let frame =
                    build_sub_trades_frame(sym.as_str(), &format!("sub-trades-{}", sym.as_str()));
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        connection_id,
                        symbol = %sym,
                        error = %e,
                        "Failed to send trades subscribe frame"
                    );
                }
            }
        }

        // Send first snapshot request if ready
        try_send_next_snapshot(
            &mut ws_tx,
            &mut fsms,
            &mut pending_snapshots,
            &mut active_snapshot_req,
            &mut snapshot_counter,
            &config,
            Instant::now(),
        )
        .await;

        let now_inst = Instant::now();
        let mut watchdog =
            SilenceWatchdog::new(config.silence_timeout / 2, config.silence_timeout, now_inst);
        let mut watchdog_ticker = tokio::time::interval(Duration::from_secs(1));
        watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut disconnect_emitted = false;

        'frame_loop: loop {
            tokio::select! {
                // The post-loop cancellation block emits the single Shutdown alert.
                _ = cancel.cancelled() => break 'reconnect_loop,

                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::ResyncSymbol(sym)) => {
                            if let Some(fsm) = fsms.get_mut(&sym) {
                                let alert = fsm.trigger_resync(Instant::now());
                                if sink.emit_alert(
                                    VenueId::Huobi,
                                    &sym,
                                    fsm.tag(),
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    alert,
                                ).is_err() {
                                    break 'frame_loop;
                                }
                                if !pending_snapshots.contains(&sym) {
                                    pending_snapshots.push_back(sym);
                                }
                                try_send_next_snapshot(
                                    &mut ws_tx,
                                    &mut fsms,
                                    &mut pending_snapshots,
                                    &mut active_snapshot_req,
                                    &mut snapshot_counter,
                                    &config,
                                    Instant::now(),
                                ).await;
                            }
                        }
                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            // Alert before arming the episode guard so the
                            // reconnect window is never silent for consumers.
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Huobi,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::VenueRequested,
                                    },
                                ).is_err() {
                                    break 'frame_loop;
                                }
                            }
                            disconnect_emitted = true;
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            is_paused = true;
                            disconnect_emitted = true;
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Huobi,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                ).is_err() {
                                    break 'frame_loop;
                                }
                            }
                            break 'frame_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        None => {
                            break 'reconnect_loop;
                        }
                    }
                }

                _ = watchdog_ticker.tick() => {
                    let now = Instant::now();
                    match watchdog.poll(now) {
                        WatchdogAction::Healthy => {}
                        WatchdogAction::SendPing => {
                            let _ = ws_tx.send(Message::Ping(vec![])).await;
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            let elapsed_ms = silent_for.as_millis() as u64;
                            disconnect_emitted = true;
                            for (sym, tag) in &symbols {
                                if sink.emit_alert(
                                    VenueId::Huobi,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::SilenceTimeout { elapsed_ms },
                                ).is_err() {
                                    break 'frame_loop;
                                }
                                if sink.emit_alert(
                                    VenueId::Huobi,
                                    sym,
                                    *tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout,
                                    },
                                ).is_err() {
                                    break 'frame_loop;
                                }
                            }
                            warn!(
                                connection_id,
                                elapsed_ms,
                                "Silence watchdog timeout on Huobi feed; forcing reconnect"
                            );
                            break 'frame_loop;
                        }
                    }

                    // Check if active snapshot request timed out
                    if let Some((active_sym, _, req_ts)) = &active_snapshot_req {
                        if now.duration_since(*req_ts) > config.snapshot_cooldown {
                            warn!(
                                connection_id,
                                symbol = %active_sym,
                                "Active snapshot request timed out; re-queueing"
                            );
                            if let Some(fsm) = fsms.get_mut(active_sym) {
                                fsm.record_snapshot_failure(now);
                            }
                            let sym = active_sym.clone();
                            active_snapshot_req = None;
                            if !pending_snapshots.contains(&sym) {
                                pending_snapshots.push_back(sym);
                            }
                            try_send_next_snapshot(
                                &mut ws_tx,
                                &mut fsms,
                                &mut pending_snapshots,
                                &mut active_snapshot_req,
                                &mut snapshot_counter,
                                &config,
                                now,
                            ).await;
                        }
                    } else if !pending_snapshots.is_empty() {
                        try_send_next_snapshot(
                            &mut ws_tx,
                            &mut fsms,
                            &mut pending_snapshots,
                            &mut active_snapshot_req,
                            &mut snapshot_counter,
                            &config,
                            now,
                        ).await;
                    }
                }

                msg_opt = ws_rx.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "WebSocket read error; reconnecting");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "WebSocket stream closed by server; reconnecting");
                            break 'frame_loop;
                        }
                    };

                    let (frame_bytes, received_ts) = match &msg {
                        Message::Binary(bytes) => {
                            let ts = crate::now_nanos();
                            match gzip_decoder.decompress(bytes) {
                                Ok(decompressed) => (decompressed, ts),
                                Err(e @ crate::huobi::gzip::GzipError::FrameTooLarge { .. }) => {
                                    warn!(
                                        connection_id,
                                        error = %e,
                                        "Decompressed frame exceeded size cap; reconnecting"
                                    );
                                    break 'frame_loop;
                                }
                                Err(e) => {
                                    warn!(
                                        connection_id,
                                        error = %e,
                                        "Gzip decompression failed; skipping frame"
                                    );
                                    continue 'frame_loop;
                                }
                            }
                        }
                        Message::Text(text) => {
                            let ts = crate::now_nanos();
                            (text.as_bytes(), ts)
                        }
                        Message::Ping(p) => {
                            let _ = ws_tx.send(Message::Pong(p.clone())).await;
                            continue 'frame_loop;
                        }
                        Message::Pong(_) => continue 'frame_loop,
                        Message::Close(_) => {
                            info!(connection_id, "Server sent close frame; reconnecting");
                            break 'frame_loop;
                        }
                        Message::Frame(_) => continue 'frame_loop,
                    };

                    let now = Instant::now();
                    watchdog.on_data_frame(now);

                    let env: RawEnvelope<'_> = match serde_json::from_slice(frame_bytes) {
                        Ok(e) => e,
                        Err(err) => {
                            warn!(connection_id, error = %err, "Malformed JSON frame; skipping");
                            continue 'frame_loop;
                        }
                    };

                    match env.message_kind() {
                        HuobiMessageKind::Ping(ts) => {
                            let pong = build_pong_frame(ts);
                            if let Err(e) = ws_tx.send(Message::Text(pong)).await {
                                warn!(connection_id, error = %e, "Failed to send pong frame");
                            }
                        }

                        HuobiMessageKind::SnapshotReply { id, rep, status, ts } => {
                            if let Some((active_sym, active_id, _)) = &active_snapshot_req {
                                if active_id == id {
                                    let active_symbol = active_sym.clone();
                                    active_snapshot_req = None;

                                    let mut snapshot_success = false;

                                    if status == "ok" {
                                        if let Some(data_raw) = env.data {
                                            if let Some(fsm) = fsms.get_mut(&active_symbol) {
                                                let mut de = serde_json::Deserializer::from_str(data_raw.get());
                                                let header_res = MbpSeed::new(&mut scratch).deserialize(&mut de);
                                                match header_res {
                                                    Ok(header) => {
                                                        let delivery = sink.next_stamp();
                                                        match fsm.on_snapshot(
                                                            &header,
                                                            &scratch,
                                                            ts,
                                                            delivery,
                                                            received_ts,
                                                            now,
                                                        ) {
                                                            Ok(SnapshotOutcome::Synchronized {
                                                                replay,
                                                                ..
                                                            }) => {
                                                                snapshot_success = true;

                                                                // Emit snapshot
                                                                let snap_ctx = sink.next_context(
                                                                    VenueId::Huobi,
                                                                    &active_symbol,
                                                                    fsm.tag(),
                                                                    ts.map(UnixNanos::from_millis),
                                                                    received_ts,
                                                                    Some(header.seq_num),
                                                                );
                                                                if sink.begin_book(&snap_ctx, BookKind::Snapshot).is_err() {
                                                                    break 'frame_loop;
                                                                }
                                                                for l in &scratch.bids {
                                                                    if sink.level(Side::Bid, l.price, l.volume).is_err() {
                                                                        break 'frame_loop;
                                                                    }
                                                                }
                                                                for l in &scratch.asks {
                                                                    if sink.level(Side::Ask, l.price, l.volume).is_err() {
                                                                        break 'frame_loop;
                                                                    }
                                                                }
                                                                if sink.end_book(&snap_ctx, None).is_err() {
                                                                    break 'frame_loop;
                                                                }

                                                                // Replay buffered deltas
                                                                for replayed_event in replay {
                                                                    let rep_ctx = sink.next_context(
                                                                        VenueId::Huobi,
                                                                        &active_symbol,
                                                                        fsm.tag(),
                                                                        replayed_event.exchange_ts,
                                                                        replayed_event.received_ts,
                                                                        replayed_event.source_sequence,
                                                                    );
                                                                    if sink.begin_book(&rep_ctx, BookKind::Delta).is_err() {
                                                                        break 'frame_loop;
                                                                    }
                                                                    for l in &replayed_event.kind.bids {
                                                                        if sink.level(Side::Bid, l.price, l.volume).is_err() {
                                                                            break 'frame_loop;
                                                                        }
                                                                    }
                                                                    for l in &replayed_event.kind.asks {
                                                                        if sink.level(Side::Ask, l.price, l.volume).is_err() {
                                                                            break 'frame_loop;
                                                                        }
                                                                    }
                                                                    if sink.end_book(&rep_ctx, replayed_event.kind.sequence).is_err() {
                                                                        break 'frame_loop;
                                                                    }
                                                                }

                                                                // Alert ResyncCompleted strictly after snapshot and replay
                                                                if sink.emit_alert(
                                                                    VenueId::Huobi,
                                                                    &active_symbol,
                                                                    fsm.tag(),
                                                                    None,
                                                                    crate::now_nanos(),
                                                                    None,
                                                                    IntegrityAlert::ResyncCompleted {
                                                                        last_update_id: Some(header.seq_num),
                                                                    },
                                                                ).is_err() {
                                                                    break 'frame_loop;
                                                                }
                                                            }
                                                            Ok(SnapshotOutcome::StaleSnapshot { .. }) => {
                                                                warn!(
                                                                    connection_id,
                                                                    symbol = %active_symbol,
                                                                    "Stale snapshot received; re-requesting after cooldown"
                                                                );
                                                            }
                                                            Ok(SnapshotOutcome::Ignored) => {}
                                                            Err(e) => {
                                                                warn!(
                                                                    connection_id,
                                                                    symbol = %active_symbol,
                                                                    error = %e,
                                                                    "Failed to process snapshot outcome"
                                                                );
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            connection_id,
                                                            symbol = %active_symbol,
                                                            error = %e,
                                                            "Failed to deserialize snapshot data"
                                                        );
                                                    }
                                                }
                                            }
                                        } else {
                                            warn!(
                                                connection_id,
                                                symbol = %active_symbol,
                                                "Huobi snapshot reply status ok but missing data field"
                                            );
                                        }
                                    } else {
                                        warn!(
                                            connection_id,
                                            symbol = %active_symbol,
                                            rep,
                                            status,
                                            "Snapshot request returned non-ok status"
                                        );
                                    }

                                    if !snapshot_success {
                                        if let Some(fsm) = fsms.get_mut(&active_symbol) {
                                            fsm.record_snapshot_failure(now);
                                            let resync_alert = fsm.trigger_resync(now);
                                            if sink.emit_alert(
                                                VenueId::Huobi,
                                                &active_symbol,
                                                fsm.tag(),
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
                                                VenueId::Huobi,
                                                &active_symbol,
                                                fsm.tag(),
                                                None,
                                                crate::now_nanos(),
                                                None,
                                                resync_alert,
                                            ).is_err() {
                                                break 'frame_loop;
                                            }
                                        }
                                        if !pending_snapshots.contains(&active_symbol) {
                                            pending_snapshots.push_back(active_symbol);
                                        }
                                    }

                                    try_send_next_snapshot(
                                        &mut ws_tx,
                                        &mut fsms,
                                        &mut pending_snapshots,
                                        &mut active_snapshot_req,
                                        &mut snapshot_counter,
                                        &config,
                                        now,
                                    ).await;
                                }
                            }
                        }

                        HuobiMessageKind::MbpTick { ch, ts } => {
                            if is_paused {
                                continue 'frame_loop;
                            }

                            let sym_opt = extract_symbol_from_channel(ch);
                            if let Some(sym_str) = sym_opt {
                                let symbol = Symbol::from(sym_str);
                                if let Some(fsm) = fsms.get_mut(&symbol) {
                                    if let Some(tick_raw) = env.tick {
                                        let mut de = serde_json::Deserializer::from_str(tick_raw.get());
                                        if let Ok(header) = MbpSeed::new(&mut scratch).deserialize(&mut de) {
                                            if let Some(prev) = header.prev_seq_num {
                                                let range = SequenceRange::new(
                                                    prev.saturating_add(1),
                                                    header.seq_num,
                                                );

                                                match fsm.check_delta(range) {
                                                    DeltaDecision::Emit => {
                                                        let ctx = sink.next_context(
                                                            VenueId::Huobi,
                                                            &symbol,
                                                            fsm.tag(),
                                                            Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            Some(header.seq_num),
                                                        );
                                                        if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                                                            break 'frame_loop;
                                                        }
                                                        for l in &scratch.bids {
                                                            if sink.level(Side::Bid, l.price, l.volume).is_err() {
                                                                break 'frame_loop;
                                                            }
                                                        }
                                                        for l in &scratch.asks {
                                                            if sink.level(Side::Ask, l.price, l.volume).is_err() {
                                                                break 'frame_loop;
                                                            }
                                                        }
                                                        if sink.end_book(&ctx, Some(range)).is_err() {
                                                            break 'frame_loop;
                                                        }
                                                    }
                                                    DeltaDecision::Buffer => {
                                                        let event = MarketEvent {
                                                            venue: VenueId::Huobi,
                                                            symbol: symbol.clone(),
                                                            exchange_ts: Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            tag: fsm.tag(),
                                                            delivery: DeliveryStamp::with_connection_id(
                                                                sink.run_id(),
                                                                connection_id,
                                                                sink.generation(),
                                                                0,
                                                            ),
                                                            source_sequence: Some(header.seq_num),
                                                            kind: L2Delta {
                                                                sequence: Some(range),
                                                                bids: scratch.bids.clone(),
                                                                asks: scratch.asks.clone(),
                                                            },
                                                        };
                                                        if let Err(e) = fsm.buffer_delta(event) {
                                                            warn!(
                                                                %symbol,
                                                                error = %e,
                                                                "Buffer overflow while buffering Huobi deltas; triggering resync"
                                                            );
                                                            let start_alert = fsm.trigger_resync(now);
                                                            if sink.emit_alert(
                                                                VenueId::Huobi,
                                                                &symbol,
                                                                fsm.tag(),
                                                                None,
                                                                crate::now_nanos(),
                                                                None,
                                                                start_alert,
                                                            ).is_err() {
                                                                break 'frame_loop;
                                                            }

                                                            if !pending_snapshots.contains(&symbol) {
                                                                pending_snapshots.push_back(symbol);
                                                            }
                                                            try_send_next_snapshot(
                                                                &mut ws_tx,
                                                                &mut fsms,
                                                                &mut pending_snapshots,
                                                                &mut active_snapshot_req,
                                                                &mut snapshot_counter,
                                                                &config,
                                                                now,
                                                            )
                                                            .await;
                                                        }
                                                    }
                                                    DeltaDecision::Drop => {
                                                        // Stale or duplicate delta; dropped silently
                                                    }
                                                    DeltaDecision::GapDetected { alert } => {
                                                        if sink.emit_alert(
                                                            VenueId::Huobi,
                                                            &symbol,
                                                            fsm.tag(),
                                                            Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            Some(header.seq_num),
                                                            alert,
                                                        ).is_err() {
                                                            break 'frame_loop;
                                                        }

                                                        let start_alert = fsm.trigger_resync(now);
                                                        if sink.emit_alert(
                                                            VenueId::Huobi,
                                                            &symbol,
                                                            fsm.tag(),
                                                            None,
                                                            crate::now_nanos(),
                                                            None,
                                                            start_alert,
                                                        ).is_err() {
                                                            break 'frame_loop;
                                                        }

                                                        if !pending_snapshots.contains(&symbol) {
                                                            pending_snapshots.push_back(symbol);
                                                        }
                                                        try_send_next_snapshot(
                                                            &mut ws_tx,
                                                            &mut fsms,
                                                            &mut pending_snapshots,
                                                            &mut active_snapshot_req,
                                                            &mut snapshot_counter,
                                                            &config,
                                                            now,
                                                        ).await;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        HuobiMessageKind::TradeTick { ch, .. } => {
                            if is_paused || !config.streams.trades {
                                continue 'frame_loop;
                            }

                            if let Some(sym_str) = extract_symbol_from_channel(ch) {
                                let symbol = Symbol::from(sym_str);
                                if let Some(fsm) = fsms.get(&symbol) {
                                    if let Some(tick_raw) = env.tick {
                                        if let Ok(trade_tick) = serde_json::from_str::<RawTradeTick<'_>>(tick_raw.get()) {
                                            for item in trade_tick.data {
                                                if let Ok((ex_ts, trade)) = normalize_trade(&item) {
                                                    let ctx = sink.next_context(
                                                        VenueId::Huobi,
                                                        &symbol,
                                                        fsm.tag(),
                                                        Some(ex_ts),
                                                        received_ts,
                                                        None,
                                                    );
                                                    if sink.trade(&ctx, &trade).is_err() {
                                                        break 'frame_loop;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        HuobiMessageKind::SubAck { id, subbed, status } => {
                            debug!(connection_id, id, subbed, status, "Subscription acknowledged");
                        }

                        HuobiMessageKind::Error { id, err_code, err_msg } => {
                            warn!(
                                connection_id,
                                ?id,
                                ?err_code,
                                ?err_msg,
                                "Received error from Huobi server"
                            );
                            if let Some((active_sym, active_id, _)) = &active_snapshot_req {
                                if Some(active_id.as_str()) == id {
                                    let sym = active_sym.clone();
                                    active_snapshot_req = None;
                                    if let Some(fsm) = fsms.get_mut(&sym) {
                                        fsm.record_snapshot_failure(now);
                                    }
                                    pending_snapshots.push_back(sym);
                                    try_send_next_snapshot(
                                        &mut ws_tx,
                                        &mut fsms,
                                        &mut pending_snapshots,
                                        &mut active_snapshot_req,
                                        &mut snapshot_counter,
                                        &config,
                                        now,
                                    ).await;
                                }
                            }
                        }

                        HuobiMessageKind::Other => {}
                    }

                    if sink.is_failed() {
                        break 'frame_loop;
                    }
                }
            }
        }

        if sink.is_failed() {
            warn!(connection_id, "Sink failed; terminating connection task");
            break 'reconnect_loop;
        }

        if is_paused {
            reconnect_attempt = 0;
            continue 'reconnect_loop;
        }

        if cancel.is_cancelled() {
            break 'reconnect_loop;
        }

        // On connection drop/disconnect, emit ConnectionLost for all symbols
        if !disconnect_emitted {
            for (sym, tag) in &symbols {
                sink.best_effort_emit_alert(
                    VenueId::Huobi,
                    sym,
                    *tag,
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

    if cancel.is_cancelled() {
        for (sym, tag) in &symbols {
            sink.best_effort_emit_alert(
                VenueId::Huobi,
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
    }

    if sink.is_failed() {
        Err(crate::sink_util::SinkError)
    } else {
        Ok(())
    }
}

/// Attempts to send the next pending snapshot request over WebSocket.
async fn try_send_next_snapshot<W>(
    ws_tx: &mut W,
    fsms: &mut HashMap<Symbol, SymbolFsm>,
    pending: &mut VecDeque<Symbol>,
    active: &mut Option<(Symbol, String, Instant)>,
    counter: &mut u64,
    config: &HuobiConfig,
    now: Instant,
) where
    W: SinkExt<Message> + Unpin,
    W::Error: std::fmt::Display,
{
    if active.is_some() || pending.is_empty() {
        return;
    }

    // Find the first symbol that is allowed to request a snapshot under cooldown
    let len = pending.len();
    for _ in 0..len {
        if let Some(sym) = pending.pop_front() {
            let can_req = if let Some(fsm) = fsms.get_mut(&sym) {
                fsm.can_request_snapshot(now)
            } else {
                false
            };

            if can_req {
                *counter = counter.saturating_add(1);
                let req_id = format!("snap-{}-{}", sym.as_str(), *counter);
                let frame = build_req_snapshot_frame(sym.as_str(), config.mbp_levels, &req_id);
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        symbol = %sym,
                        error = %e,
                        "Failed to send snapshot request frame"
                    );
                    pending.push_back(sym);
                } else {
                    *active = Some((sym, req_id, now));
                }
                return;
            } else {
                pending.push_back(sym);
            }
        }
    }
}
