//! Huobi (HTX) WebSocket connection worker and frame protocol loop.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, L2Delta, MarketEvent, RunId, SequenceRange,
    Side, Symbol, SymbolTag, UnixNanos, VenueId,
};
use mdc_core::resync::{DeltaDecision, SnapshotOutcome};
use mdc_core::sink::{BookKind, EventContext, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
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

/// Runs the Huobi connection task until cancelled.
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: HuobiConfig,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0x68, 0x75, 0x6f, 0x62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let mut fsms: HashMap<Symbol, SymbolFsm> = symbols
        .iter()
        .map(|(s, tag)| (s.clone(), SymbolFsm::new(s.clone(), *tag, &config)))
        .collect();

    let mut scratch = HuobiConnectionScratch::with_capacity(1_024);
    let mut gzip_decoder = GzipDecoder::with_capacity(64 * 1024);
    let mut reconnect_attempt = 0u32;
    let mut is_paused = false;

    'reconnect_loop: loop {
        if cancel.is_cancelled() {
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
            let backoff_secs = (1u64 << reconnect_attempt.min(5)).min(30);
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

        let ws_stream = match connect_async(config.ws_url.as_str()).await {
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

            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Huobi,
                symbol: sym,
                tag: *tag,
                exchange_ts: None,
                received_ts: UnixNanos::now(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
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

        'frame_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    for (sym, tag) in &symbols {
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Huobi,
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
                    break 'reconnect_loop;
                }

                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::ResyncSymbol(sym)) => {
                            if let Some(fsm) = fsms.get_mut(&sym) {
                                let alert = fsm.trigger_resync(Instant::now());
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Huobi,
                                    symbol: &sym,
                                    tag: fsm.tag(),
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, alert);
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
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            is_paused = true;
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
                        WatchdogAction::Reconnect { .. } => {
                            warn!(
                                connection_id,
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
                            let ts = UnixNanos::now();
                            match gzip_decoder.decompress(bytes) {
                                Ok(decompressed) => (decompressed, ts),
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
                            let ts = UnixNanos::now();
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

                                    if status == "ok" {
                                        if let Some(data_raw) = env.data {
                                            if let Some(fsm) = fsms.get_mut(&active_symbol) {
                                                let mut de = serde_json::Deserializer::from_str(data_raw.get());
                                                let header_res = MbpSeed::new(&mut scratch).deserialize(&mut de);
                                                match header_res {
                                                    Ok(header) => {
                                                        delivery_ordinal += 1;
                                                        let delivery = DeliveryStamp::new(
                                                            run_id,
                                                            connection_id as u64,
                                                            delivery_ordinal,
                                                        );
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
                                                                // Emit snapshot
                                                                delivery_ordinal += 1;
                                                                let ctx = EventContext {
                                                                    venue: VenueId::Huobi,
                                                                    symbol: &active_symbol,
                                                                    tag: fsm.tag(),
                                                                    exchange_ts: ts.map(UnixNanos::from_millis),
                                                                    received_ts,
                                                                    connection_id,
                                                                    delivery: DeliveryStamp::new(
                                                                        run_id,
                                                                        connection_id as u64,
                                                                        delivery_ordinal,
                                                                    ),
                                                                    source_sequence: Some(header.seq_num),
                                                                };
                                                                let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                                                for l in &scratch.bids {
                                                                    let _ = sink.level(Side::Bid, l.price, l.volume);
                                                                }
                                                                for l in &scratch.asks {
                                                                    let _ = sink.level(Side::Ask, l.price, l.volume);
                                                                }
                                                                let _ = sink.end_book(&ctx, None);

                                                                // Replay buffered deltas
                                                                for replayed_event in replay {
                                                                    delivery_ordinal += 1;
                                                                    let rep_ctx = EventContext {
                                                                        venue: VenueId::Huobi,
                                                                        symbol: &active_symbol,
                                                                        tag: fsm.tag(),
                                                                        exchange_ts: replayed_event.exchange_ts,
                                                                        received_ts: replayed_event.received_ts,
                                                                        connection_id,
                                                                        delivery: DeliveryStamp::new(
                                                                            run_id,
                                                                            connection_id as u64,
                                                                            delivery_ordinal,
                                                                        ),
                                                                        source_sequence: replayed_event.source_sequence,
                                                                    };
                                                                    let _ = sink.begin_book(&rep_ctx, BookKind::Delta);
                                                                    for l in &replayed_event.kind.bids {
                                                                        let _ = sink.level(Side::Bid, l.price, l.volume);
                                                                    }
                                                                    for l in &replayed_event.kind.asks {
                                                                        let _ = sink.level(Side::Ask, l.price, l.volume);
                                                                    }
                                                                    let _ = sink.end_book(&rep_ctx, replayed_event.kind.sequence);
                                                                }

                                                                // Alert ResyncCompleted
                                                                delivery_ordinal += 1;
                                                                let comp_ctx = EventContext {
                                                                    venue: VenueId::Huobi,
                                                                    symbol: &active_symbol,
                                                                    tag: fsm.tag(),
                                                                    exchange_ts: None,
                                                                    received_ts: UnixNanos::now(),
                                                                    connection_id,
                                                                    delivery: DeliveryStamp::new(
                                                                        run_id,
                                                                        connection_id as u64,
                                                                        delivery_ordinal,
                                                                    ),
                                                                    source_sequence: None,
                                                                };
                                                                let _ = sink.alert(
                                                                    &comp_ctx,
                                                                    IntegrityAlert::ResyncCompleted {
                                                                        last_update_id: Some(header.seq_num),
                                                                    },
                                                                );
                                                            }
                                                            Ok(SnapshotOutcome::StaleSnapshot { .. }) => {
                                                                warn!(
                                                                    connection_id,
                                                                    symbol = %active_symbol,
                                                                    "Stale snapshot received; re-requesting after cooldown"
                                                                );
                                                                fsm.record_snapshot_failure(now);
                                                                pending_snapshots.push_back(active_symbol.clone());
                                                            }
                                                            Ok(SnapshotOutcome::Ignored) => {}
                                                            Err(e) => {
                                                                warn!(
                                                                    connection_id,
                                                                    symbol = %active_symbol,
                                                                    error = %e,
                                                                    "Failed to process snapshot outcome"
                                                                );
                                                                fsm.record_snapshot_failure(now);
                                                                pending_snapshots.push_back(active_symbol.clone());
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
                                        }
                                    } else {
                                        warn!(
                                            connection_id,
                                            symbol = %active_symbol,
                                            rep,
                                            status,
                                            "Snapshot request returned non-ok status"
                                        );
                                        if let Some(fsm) = fsms.get_mut(&active_symbol) {
                                            fsm.record_snapshot_failure(now);
                                        }
                                        pending_snapshots.push_back(active_symbol);
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
                                                        delivery_ordinal += 1;
                                                        let ctx = EventContext {
                                                            venue: VenueId::Huobi,
                                                            symbol: &symbol,
                                                            tag: fsm.tag(),
                                                            exchange_ts: Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            connection_id,
                                                            delivery: DeliveryStamp::new(
                                                                run_id,
                                                                connection_id as u64,
                                                                delivery_ordinal,
                                                            ),
                                                            source_sequence: Some(header.seq_num),
                                                        };
                                                        let _ = sink.begin_book(&ctx, BookKind::Delta);
                                                        for l in &scratch.bids {
                                                            let _ = sink.level(Side::Bid, l.price, l.volume);
                                                        }
                                                        for l in &scratch.asks {
                                                            let _ = sink.level(Side::Ask, l.price, l.volume);
                                                        }
                                                        let _ = sink.end_book(&ctx, Some(range));
                                                    }
                                                    DeltaDecision::Buffer => {
                                                        delivery_ordinal += 1;
                                                        let event = MarketEvent {
                                                            venue: VenueId::Huobi,
                                                            symbol: symbol.clone(),
                                                            exchange_ts: Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            tag: fsm.tag(),
                                                            delivery: DeliveryStamp::new(
                                                                run_id,
                                                                connection_id as u64,
                                                                delivery_ordinal,
                                                            ),
                                                            source_sequence: Some(header.seq_num),
                                                            kind: L2Delta {
                                                                sequence: Some(range),
                                                                bids: scratch.bids.clone(),
                                                                asks: scratch.asks.clone(),
                                                            },
                                                        };
                                                        fsm.buffer_delta(event);
                                                    }
                                                    DeltaDecision::Drop => {
                                                        // Stale or duplicate delta; dropped silently
                                                    }
                                                    DeltaDecision::GapDetected { alert } => {
                                                        delivery_ordinal += 1;
                                                        let gap_ctx = EventContext {
                                                            venue: VenueId::Huobi,
                                                            symbol: &symbol,
                                                            tag: fsm.tag(),
                                                            exchange_ts: Some(UnixNanos::from_millis(ts)),
                                                            received_ts,
                                                            connection_id,
                                                            delivery: DeliveryStamp::new(
                                                                run_id,
                                                                connection_id as u64,
                                                                delivery_ordinal,
                                                            ),
                                                            source_sequence: Some(header.seq_num),
                                                        };
                                                        let _ = sink.alert(&gap_ctx, alert);

                                                        let start_alert = fsm.trigger_resync(now);
                                                        delivery_ordinal += 1;
                                                        let start_ctx = EventContext {
                                                            venue: VenueId::Huobi,
                                                            symbol: &symbol,
                                                            tag: fsm.tag(),
                                                            exchange_ts: None,
                                                            received_ts: UnixNanos::now(),
                                                            connection_id,
                                                            delivery: DeliveryStamp::new(
                                                                run_id,
                                                                connection_id as u64,
                                                                delivery_ordinal,
                                                            ),
                                                            source_sequence: None,
                                                        };
                                                        let _ = sink.alert(&start_ctx, start_alert);

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
                                                    delivery_ordinal += 1;
                                                    let ctx = EventContext {
                                                        venue: VenueId::Huobi,
                                                        symbol: &symbol,
                                                        tag: fsm.tag(),
                                                        exchange_ts: Some(ex_ts),
                                                        received_ts,
                                                        connection_id,
                                                        delivery: DeliveryStamp::new(
                                                            run_id,
                                                            connection_id as u64,
                                                            delivery_ordinal,
                                                        ),
                                                        source_sequence: None,
                                                    };
                                                    let _ = sink.trade(&ctx, &trade);
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
                }
            }
        }

        // On connection drop/disconnect, emit ConnectionLost for all symbols
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Huobi,
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
                    reason: DisconnectKind::TransportError,
                },
            );
        }
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
