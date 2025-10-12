//! Gate.io WebSocket per-connection worker task and protocol loop.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, MarketEvent, RunId, SequenceRange, Side, Symbol,
    SymbolTag, UnixNanos, VenueId,
};
use mdc_core::resync::{DeltaDecision, SnapshotOutcome};
use mdc_core::sink::{BookKind, EventContext, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::gateio::config::GateioConfig;
use crate::gateio::control::Command;
use crate::gateio::fsm::SymbolFsm;
use crate::gateio::normalize::normalize_trade;
use crate::gateio::raw::{
    GateioConnectionScratch, ObuSeed, RawEnvelope, RawSubAckError, RawSubAckResult, RawTrade,
    SpotPong, SubscribeRequest, extract_symbol,
};

/// Helper returning current Unix timestamp in whole seconds.
pub fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

/// Builds the payload name for an order book subscription (e.g. `"ob.BTC_USDT.400"`).
pub fn build_obu_payload(symbol: &str, levels: u32) -> String {
    format!("ob.{symbol}.{levels}")
}

/// Builds a JSON subscribe message frame for order book updates.
pub fn build_obu_subscribe_frame(symbol: &str, levels: u32, time_secs: u64) -> String {
    let payload = build_obu_payload(symbol, levels);
    let req = SubscribeRequest {
        time: time_secs,
        channel: "spot.obu",
        event: "subscribe",
        payload: vec![&payload],
    };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON unsubscribe message frame for order book updates.
pub fn build_obu_unsubscribe_frame(symbol: &str, levels: u32, time_secs: u64) -> String {
    let payload = build_obu_payload(symbol, levels);
    let req = SubscribeRequest {
        time: time_secs,
        channel: "spot.obu",
        event: "unsubscribe",
        payload: vec![&payload],
    };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON subscribe message frame for public trades.
pub fn build_trades_subscribe_frame(symbols: &[Symbol], time_secs: u64) -> String {
    let payloads: Vec<&str> = symbols.iter().map(Symbol::as_str).collect();
    let req = SubscribeRequest {
        time: time_secs,
        channel: "spot.trades",
        event: "subscribe",
        payload: payloads,
    };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Runs a single Gate.io WebSocket connection worker managing a slice of symbols.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: GateioConfig,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0x6a, 0x7e, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let mut fsms: HashMap<Symbol, SymbolFsm> = symbols
        .iter()
        .map(|(s, tag)| (s.clone(), SymbolFsm::new(s.clone(), *tag, &config)))
        .collect();

    let mut scratch = GateioConnectionScratch::with_capacity(1_024);
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
            "Connecting to Gate.io WebSocket"
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
                    "Failed to connect to Gate.io WebSocket; retrying"
                );
                continue 'reconnect_loop;
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Send initial subscriptions for all configured symbols
        let now_secs = current_unix_secs();
        for (sym, _) in &symbols {
            if config.streams.depth {
                let frame =
                    build_obu_subscribe_frame(sym.as_str(), config.orderbook_levels, now_secs);
                if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                    warn!(
                        connection_id,
                        symbol = %sym,
                        error = %e,
                        "Failed to send obu subscribe frame"
                    );
                    break;
                }
            }
        }

        if config.streams.trades {
            let symbol_list: Vec<Symbol> = symbols.iter().map(|(s, _)| s.clone()).collect();
            let frame = build_trades_subscribe_frame(&symbol_list, now_secs);
            if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to send trades subscribe frame"
                );
            }
        }

        // Gate.io server sends ping every ~30s; timeout if no data frames within 45s
        let now_inst = Instant::now();
        let mut watchdog =
            SilenceWatchdog::new(Duration::from_secs(30), Duration::from_secs(45), now_inst);
        let mut watchdog_ticker = tokio::time::interval(Duration::from_secs(1));
        watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        'frame_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    for (sym, tag) in &symbols {
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Gateio,
                            symbol: sym,
                            tag: *tag,
                            exchange_ts: None,
                            received_ts: crate::now_nanos(),
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
                        WatchdogAction::Healthy => {}
                        WatchdogAction::SendPing => {
                            let _ = ws_tx.send(Message::Ping(vec![])).await;
                        }
                        WatchdogAction::Reconnect { .. } => {
                            warn!(
                                connection_id,
                                "Silence watchdog timeout on Gate.io feed; forcing reconnect"
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
                                let alert = fsm.trigger_resync(now);
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Gateio,
                                    symbol: &sym,
                                    tag: fsm.tag(),
                                    exchange_ts: None,
                                    received_ts: crate::now_nanos(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, alert);

                                let now_secs = current_unix_secs();
                                let un_frame = build_obu_unsubscribe_frame(sym.as_str(), config.orderbook_levels, now_secs);
                                let sub_frame = build_obu_subscribe_frame(sym.as_str(), config.orderbook_levels, now_secs);
                                let _ = ws_tx.send(Message::Text(un_frame)).await;
                                let _ = ws_tx.send(Message::Text(sub_frame)).await;
                            }
                        }
                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            is_paused = true;
                            for (sym, tag) in &symbols {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Gateio,
                                    symbol: sym,
                                    tag: *tag,
                                    exchange_ts: None,
                                    received_ts: crate::now_nanos(),
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
                            break 'frame_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        None => break 'reconnect_loop,
                    }
                }

                // Inbound WebSocket messages
                msg_opt = ws_rx.next() => {
                    let received_ts = crate::now_nanos();

                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "Gate.io WebSocket stream error");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "Gate.io WebSocket connection closed by peer");
                            break 'frame_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let now = Instant::now();
                            watchdog.on_data_frame(now);

                            let env: RawEnvelope<'_> = match serde_json::from_str(&text) {
                                Ok(e) => e,
                                Err(e) => {
                                    warn!(connection_id, error = %e, "Failed to parse Gate.io envelope");
                                    continue;
                                }
                            };

                            // Heartbeat: spot.ping must be answered immediately with spot.pong
                            if env.channel == "spot.ping" {
                                let pong = SpotPong::new(env.time);
                                #[allow(clippy::unwrap_used)]
                                let pong_str = serde_json::to_string(&pong).unwrap();
                                if let Err(e) = ws_tx.send(Message::Text(pong_str)).await {
                                    warn!(connection_id, error = %e, "Failed to reply to spot.ping with spot.pong");
                                    break 'frame_loop;
                                }
                                continue;
                            }

                            // Order book updates
                            if env.channel == "spot.obu" {
                                if env.event == Some("subscribe") {
                                    if let Some(res_val) = env.result {
                                        if let Ok(ack) = serde_json::from_str::<RawSubAckResult<'_>>(res_val.get()) {
                                            if ack.status != "success" {
                                                let err_msg = env.error.and_then(|er| {
                                                    serde_json::from_str::<RawSubAckError<'_>>(er.get()).ok()
                                                });
                                                warn!(
                                                    connection_id,
                                                    status = ack.status,
                                                    error = ?err_msg,
                                                    "Subscription error acknowledged by Gate.io"
                                                );
                                            }
                                        }
                                    }
                                    continue;
                                }

                                if env.event == Some("update") {
                                    let Some(res_val) = env.result else {
                                        continue;
                                    };

                                    let mut de = serde_json::Deserializer::from_str(res_val.get());
                                    let header = match ObuSeed::new(&mut scratch).deserialize(&mut de) {
                                        Ok(h) => h,
                                        Err(e) => {
                                            warn!(connection_id, error = %e, "Failed to parse spot.obu result payload");
                                            continue;
                                        }
                                    };

                                    let sym_str = extract_symbol(header.s);
                                    let sym = Symbol::from(sym_str);

                                    let Some(fsm) = fsms.get_mut(&sym) else {
                                        continue;
                                    };

                                    let tag = fsm.tag();
                                    let exchange_ts = Some(UnixNanos::from_millis(header.t));

                                    // Full snapshot
                                    if header.full {
                                        delivery_ordinal += 1;
                                        let delivery = DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal);
                                        let outcome = fsm.on_snapshot(&header, &scratch, delivery, received_ts, now);
                                        match outcome {
                                            Ok(SnapshotOutcome::Synchronized { snapshot, replay }) => {
                                                let ctx = EventContext {
                                                    venue: VenueId::Gateio,
                                                    symbol: &sym,
                                                    tag,
                                                    exchange_ts,
                                                    received_ts,
                                                    connection_id,
                                                    delivery,
                                                    source_sequence: Some(header.last_id),
                                                };

                                                let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                                for level in &snapshot.kind.bids {
                                                    let _ = sink.level(Side::Bid, level.price, level.volume);
                                                }
                                                for level in &snapshot.kind.asks {
                                                    let _ = sink.level(Side::Ask, level.price, level.volume);
                                                }
                                                let _ = sink.end_book(&ctx, None);

                                                // Replay deltas buffered during resync
                                                for delta in replay {
                                                    delivery_ordinal += 1;
                                                    let delta_ctx = EventContext {
                                                        venue: VenueId::Gateio,
                                                        symbol: &sym,
                                                        tag,
                                                        exchange_ts: delta.exchange_ts,
                                                        received_ts: delta.received_ts,
                                                        connection_id,
                                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                        source_sequence: delta.source_sequence,
                                                    };
                                                    let _ = sink.begin_book(&delta_ctx, BookKind::Delta);
                                                    for level in &delta.kind.bids {
                                                        let _ = sink.level(Side::Bid, level.price, level.volume);
                                                    }
                                                    for level in &delta.kind.asks {
                                                        let _ = sink.level(Side::Ask, level.price, level.volume);
                                                    }
                                                    let _ = sink.end_book(&delta_ctx, delta.kind.sequence);
                                                }

                                                // Emit ResyncCompleted
                                                delivery_ordinal += 1;
                                                let alert_ctx = EventContext {
                                                    venue: VenueId::Gateio,
                                                    symbol: &sym,
                                                    tag,
                                                    exchange_ts,
                                                    received_ts,
                                                    connection_id,
                                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                    source_sequence: Some(header.last_id),
                                                };
                                                let _ = sink.alert(
                                                    &alert_ctx,
                                                    IntegrityAlert::ResyncCompleted {
                                                        last_update_id: Some(header.last_id),
                                                    },
                                                );
                                            }
                                            Ok(SnapshotOutcome::Ignored) => {}
                                            Ok(SnapshotOutcome::StaleSnapshot { .. }) => {
                                                warn!(%sym, "Stale snapshot received on Gate.io feed");
                                            }
                                            Err(e) => {
                                                warn!(%sym, error = %e, "Failed to apply snapshot to FSM");
                                            }
                                        }
                                        continue;
                                    }

                                    // Incremental delta
                                    let range = SequenceRange::new(header.first_id, header.last_id);
                                    match fsm.check_delta(range) {
                                        DeltaDecision::Emit => {
                                            delivery_ordinal += 1;
                                            let ctx = EventContext {
                                                venue: VenueId::Gateio,
                                                symbol: &sym,
                                                tag,
                                                exchange_ts,
                                                received_ts,
                                                connection_id,
                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                source_sequence: Some(header.last_id),
                                            };

                                            let _ = sink.begin_book(&ctx, BookKind::Delta);
                                            for level in &scratch.bids {
                                                let _ = sink.level(Side::Bid, level.price, level.volume);
                                            }
                                            for level in &scratch.asks {
                                                let _ = sink.level(Side::Ask, level.price, level.volume);
                                            }
                                            let _ = sink.end_book(&ctx, Some(range));
                                        }
                                        DeltaDecision::Drop => {}
                                        DeltaDecision::Buffer => {
                                            let delta = mdc_core::model::L2Delta {
                                                sequence: Some(range),
                                                bids: scratch.bids.clone(),
                                                asks: scratch.asks.clone(),
                                            };
                                            delivery_ordinal += 1;
                                            let event = MarketEvent {
                                                venue: VenueId::Gateio,
                                                symbol: sym.clone(),
                                                exchange_ts,
                                                received_ts,
                                                tag,
                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                source_sequence: Some(header.last_id),
                                                kind: delta,
                                            };
                                            fsm.buffer_delta(event);
                                        }
                                        DeltaDecision::GapDetected { alert } => {
                                            warn!(
                                                %sym,
                                                first = header.first_id,
                                                last = header.last_id,
                                                "Sequence gap detected on Gate.io feed; resubscribing"
                                            );
                                            let _ = fsm.trigger_resync(now);

                                            delivery_ordinal += 1;
                                            let ctx = EventContext {
                                                venue: VenueId::Gateio,
                                                symbol: &sym,
                                                tag,
                                                exchange_ts,
                                                received_ts,
                                                connection_id,
                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                source_sequence: Some(header.last_id),
                                            };
                                            let _ = sink.alert(&ctx, alert);

                                            if fsm.can_resubscribe(now) {
                                                let now_secs = current_unix_secs();
                                                let un_frame = build_obu_unsubscribe_frame(sym.as_str(), config.orderbook_levels, now_secs);
                                                let sub_frame = build_obu_subscribe_frame(sym.as_str(), config.orderbook_levels, now_secs);
                                                let _ = ws_tx.send(Message::Text(un_frame)).await;
                                                let _ = ws_tx.send(Message::Text(sub_frame)).await;
                                            } else {
                                                fsm.record_resubscribe_failure(now);
                                            }
                                        }
                                    }
                                }
                                continue;
                            }

                            // Public trades
                            if env.channel == "spot.trades" && env.event == Some("update") {
                                if let Some(res_val) = env.result {
                                    if let Ok(raw_trade) = serde_json::from_str::<RawTrade<'_>>(res_val.get()) {
                                        if let Ok((ex_ts, trade)) = normalize_trade(&raw_trade) {
                                            let sym = Symbol::from(raw_trade.currency_pair);
                                            let tag = fsms.get(&sym).map(SymbolFsm::tag).unwrap_or_default();
                                            delivery_ordinal += 1;
                                            let ctx = EventContext {
                                                venue: VenueId::Gateio,
                                                symbol: &sym,
                                                tag,
                                                exchange_ts: Some(ex_ts),
                                                received_ts,
                                                connection_id,
                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                source_sequence: None,
                                            };
                                            let _ = sink.trade(&ctx, &trade);
                                        }
                                    }
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_tx.send(Message::Pong(payload)).await;
                        }
                        Message::Pong(_) => {}
                        Message::Close(_) => {
                            info!(connection_id, "Received WebSocket Close frame");
                            break 'frame_loop;
                        }
                        Message::Binary(_) | Message::Frame(_) => {}
                    }
                }
            }
        }

        // Connection dropped: emit ConnectionLost and reset FSMs
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Gateio,
                symbol: sym,
                tag: *tag,
                exchange_ts: None,
                received_ts: crate::now_nanos(),
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

        for fsm in fsms.values_mut() {
            fsm.reset(&config);
        }
    }
}
