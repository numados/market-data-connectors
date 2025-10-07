//! Connection driver and WebSocket lifecycle management for Coinbase Advanced Trade.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::{
    BookKind, DeliveryStamp, DisconnectKind, EventContext, GlobalSequenceCheck, IntegrityAlert,
    RunId, Side, SilenceWatchdog, StreamingSink, Symbol, SymbolTag, UnixNanos, VenueId,
    WatchdogAction,
};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::coinbase_advanced::config::CoinbaseAdvancedConfig;
use crate::coinbase_advanced::control::Command;
use crate::coinbase_advanced::fsm::CoinbaseAdvancedFsm;
use crate::coinbase_advanced::normalize::{
    normalize_delta_from_scratch, normalize_snapshot_from_scratch, normalize_trade,
    parse_rfc3339_nanos,
};
use crate::coinbase_advanced::raw::{
    CoinbaseAdvancedFrameSeed, CoinbaseAdvancedScratch, L2EventKind, RawEnvelope, RawTradeEvent,
    build_subscribe_frame, build_unsubscribe_frame,
};

/// Computes exponential reconnect backoff with a 30-second cap.
fn compute_backoff(attempt: u32) -> Duration {
    let base_ms = 200u64;
    let factor = 2u64.saturating_pow(attempt.min(8));
    Duration::from_millis(base_ms.saturating_mul(factor).min(30_000))
}

/// Runs a single Coinbase Advanced Trade WebSocket connection worker managing a slice of products.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: CoinbaseAdvancedConfig,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0xc0, 0x1b, 0xad, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let mut fsm = CoinbaseAdvancedFsm::new(symbols.clone(), config.resync_timeout);
    let mut scratch = CoinbaseAdvancedScratch::with_capacity(1_024);
    let mut reconnect_attempt = 0u32;
    let mut is_paused = false;

    // Collect product string slices for subscription messages
    let product_strs: Vec<&str> = symbols.iter().map(|(s, _)| s.as_str()).collect();

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
                        _ => {}
                    }
                }
            }
        }

        if reconnect_attempt > 0 {
            let backoff = compute_backoff(reconnect_attempt);
            debug!(
                connection_id,
                attempt = reconnect_attempt,
                delay_ms = backoff.as_millis(),
                "Backing off before reconnecting to Coinbase Advanced"
            );
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        info!(
            connection_id,
            url = %config.ws_url,
            "Connecting to Coinbase Advanced Trade WebSocket"
        );

        let ws_stream = match tokio::time::timeout(
            config.connection_timeout,
            connect_async(config.ws_url.as_str()),
        )
        .await
        {
            Ok(Ok((stream, _response))) => {
                reconnect_attempt = 0;
                stream
            }
            Ok(Err(e)) => {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to connect to Coinbase Advanced WebSocket"
                );
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
            Err(_) => {
                warn!(
                    connection_id,
                    "Connection attempt to Coinbase Advanced timed out"
                );
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Reset FSM and emit ResyncStarted for all products
        fsm.reset();
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::CoinbaseAdvanced,
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

        // Send subscribe frames in order: level2, heartbeats, optional market_trades
        if config.streams.level2 {
            let sub_l2 = build_subscribe_frame("level2", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_l2)).await {
                warn!(connection_id, error = %e, "Failed to send level2 subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

        if config.streams.heartbeats {
            let sub_hb = build_subscribe_frame("heartbeats", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_hb)).await {
                warn!(connection_id, error = %e, "Failed to send heartbeats subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

        if config.streams.market_trades {
            let sub_trades = build_subscribe_frame("market_trades", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_trades)).await {
                warn!(connection_id, error = %e, "Failed to send market_trades subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

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
                            venue: VenueId::CoinbaseAdvanced,
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
                        Some(Command::ResyncProduct(sym)) => {
                            if let Some((tag, alert)) = fsm.trigger_manual_resync(&sym) {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::CoinbaseAdvanced,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, alert);

                                let sym_str = sym.as_str();
                                let unsub = build_unsubscribe_frame("level2", &[sym_str]);
                                let sub = build_subscribe_frame("level2", &[sym_str]);
                                let _ = ws_tx.send(Message::Text(unsub)).await;
                                let _ = ws_tx.send(Message::Text(sub)).await;
                            }
                        }

                        Some(Command::ResyncAll) => {
                            let (should_send, alerts) = fsm.trigger_resync_all(Instant::now());
                            for (sym, tag, alert) in alerts {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::CoinbaseAdvanced,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, alert);
                            }

                            if should_send {
                                let unsub = build_unsubscribe_frame("level2", &product_strs);
                                let sub = build_subscribe_frame("level2", &product_strs);
                                let _ = ws_tx.send(Message::Text(unsub)).await;
                                let _ = ws_tx.send(Message::Text(sub)).await;
                            }
                        }

                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            break 'frame_loop;
                        }

                        Some(Command::Pause) => {
                            is_paused = true;
                            info!(connection_id, "Operator paused connector");
                        }

                        Some(Command::Resume) => {
                            is_paused = false;
                            info!(connection_id, "Operator resumed connector");
                        }

                        None => break 'frame_loop,
                    }
                }

                _ = watchdog_ticker.tick() => {
                    let now = Instant::now();
                    match watchdog.poll(now) {
                        WatchdogAction::Healthy => {}
                        WatchdogAction::SendPing => {
                            let _ = ws_tx.send(Message::Ping(Vec::new())).await;
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            warn!(
                                connection_id,
                                ?silent_for,
                                "Silence watchdog timeout on Coinbase Advanced; forcing reconnect"
                            );
                            break 'frame_loop;
                        }
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

                    let (text, received_ts) = match &msg {
                        Message::Text(t) => (t.as_str(), UnixNanos::now()),
                        Message::Ping(p) => {
                            let _ = ws_tx.send(Message::Pong(p.clone())).await;
                            continue 'frame_loop;
                        }
                        Message::Pong(_) => continue 'frame_loop,
                        Message::Close(_) => {
                            info!(connection_id, "Server sent close frame; reconnecting");
                            break 'frame_loop;
                        }
                        Message::Binary(_) | Message::Frame(_) => continue 'frame_loop,
                    };

                    let now = Instant::now();
                    watchdog.on_data_frame(now);

                    // Parse envelope
                    let env: RawEnvelope<'_> = match serde_json::from_str(text) {
                        Ok(e) => e,
                        Err(err) => {
                            warn!(connection_id, error = %err, "Malformed JSON envelope; skipping");
                            continue 'frame_loop;
                        }
                    };

                    // Check connection-wide sequence number
                    if let Some(seq) = env.sequence_num {
                        match fsm.check_sequence(seq) {
                            GlobalSequenceCheck::First | GlobalSequenceCheck::Valid => {}
                            GlobalSequenceCheck::OldDataIgnored => {
                                debug!(
                                    connection_id,
                                    seq,
                                    "Old or duplicate envelope sequence dropped"
                                );
                                continue 'frame_loop;
                            }
                            GlobalSequenceCheck::GapDetected { expected, received } => {
                                warn!(
                                    connection_id,
                                    expected,
                                    received,
                                    "Connection-wide sequence gap detected on Coinbase Advanced"
                                );

                                let (should_resubscribe, alerts) = fsm.on_gap(expected, received, now);
                                for (sym, tag, [gap_alert, start_alert]) in alerts {
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::CoinbaseAdvanced,
                                        symbol: &sym,
                                        tag,
                                        exchange_ts: env.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok()),
                                        received_ts,
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: Some(received),
                                    };
                                    let _ = sink.alert(&ctx, gap_alert);

                                    delivery_ordinal += 1;
                                    let start_ctx = EventContext {
                                        venue: VenueId::CoinbaseAdvanced,
                                        symbol: &sym,
                                        tag,
                                        exchange_ts: None,
                                        received_ts: UnixNanos::now(),
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: None,
                                    };
                                    let _ = sink.alert(&start_ctx, start_alert);
                                }

                                if should_resubscribe {
                                    let unsub = build_unsubscribe_frame("level2", &product_strs);
                                    let sub = build_subscribe_frame("level2", &product_strs);
                                    let _ = ws_tx.send(Message::Text(unsub)).await;
                                    let _ = ws_tx.send(Message::Text(sub)).await;
                                }

                                continue 'frame_loop;
                            }
                        }
                    }

                    if is_paused {
                        continue 'frame_loop;
                    }

                    // Dispatch according to channel
                    match env.channel {
                        "heartbeats" => {
                            // Watchdog already stamped above
                        }

                        "subscriptions" => {
                            debug!(connection_id, "Subscription acknowledged");
                        }

                        "error" => {
                            warn!(connection_id, payload = %text, "Error frame received from Coinbase Advanced");
                        }

                        "l2_data" => {
                            // Use stream deserialization directly into scratch for zero heap allocations
                            let mut de = serde_json::Deserializer::from_str(text);
                            let header_res = CoinbaseAdvancedFrameSeed::new(&mut scratch).deserialize(&mut de);

                            match header_res {
                                Ok(header) => {
                                    if let Some(p_id) = header.product_id {
                                        let sym = Symbol::from(p_id);
                                        if let Some(tag) = fsm.tag(&sym) {
                                            match header.l2_kind {
                                                Some(L2EventKind::Snapshot) => {
                                                    if let Ok((exchange_ts, snapshot)) = normalize_snapshot_from_scratch(&scratch, header.timestamp) {
                                                        delivery_ordinal += 1;
                                                        let ctx = EventContext {
                                                            venue: VenueId::CoinbaseAdvanced,
                                                            symbol: &sym,
                                                            tag,
                                                            exchange_ts: Some(exchange_ts),
                                                            received_ts,
                                                            connection_id,
                                                            delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                            source_sequence: header.sequence_num,
                                                        };

                                                        let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                                        for l in &snapshot.bids {
                                                            let _ = sink.level(Side::Bid, l.price, l.volume);
                                                        }
                                                        for l in &snapshot.asks {
                                                            let _ = sink.level(Side::Ask, l.price, l.volume);
                                                        }
                                                        let _ = sink.end_book(&ctx, None);

                                                        if let Some(completed_alert) = fsm.on_snapshot(&sym) {
                                                            delivery_ordinal += 1;
                                                            let comp_ctx = EventContext {
                                                                venue: VenueId::CoinbaseAdvanced,
                                                                symbol: &sym,
                                                                tag,
                                                                exchange_ts: None,
                                                                received_ts: UnixNanos::now(),
                                                                connection_id,
                                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                                source_sequence: None,
                                                            };
                                                            let _ = sink.alert(&comp_ctx, completed_alert);
                                                        }
                                                    }
                                                }

                                                Some(L2EventKind::Update) => {
                                                    if fsm.is_in_sync(&sym) {
                                                        if let Ok((exchange_ts, delta)) = normalize_delta_from_scratch(&scratch, header.timestamp) {
                                                            delivery_ordinal += 1;
                                                            let ctx = EventContext {
                                                                venue: VenueId::CoinbaseAdvanced,
                                                                symbol: &sym,
                                                                tag,
                                                                exchange_ts: Some(exchange_ts),
                                                                received_ts,
                                                                connection_id,
                                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                                source_sequence: header.sequence_num,
                                                            };

                                                            let _ = sink.begin_book(&ctx, BookKind::Delta);
                                                            for l in &delta.bids {
                                                                let _ = sink.level(Side::Bid, l.price, l.volume);
                                                            }
                                                            for l in &delta.asks {
                                                                let _ = sink.level(Side::Ask, l.price, l.volume);
                                                            }
                                                            let _ = sink.end_book(&ctx, None);
                                                        }
                                                    } else {
                                                        debug!(connection_id, symbol = %sym, "L2 update dropped while awaiting snapshot");
                                                    }
                                                }

                                                None => {}
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(connection_id, error = %e, "Failed to stream-deserialize l2_data frame");
                                }
                            }
                        }

                        "market_trades" => {
                            if config.streams.market_trades {
                                for raw_ev in &env.events {
                                    if let Ok(trade_ev) = serde_json::from_str::<RawTradeEvent<'_>>(raw_ev.get()) {
                                        for t in &trade_ev.trades {
                                            let sym = Symbol::from(t.product_id);
                                            if let Some(tag) = fsm.tag(&sym) {
                                                if let Ok((exchange_ts, canonical_trade)) = normalize_trade(t) {
                                                    delivery_ordinal += 1;
                                                    let ctx = EventContext {
                                                        venue: VenueId::CoinbaseAdvanced,
                                                        symbol: &sym,
                                                        tag,
                                                        exchange_ts: Some(exchange_ts),
                                                        received_ts,
                                                        connection_id,
                                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                        source_sequence: env.sequence_num,
                                                    };
                                                    let _ = sink.trade(&ctx, &canonical_trade);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        _ => {
                            debug!(connection_id, channel = env.channel, "Unhandled channel frame");
                        }
                    }
                }
            }
        }

        // Emit ConnectionLost when socket breaks
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::CoinbaseAdvanced,
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
                    reason: DisconnectKind::Closed,
                },
            );
        }

        reconnect_attempt += 1;
    }
}
