//! LMAX Digital WebSocket per-connection worker task and protocol loop.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use mdc_core::model::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, RunId, Side, Symbol, SymbolTag, VenueId,
};
use mdc_core::sink::{BookKind, EventContext, StreamingSink};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::lmax::config::LmaxConfig;
use crate::lmax::control::Command;
use crate::lmax::normalize::{normalize_trade, parse_rfc3339_nanos};
use crate::lmax::raw::{
    LmaxBookFrameSeed, LmaxConnectionScratch, LmaxFrameKind, RawControl, RawTrade,
    SubscribeMessage, peek_frame_kind,
};

/// Builds a JSON subscribe message frame for the target channel and instruments.
pub fn build_subscribe_frame(channel: &str, instruments: &[String]) -> String {
    let req = SubscribeMessage::subscribe(channel, instruments);
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON unsubscribe message frame for the target channel and instruments.
pub fn build_unsubscribe_frame(channel: &str, instruments: &[String]) -> String {
    let req = SubscribeMessage::unsubscribe(channel, instruments);
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Runs a single LMAX Digital WebSocket connection worker managing a slice of symbols.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: LmaxConfig,
    mut sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0x1a, 0x4a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let mut scratch = LmaxConnectionScratch::with_capacity(512);

    let symbol_map: HashMap<String, Symbol> = symbols
        .iter()
        .map(|(s, _)| (s.as_str().to_lowercase(), s.clone()))
        .collect();
    let symbol_tags: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();

    let instrument_names: Vec<String> = symbols
        .iter()
        .map(|(s, _)| s.as_str().to_lowercase())
        .collect();

    let mut conn_backoff = Duration::from_millis(500);
    let max_conn_backoff = Duration::from_secs(10);

    'reconnect_loop: loop {
        if cancel.is_cancelled() {
            break 'reconnect_loop;
        }

        info!(
            connection_id,
            url = %config.ws_url,
            "Connecting to LMAX Digital WebSocket"
        );

        let ws_stream = match connect_async(config.ws_url.as_str()).await {
            Ok((stream, _resp)) => {
                info!(connection_id, "Connected to LMAX Digital WebSocket");
                conn_backoff = Duration::from_millis(500);
                stream
            }
            Err(e) => {
                warn!(
                    connection_id,
                    error = %e,
                    retry_in = ?conn_backoff,
                    "Failed to connect to LMAX Digital WebSocket"
                );
                tokio::select! {
                    _ = cancel.cancelled() => break 'reconnect_loop,
                    _ = tokio::time::sleep(conn_backoff) => {
                        conn_backoff = (conn_backoff * 2).min(max_conn_backoff);
                        continue 'reconnect_loop;
                    }
                }
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Control pacing: wait initial_delay before sending SUBSCRIBE
        info!(
            connection_id,
            delay = ?config.control_pacing.initial_delay,
            "Waiting initial control pacing delay before subscription"
        );
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = ws_tx.close().await;
                break 'reconnect_loop;
            }
            _ = tokio::time::sleep(config.control_pacing.initial_delay) => {}
        }

        let mut awaiting_first_snapshot: HashSet<Symbol> = HashSet::new();

        // Emit ResyncStarted for each symbol upon fresh connection
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Lmax,
                symbol: sym,
                tag: *tag,
                exchange_ts: None,
                received_ts: crate::now_nanos(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
            awaiting_first_snapshot.insert(sym.clone());
        }

        // Send subscribe frames
        if config.streams.book {
            let frame = build_subscribe_frame("ORDER_BOOK", &instrument_names);
            if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to send LMAX ORDER_BOOK subscribe frame"
                );
            }
        }

        if config.streams.trades {
            let frame = build_subscribe_frame("TRADE", &instrument_names);
            if let Err(e) = ws_tx.send(Message::Text(frame)).await {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to send LMAX TRADE subscribe frame"
                );
            }
        }

        let now_inst = Instant::now();
        let mut watchdog =
            SilenceWatchdog::new(config.ping_interval, config.silence_timeout, now_inst);
        let mut watchdog_ticker = tokio::time::interval(Duration::from_secs(1));
        watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut current_rate_limit_backoff = config.control_pacing.too_many_requests_backoff;
        let mut pending_resync_symbols: Vec<Symbol> = Vec::new();
        let mut resync_batch_timer: Option<tokio::time::Instant> = None;

        'frame_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    for (sym, tag) in &symbols {
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Lmax,
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

                // Silence watchdog
                _ = watchdog_ticker.tick() => {
                    let now = Instant::now();
                    match watchdog.poll(now) {
                        WatchdogAction::Healthy => {}
                        WatchdogAction::SendPing => {
                            let _ = ws_tx.send(Message::Ping(Vec::new())).await;
                        }
                        WatchdogAction::Reconnect { .. } => {
                            warn!(
                                connection_id,
                                "Silence watchdog timeout on LMAX connection; reconnecting"
                            );
                            break 'frame_loop;
                        }
                    }
                }

                // Batching timer for pending resync requests
                _ = async {
                    match resync_batch_timer {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    resync_batch_timer = None;
                    if !pending_resync_symbols.is_empty() {
                        let to_resync: Vec<Symbol> = std::mem::take(&mut pending_resync_symbols);
                        let names: Vec<String> = to_resync.iter().map(|s| s.as_str().to_lowercase()).collect();

                        // 1. Send UNSUBSCRIBE
                        let unsubs = build_unsubscribe_frame("ORDER_BOOK", &names);
                        let _ = ws_tx.send(Message::Text(unsubs)).await;

                        // 2. Wait min_command_interval
                        tokio::time::sleep(config.control_pacing.min_command_interval).await;

                        // 3. Send SUBSCRIBE
                        let subs = build_subscribe_frame("ORDER_BOOK", &names);
                        let _ = ws_tx.send(Message::Text(subs)).await;
                    }
                }

                // Operator commands
                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::ResyncSymbol(sym)) => {
                            if let Some(tag) = symbol_tags.get(&sym) {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Lmax,
                                    symbol: &sym,
                                    tag: *tag,
                                    exchange_ts: None,
                                    received_ts: crate::now_nanos(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
                                awaiting_first_snapshot.insert(sym.clone());

                                if !pending_resync_symbols.contains(&sym) {
                                    pending_resync_symbols.push(sym);
                                }

                                if resync_batch_timer.is_none() {
                                    resync_batch_timer = Some(tokio::time::Instant::now() + Duration::from_millis(200));
                                }
                            }
                        }
                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            info!(connection_id, "Operator paused LMAX connection");
                            for (sym, tag) in &symbols {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Lmax,
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

                // WebSocket frames
                msg_opt = ws_rx.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "LMAX WebSocket stream error");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "LMAX WebSocket closed by peer");
                            break 'frame_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let now = Instant::now();
                            watchdog.on_data_frame(now);

                            let bytes = text.as_bytes();
                            match peek_frame_kind(bytes) {
                                LmaxFrameKind::OrderBook => {
                                    let mut de = serde_json::Deserializer::from_str(&text);
                                    let header = match LmaxBookFrameSeed::new(&mut scratch).deserialize(&mut de) {
                                        Ok(h) => h,
                                        Err(e) => {
                                            warn!(connection_id, error = %e, "Failed to parse LMAX order book frame");
                                            continue;
                                        }
                                    };

                                    if let Some(sym) = symbol_map.get(header.instrument_id) {
                                        let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                        let exchange_ts = parse_rfc3339_nanos(header.timestamp).ok();

                                        delivery_ordinal += 1;
                                        let ctx = EventContext {
                                            venue: VenueId::Lmax,
                                            symbol: sym,
                                            tag,
                                            exchange_ts,
                                            received_ts: crate::now_nanos(),
                                            connection_id,
                                            delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                            source_sequence: None,
                                        };

                                        // Every book frame is emitted as an authoritative Snapshot
                                        let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                        for level in &scratch.bids {
                                            let _ = sink.level(Side::Bid, level.price, level.volume);
                                        }
                                        for level in &scratch.asks {
                                            let _ = sink.level(Side::Ask, level.price, level.volume);
                                        }
                                        let _ = sink.end_book(&ctx, None);

                                        // Complete resync lifecycle if awaiting initial snapshot
                                        if awaiting_first_snapshot.remove(sym) {
                                            let _ = sink.alert(
                                                &ctx,
                                                IntegrityAlert::ResyncCompleted {
                                                    last_update_id: None,
                                                },
                                            );
                                        }
                                    }
                                }
                                LmaxFrameKind::Trade => {
                                    if config.streams.trades {
                                        if let Ok(raw_trade) = serde_json::from_str::<RawTrade<'_>>(&text) {
                                            if let Some(sym) = symbol_map.get(raw_trade.instrument_id) {
                                                if let Ok((_trade_ts, canonical_trade)) = normalize_trade(&raw_trade) {
                                                    let tag = symbol_tags.get(sym).copied().unwrap_or_default();
                                                    let exchange_ts = parse_rfc3339_nanos(raw_trade.timestamp).ok();

                                                    delivery_ordinal += 1;
                                                    let ctx = EventContext {
                                                        venue: VenueId::Lmax,
                                                        symbol: sym,
                                                        tag,
                                                        exchange_ts,
                                                        received_ts: crate::now_nanos(),
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
                                LmaxFrameKind::Control => {
                                    if let Ok(ctrl) = serde_json::from_str::<RawControl<'_>>(&text) {
                                        if ctrl.kind == "SUBSCRIPTIONS" {
                                            debug!(connection_id, "LMAX subscription acknowledged");
                                        } else if ctrl.kind == "ERROR" {
                                            if ctrl.error_code == Some("TOO_MANY_REQUESTS") {
                                                warn!(
                                                    connection_id,
                                                    backoff = ?current_rate_limit_backoff,
                                                    "LMAX TOO_MANY_REQUESTS rate limit hit; backing off control sends"
                                                );
                                                tokio::time::sleep(current_rate_limit_backoff).await;
                                                current_rate_limit_backoff = (current_rate_limit_backoff * 2)
                                                    .min(config.control_pacing.max_backoff);
                                            } else {
                                                warn!(
                                                    connection_id,
                                                    error_code = ?ctrl.error_code,
                                                    error_message = ?ctrl.error_message,
                                                    "LMAX server error frame"
                                                );
                                            }
                                        }
                                    }
                                }
                                LmaxFrameKind::Unknown => {}
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_tx.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            info!(connection_id, "LMAX close frame received");
                            break 'frame_loop;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
