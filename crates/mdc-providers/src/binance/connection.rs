//! Binance WebSocket per-bucket connection worker task.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::{
    BookKind, CooldownPolicy, DeliveryStamp, DeltaDecision, DisconnectKind, EventContext,
    IntegrityAlert, MarketEvent, ResyncController, ResyncDecision, ResyncFailure, ResyncPolicy,
    RunId, SequenceRange, Side, SilenceWatchdog, SnapshotOutcome, StreamingSink, Symbol, SymbolTag,
    UnixNanos, VenueId, WatchdogAction,
};
use tokio::sync::{Semaphore, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use serde::de::DeserializeSeed;

use crate::binance::config::BinanceConfig;
use crate::binance::control::Command;
use crate::binance::normalize::{normalize_book_ticker, normalize_snapshot, normalize_trade};
use crate::binance::raw::{
    ConnectionScratchBuffers, DepthUpdateSeed, RawBookTicker, RawDepthSnapshot, RawTrade,
    StreamEnvelope,
};
use crate::binance::rest::{BinanceRestClient, RestError};

/// Constructs the combined WebSocket subscription URL for a slice of symbols.
pub fn build_combined_stream_url(
    base_url: &Url,
    symbols: &[(Symbol, SymbolTag)],
    config: &BinanceConfig,
) -> Url {
    let mut streams = Vec::new();
    for (sym, _) in symbols {
        let sym_lower = sym.as_str().to_lowercase();
        if config.streams.depth {
            streams.push(format!("{sym_lower}@depth@100ms"));
        }
        if config.streams.book_ticker {
            streams.push(format!("{sym_lower}@bookTicker"));
        }
        if config.streams.trade {
            streams.push(format!("{sym_lower}@trade"));
        }
    }

    let mut url = base_url.clone();
    url.set_path("/stream");
    let joined = streams.join("/");
    url.set_query(Some(&format!("streams={joined}")));
    url
}

/// Runs a single connection worker managing up to `symbols_per_connection` symbols.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: BinanceConfig,
    mut sink: S,
    rest_client: BinanceRestClient,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
) {
    let run_id = RunId::from_bytes([0xba, 0xba, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal = 0u64;

    let tag_map: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();
    let mut controllers: HashMap<Symbol, ResyncController> = symbols
        .iter()
        .map(|(s, _)| {
            (
                s.clone(),
                ResyncController::new(config.resync_buffer_capacity),
            )
        })
        .collect();

    let mut policy = CooldownPolicy::new(config.snapshot_retry_cooldown);
    let snapshot_semaphore = Arc::new(Semaphore::new(1));
    let (snapshot_tx, mut snapshot_rx) =
        mpsc::channel::<(Symbol, Result<RawDepthSnapshot, RestError>)>(16);

    let mut scratch = ConnectionScratchBuffers::with_capacity(1_024);
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
                    if let Some(Command::Resume) = cmd {
                        is_paused = false;
                        break;
                    }
                }
            }
        }

        // Exponential backoff delay
        if reconnect_attempt > 0 {
            let mult = 1u32.checked_shl(reconnect_attempt.min(4) - 1).unwrap_or(16);
            let backoff = (config.initial_backoff * mult).min(config.max_backoff);
            info!(
                connection_id,
                ?backoff,
                reconnect_attempt,
                "Reconnecting with backoff"
            );
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        let ws_url = build_combined_stream_url(&config.ws_base_url, &symbols, &config);
        info!(connection_id, %ws_url, "Connecting to Binance WebSocket");

        let ws_res = tokio_tungstenite::connect_async(ws_url.as_str()).await;
        let (mut ws_stream, _) = match ws_res {
            Ok(s) => {
                reconnect_attempt = 0;
                s
            }
            Err(e) => {
                reconnect_attempt += 1;
                warn!(connection_id, error = %e, "WebSocket connect failed");
                continue 'reconnect_loop;
            }
        };

        // Reset silence watchdog
        let now = Instant::now();
        let mut watchdog = SilenceWatchdog::new(
            config.watchdog_ping_after,
            config.watchdog_reconnect_after,
            now,
        );

        // Reset controllers, emit ResyncStarted, and trigger initial snapshots
        for (sym, tag) in &symbols {
            if let Some(ctrl) = controllers.get_mut(sym) {
                ctrl.reset();
            }

            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Binance,
                symbol: sym,
                tag: *tag,
                exchange_ts: None,
                received_ts: UnixNanos::now(),
                connection_id,
                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                source_sequence: None,
            };

            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
            policy.on_started(sym, now);

            // Spawn snapshot request
            spawn_snapshot_request(
                sym.clone(),
                rest_client.clone(),
                Arc::clone(&snapshot_semaphore),
                snapshot_tx.clone(),
                cancel.clone(),
            );
        }

        let tick_interval = (config.watchdog_ping_after / 2)
            .min(Duration::from_millis(100))
            .max(Duration::from_millis(5));
        let mut watchdog_ticker = tokio::time::interval(tick_interval);

        // Socket frame loop
        'frame_loop: loop {
            let now = Instant::now();

            tokio::select! {
                _ = cancel.cancelled() => {
                    for (sym, tag) in &symbols {
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Binance,
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
                    let _ = ws_stream.close(None).await;
                    break 'reconnect_loop;
                }

                // Operator commands
                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::Resync(sym)) => {
                            if let Some(ctrl) = controllers.get_mut(&sym) {
                                ctrl.reset();
                                if let Some(&tag) = tag_map.get(&sym) {
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::Binance,
                                        symbol: &sym,
                                        tag,
                                        exchange_ts: None,
                                        received_ts: UnixNanos::now(),
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: None,
                                    };
                                    let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
                                    policy.on_started(&sym, now);
                                    spawn_snapshot_request(
                                        sym,
                                        rest_client.clone(),
                                        Arc::clone(&snapshot_semaphore),
                                        snapshot_tx.clone(),
                                        cancel.clone(),
                                    );
                                }
                            }
                        }
                        Some(Command::ResyncAll) => {
                            for (sym, tag) in &symbols {
                                if let Some(ctrl) = controllers.get_mut(sym) {
                                    ctrl.reset();
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::Binance,
                                        symbol: sym,
                                        tag: *tag,
                                        exchange_ts: None,
                                        received_ts: UnixNanos::now(),
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: None,
                                    };
                                    let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
                                    policy.on_started(sym, now);
                                    spawn_snapshot_request(
                                        sym.clone(),
                                        rest_client.clone(),
                                        Arc::clone(&snapshot_semaphore),
                                        snapshot_tx.clone(),
                                        cancel.clone(),
                                    );
                                }
                            }
                        }
                        Some(Command::Reconnect(reason)) => {
                            info!(connection_id, %reason, "Operator reconnect requested");
                            break 'frame_loop;
                        }
                        Some(Command::Pause) => {
                            is_paused = true;
                            for (sym, tag) in &symbols {
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Binance,
                                    symbol: sym,
                                    tag: *tag,
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: None,
                                };
                                let _ = sink.alert(&ctx, IntegrityAlert::ConnectionLost { reason: DisconnectKind::Paused });
                            }
                            let _ = ws_stream.close(None).await;
                            break 'frame_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        None => {}
                    }
                }

                // Silence watchdog
                _ = watchdog_ticker.tick() => {
                    match watchdog.poll(Instant::now()) {
                        WatchdogAction::SendPing => {
                            debug!(connection_id, "Watchdog triggered ping");
                            let _ = ws_stream.send(Message::Ping(Vec::new())).await;
                        }
                        WatchdogAction::Reconnect { silent_for: _ } => {
                            warn!(connection_id, "Watchdog timeout: forcing reconnect");
                            break 'frame_loop;
                        }
                        WatchdogAction::Healthy => {}
                    }
                }

                // REST depth snapshot results
                snap_res_opt = snapshot_rx.recv() => {
                    if let Some((sym, res)) = snap_res_opt {
                        let tag = tag_map.get(&sym).copied().unwrap_or_default();
                        delivery_ordinal += 1;
                        let ctx = EventContext {
                            venue: VenueId::Binance,
                            symbol: &sym,
                            tag,
                            exchange_ts: None,
                            received_ts: UnixNanos::now(),
                            connection_id,
                            delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                            source_sequence: None,
                        };

                        match res {
                            Ok(raw_snapshot) => {
                                let snapshot = normalize_snapshot(&raw_snapshot);
                                let snapshot_event = MarketEvent {
                                    exchange_ts: None,
                                    received_ts: UnixNanos::now(),
                                    venue: VenueId::Binance,
                                    symbol: sym.clone(),
                                    tag,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: raw_snapshot.last_update_id.into(),
                                    kind: snapshot,
                                };

                                if let Some(ctrl) = controllers.get_mut(&sym) {
                                    match ctrl.on_snapshot(snapshot_event) {
                                        Ok(SnapshotOutcome::Synchronized { snapshot, replay }) => {
                                            policy.on_completed(&sym, now);
                                            // 1. Emit snapshot
                                            let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                                            for level in &snapshot.kind.bids {
                                                let _ = sink.level(Side::Bid, level.price, level.volume);
                                            }
                                            for level in &snapshot.kind.asks {
                                                let _ = sink.level(Side::Ask, level.price, level.volume);
                                            }
                                            let last_id = snapshot.kind.last_update_id.unwrap_or(0);
                                            let _ = sink.end_book(&ctx, Some(SequenceRange::new(last_id, last_id)));

                                            // 2. Replay buffered deltas
                                            for delta in replay {
                                                delivery_ordinal += 1;
                                                let delta_ctx = EventContext {
                                                    venue: VenueId::Binance,
                                                    symbol: &sym,
                                                    tag,
                                                    exchange_ts: delta.exchange_ts,
                                                    received_ts: delta.received_ts,
                                                    connection_id,
                                                    delivery: DeliveryStamp::new(
                                                        run_id,
                                                        connection_id as u64,
                                                        delivery_ordinal,
                                                    ),
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

                                            // 3. Emit ResyncCompleted
                                            delivery_ordinal += 1;
                                            let alert_ctx = EventContext {
                                                venue: VenueId::Binance,
                                                symbol: &sym,
                                                tag,
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
                                            let _ = sink.alert(&alert_ctx, IntegrityAlert::ResyncCompleted { last_update_id: Some(last_id) });
                                        }
                                        Ok(SnapshotOutcome::StaleSnapshot { .. }) => {
                                            debug!(%sym, "Stale REST snapshot received; scheduling refetch");
                                            policy.on_failed(&sym, &ResyncFailure::StaleSnapshot, now);
                                            spawn_delayed_snapshot(
                                                sym,
                                                &mut policy,
                                                now,
                                                rest_client.clone(),
                                                Arc::clone(&snapshot_semaphore),
                                                snapshot_tx.clone(),
                                                cancel.clone(),
                                            );
                                        }
                                        Ok(SnapshotOutcome::Ignored) => {}
                                        Err(e) => {
                                            warn!(%sym, error = %e, "Resync controller error on snapshot");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(%sym, error = %e, "REST depth snapshot fetch failed");
                                policy.on_failed(&sym, &ResyncFailure::Transport(e.to_string()), now);
                                spawn_delayed_snapshot(
                                    sym,
                                    &mut policy,
                                    now,
                                    rest_client.clone(),
                                    Arc::clone(&snapshot_semaphore),
                                    snapshot_tx.clone(),
                                    cancel.clone(),
                                );
                            }
                        }
                    }
                }

                // Incoming WebSocket messages
                ws_msg_opt = ws_stream.next() => {
                    let received_ts = UnixNanos::now();

                    let msg = match ws_msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "WebSocket error");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "WebSocket stream closed by exchange");
                            break 'frame_loop;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            let now = Instant::now();
                            watchdog.on_data_frame(now);

                            let env: StreamEnvelope<'_> = match serde_json::from_str(&text) {
                                Ok(e) => e,
                                Err(e) => {
                                    warn!(connection_id, error = %e, "Failed to parse Binance combined stream envelope");
                                    continue;
                                }
                            };

                            if env.stream.ends_with("@depth@100ms") || env.stream.ends_with("@depth") {
                                let mut de = serde_json::Deserializer::from_str(env.data.get());
                                let header = match DepthUpdateSeed::new(&mut scratch).deserialize(&mut de) {
                                    Ok(h) => h,
                                    Err(e) => {
                                        warn!(connection_id, error = %e, "Failed to parse Binance depth update");
                                        continue;
                                    }
                                };

                                let sym = Symbol::from(header.symbol);
                                let tag = tag_map.get(&sym).copied().unwrap_or_default();
                                let range = SequenceRange::new(header.first_update_id, header.last_update_id);
                                let exchange_ts = Some(UnixNanos::from_millis(header.event_time_ms));

                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Binance,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts,
                                    received_ts,
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: Some(header.last_update_id),
                                };

                                if let Some(ctrl) = controllers.get_mut(&sym) {
                                    match ctrl.check_delta(range) {
                                        DeltaDecision::Emit => {
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
                                            let delta = mdc_core::L2Delta {
                                                bids: scratch.bids.clone(),
                                                asks: scratch.asks.clone(),
                                                sequence: Some(range),
                                            };
                                            let event = MarketEvent {
                                                exchange_ts,
                                                received_ts,
                                                venue: VenueId::Binance,
                                                symbol: sym.clone(),
                                                tag,
                                                delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                                source_sequence: Some(header.last_update_id),
                                                kind: delta,
                                            };
                                            if let Err(e) = ctrl.buffer(event) {
                                                warn!(%sym, error = ?e, "Buffer overflow during delta buffering; requesting snapshot");
                                                let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
                                                policy.on_started(&sym, now);
                                                spawn_snapshot_request(
                                                    sym,
                                                    rest_client.clone(),
                                                    Arc::clone(&snapshot_semaphore),
                                                    snapshot_tx.clone(),
                                                    cancel.clone(),
                                                );
                                            }
                                        }
                                        DeltaDecision::GapDetected { alert } => {
                                            warn!(%sym, ?alert, "Sequence gap detected; starting resynchronization");
                                            let _ = sink.alert(&ctx, alert);
                                            let _ = sink.alert(&ctx, IntegrityAlert::ResyncStarted);
                                            policy.on_started(&sym, now);
                                            spawn_snapshot_request(
                                                sym,
                                                rest_client.clone(),
                                                Arc::clone(&snapshot_semaphore),
                                                snapshot_tx.clone(),
                                                cancel.clone(),
                                            );
                                        }
                                    }
                                }
                            } else if env.stream.ends_with("@bookTicker") {
                                let raw_ticker: RawBookTicker<'_> = match serde_json::from_str(env.data.get()) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!(connection_id, error = %e, "Failed to parse Binance book ticker");
                                        continue;
                                    }
                                };
                                let sym = Symbol::from(raw_ticker.symbol);
                                let tag = tag_map.get(&sym).copied().unwrap_or_default();
                                delivery_ordinal += 1;
                                let ctx = EventContext {
                                    venue: VenueId::Binance,
                                    symbol: &sym,
                                    tag,
                                    exchange_ts: None,
                                    received_ts,
                                    connection_id,
                                    delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                    source_sequence: Some(raw_ticker.update_id),
                                };
                                let ticker = normalize_book_ticker(&raw_ticker);
                                let _ = sink.book_ticker(&ctx, &ticker);
                            } else if env.stream.ends_with("@trade") {
                                let raw_trade: RawTrade<'_> = match serde_json::from_str(env.data.get()) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!(connection_id, error = %e, "Failed to parse Binance trade");
                                        continue;
                                    }
                                };
                                let sym = Symbol::from(raw_trade.symbol);
                                let tag = tag_map.get(&sym).copied().unwrap_or_default();
                                if let Ok((exchange_ts, trade)) = normalize_trade(&raw_trade) {
                                    delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::Binance,
                                        symbol: &sym,
                                        tag,
                                        exchange_ts: Some(exchange_ts),
                                        received_ts,
                                        connection_id,
                                        delivery: DeliveryStamp::new(run_id, connection_id as u64, delivery_ordinal),
                                        source_sequence: Some(raw_trade.trade_id),
                                    };
                                    let _ = sink.trade(&ctx, &trade);
                                }
                            }
                        }
                        Message::Ping(_) => {}
                        Message::Close(_) => {
                            info!(connection_id, "Received WebSocket close frame");
                            break 'frame_loop;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Connection dropped or broken: alert ConnectionLost and reconnect
        for (sym, tag) in &symbols {
            delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Binance,
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
        if !is_paused {
            reconnect_attempt += 1;
        } else {
            reconnect_attempt = 0;
        }
    }
}

fn spawn_snapshot_request(
    symbol: Symbol,
    rest_client: BinanceRestClient,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(Symbol, Result<RawDepthSnapshot, RestError>)>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = cancel.cancelled() => {}
            permit_res = semaphore.acquire() => {
                if let Ok(_permit) = permit_res {
                    let res = rest_client.fetch_depth(symbol.as_str(), Instant::now()).await;
                    let _ = tx.send((symbol, res)).await;
                }
            }
        }
    });
}

fn spawn_delayed_snapshot(
    symbol: Symbol,
    policy: &mut CooldownPolicy,
    now: Instant,
    rest_client: BinanceRestClient,
    semaphore: Arc<Semaphore>,
    tx: mpsc::Sender<(Symbol, Result<RawDepthSnapshot, RestError>)>,
    cancel: CancellationToken,
) {
    let delay = match policy.decide(&symbol, now) {
        ResyncDecision::Proceed => Duration::ZERO,
        ResyncDecision::Wait(until) | ResyncDecision::Quarantine(until) => {
            until.saturating_duration_since(now)
        }
    };

    tokio::spawn(async move {
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = tokio::time::sleep(delay) => {
                spawn_snapshot_request(symbol, rest_client, semaphore, tx, cancel);
            }
        }
    });
}
