//! Connection task and cancellable polling loop for VWD SNAPQUOTE.

use std::collections::HashMap;
use std::time::Duration;

use mdc_core::{
    BookKind, BookTicker, DeliveryStamp, EventContext, IntegrityAlert, PriceLevel, RunId, Side,
    SourceErrorKind, StreamingSink, Symbol, SymbolTag, VenueId,
};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::vwd::config::VwdConfig;
use crate::vwd::control::Command;
use crate::vwd::normalize::normalize_symbol_from_scratch;
use crate::vwd::raw::{VwdScratch, VwdScratchSeed};
use crate::vwd::rest::VwdRestClient;

/// Calculates poll interval with ±10% jitter.
pub fn calculate_jittered_interval(base: Duration, step: u64) -> Duration {
    let base_millis = base.as_millis() as u64;
    if base_millis == 0 {
        return Duration::ZERO;
    }
    // Determinisitc pseudo-jitter sequence: [-10% .. +10%]
    let delta_pct = ((step.wrapping_mul(2_654_435_761) % 21) as i64) - 10;
    let millis = (base_millis as i64) + (base_millis as i64 * delta_pct / 100);
    Duration::from_millis(millis.max(1) as u64)
}

/// Runs the continuous VWD REST polling loop.
pub async fn run_connection_task<S: StreamingSink>(
    config: VwdConfig,
    mut sink: S,
    mut cmd_rx: mpsc::Receiver<Command>,
    cancel_token: CancellationToken,
) {
    let client = match VwdRestClient::new(&config) {
        Ok(c) => c,
        Err(e) => {
            warn!(venue = "vwd", error = %e, "failed to initialize REST client");
            return;
        }
    };

    let run_id = RunId::from_bytes([0x76, 0x77, 0x64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let mut delivery_ordinal: u64 = 0;
    let mut paused = false;
    let mut scratch = VwdScratch::with_capacity(32);
    let mut stale_map: HashMap<Symbol, bool> = HashMap::new();
    for sym in &config.symbols {
        stale_map.insert(sym.symbol.clone(), true);
    }

    let mut step: u64 = 0;

    // Execute first poll immediately upon start
    execute_poll_cycle(
        &client,
        &config,
        &mut sink,
        &mut scratch,
        &mut stale_map,
        &mut delivery_ordinal,
        run_id,
    )
    .await;

    while !cancel_token.is_cancelled() {
        let sleep_duration = calculate_jittered_interval(config.poll_interval, step);
        step = step.wrapping_add(1);

        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!(venue = "vwd", "polling task cancelled");
                break;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::PollNow) => {
                        if !paused {
                            execute_poll_cycle(
                                &client,
                                &config,
                                &mut sink,
                                &mut scratch,
                                &mut stale_map,
                                &mut delivery_ordinal,
                                run_id,
                            ).await;
                        }
                    }
                    Some(Command::Pause) => {
                        debug!(venue = "vwd", "polling task paused");
                        paused = true;
                    }
                    Some(Command::Resume) => {
                        debug!(venue = "vwd", "polling task resumed");
                        paused = false;
                        execute_poll_cycle(
                            &client,
                            &config,
                            &mut sink,
                            &mut scratch,
                            &mut stale_map,
                            &mut delivery_ordinal,
                            run_id,
                        ).await;
                    }
                    None => {
                        debug!(venue = "vwd", "control command channel closed");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(sleep_duration) => {
                if !paused {
                    execute_poll_cycle(
                        &client,
                        &config,
                        &mut sink,
                        &mut scratch,
                        &mut stale_map,
                        &mut delivery_ordinal,
                        run_id,
                    ).await;
                }
            }
        }
    }
}

async fn execute_poll_cycle<S: StreamingSink>(
    client: &VwdRestClient,
    config: &VwdConfig,
    sink: &mut S,
    scratch: &mut VwdScratch,
    stale_map: &mut HashMap<Symbol, bool>,
    delivery_ordinal: &mut u64,
    run_id: RunId,
) {
    let poll_result = client.fetch_snap_quotes().await;

    let poll_resp = match poll_result {
        Ok(resp) => resp,
        Err(err_kind) => {
            // Transport or HTTP status failure affects ALL symbols
            for sym in &config.symbols {
                *delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::Vwd,
                    symbol: &sym.symbol,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: crate::now_nanos(),
                    connection_id: 1,
                    delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                    source_sequence: None,
                };
                let _ = sink.alert(&ctx, IntegrityAlert::SourceError { kind: err_kind });
                stale_map.insert(sym.symbol.clone(), true);
            }
            return;
        }
    };

    let body_str = match std::str::from_utf8(&poll_resp.body) {
        Ok(s) => s,
        Err(_) => {
            for sym in &config.symbols {
                *delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::Vwd,
                    symbol: &sym.symbol,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: poll_resp.received_ts,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                    source_sequence: None,
                };
                let _ = sink.alert(
                    &ctx,
                    IntegrityAlert::SourceError {
                        kind: SourceErrorKind::Parse,
                    },
                );
                stale_map.insert(sym.symbol.clone(), true);
            }
            return;
        }
    };

    let mut deserializer = serde_json::Deserializer::from_str(body_str);
    if VwdScratchSeed::new(scratch)
        .deserialize(&mut deserializer)
        .is_err()
    {
        for sym in &config.symbols {
            *delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Vwd,
                symbol: &sym.symbol,
                tag: SymbolTag::default(),
                exchange_ts: None,
                received_ts: poll_resp.received_ts,
                connection_id: 1,
                delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(
                &ctx,
                IntegrityAlert::SourceError {
                    kind: SourceErrorKind::Parse,
                },
            );
            stale_map.insert(sym.symbol.clone(), true);
        }
        return;
    }

    if scratch.quotes.is_empty() {
        for sym in &config.symbols {
            *delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Vwd,
                symbol: &sym.symbol,
                tag: SymbolTag::default(),
                exchange_ts: None,
                received_ts: poll_resp.received_ts,
                connection_id: 1,
                delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                source_sequence: None,
            };
            let _ = sink.alert(
                &ctx,
                IntegrityAlert::SourceError {
                    kind: SourceErrorKind::MissingQuote,
                },
            );
            stale_map.insert(sym.symbol.clone(), true);
        }
        return;
    }

    // Process each configured symbol individually
    for sym in &config.symbols {
        *delivery_ordinal += 1;
        match normalize_symbol_from_scratch(sym, scratch, config.significant_digits) {
            Ok(norm) => {
                let ctx = EventContext {
                    venue: VenueId::Vwd,
                    symbol: &sym.symbol,
                    tag: SymbolTag::default(),
                    exchange_ts: norm.exchange_ts,
                    received_ts: poll_resp.received_ts,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                    source_sequence: None,
                };

                // Emit full L2Snapshot
                let _ = sink.begin_book(&ctx, BookKind::Snapshot);
                let _ = sink.level(Side::Bid, norm.bid, config.indicative_size);
                let _ = sink.level(Side::Ask, norm.ask, config.indicative_size);
                let _ = sink.end_book(&ctx, None);

                if config.emit_book_ticker {
                    let ticker = BookTicker::new(
                        PriceLevel::new(norm.bid, config.indicative_size),
                        PriceLevel::new(norm.ask, config.indicative_size),
                        None,
                    );
                    let _ = sink.book_ticker(&ctx, &ticker);
                }

                // If symbol was stale, emit ResyncCompleted alert
                let was_stale = stale_map.get(&sym.symbol).copied().unwrap_or(true);
                if was_stale {
                    let _ = sink.alert(
                        &ctx,
                        IntegrityAlert::ResyncCompleted {
                            last_update_id: None,
                        },
                    );
                    stale_map.insert(sym.symbol.clone(), false);
                }
            }
            Err(err_kind) => {
                let ctx = EventContext {
                    venue: VenueId::Vwd,
                    symbol: &sym.symbol,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: poll_resp.received_ts,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
                    source_sequence: None,
                };
                let _ = sink.alert(&ctx, IntegrityAlert::SourceError { kind: err_kind });
                stale_map.insert(sym.symbol.clone(), true);
            }
        }
    }
}
