//! Connection task and cancellable polling loop for Frankfurter ECB reference rates.

use std::collections::HashSet;
use std::time::Duration;

use mdc_core::{
    DeliveryStamp, DisconnectKind, EventContext, IntegrityAlert, ReferenceRate, RunId,
    SourceErrorKind, StreamingSink, SymbolTag, VenueId,
    integrity::embargo::{EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP},
};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::frankfurter::config::FrankfurterConfig;
use crate::frankfurter::control::Command;
use crate::frankfurter::normalize::{
    NormalizedBatchRate, build_pair_symbol, normalize_batch_from_scratch,
};
use crate::frankfurter::raw::{FrankfurterScratch, FrankfurterScratchSeed};
use crate::frankfurter::rest::{FrankfurterRestClient, HttpPollError};

const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Runs the continuous Frankfurter REST polling loop.
pub async fn run_connection_task<S: StreamingSink>(
    config: FrankfurterConfig,
    client: FrankfurterRestClient,
    sink: S,
    cmd_rx: mpsc::Receiver<Command>,
    cancel_token: CancellationToken,
) {
    ConnectionTask::new(config, client, sink)
        .run(cmd_rx, cancel_token)
        .await;
}

struct ConnectionTask<S> {
    config: FrankfurterConfig,
    client: FrankfurterRestClient,
    sink: S,
    run_id: RunId,
    connection_gen: u64,
    delivery_ordinal: u64,
    active_quotes: Vec<String>,
    paused: bool,
    in_sync: bool,
    scratch: FrankfurterScratch,
    normalized_buf: Vec<NormalizedBatchRate>,
    backoff_duration: Duration,
    retry_wait: Option<Duration>,
}

impl<S: StreamingSink> ConnectionTask<S> {
    fn new(config: FrankfurterConfig, client: FrankfurterRestClient, sink: S) -> Self {
        let run_id = RunId::from_bytes([
            0x66, 0x72, 0x61, 0x6e, 0x6b, 0x66, 0x75, 0x72, 0x74, 0x65, 0x72, 0, 0, 0, 0, 1,
        ]);
        let active_quotes = config.quote_currencies.clone();
        Self {
            config,
            client,
            sink,
            run_id,
            connection_gen: 1,
            delivery_ordinal: 0,
            active_quotes,
            paused: false,
            in_sync: false,
            scratch: FrankfurterScratch::with_capacity(32),
            normalized_buf: Vec::with_capacity(32),
            backoff_duration: Duration::from_secs(1),
            retry_wait: None,
        }
    }

    async fn run(&mut self, mut cmd_rx: mpsc::Receiver<Command>, cancel_token: CancellationToken) {
        // Initial startup: emit ResyncStarted and execute first poll immediately
        self.emit_resync_started();
        self.execute_poll().await;

        while !cancel_token.is_cancelled() {
            let sleep_duration = if let Some(wait) = self.retry_wait.take() {
                wait
            } else {
                self.config.poll_interval
            };

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(venue = "frankfurter_ecb", "polling task cancelled");
                    break;
                }
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(Command::PollNow) => {
                            if !self.paused {
                                self.execute_poll().await;
                            }
                        }
                        Some(Command::Pause) => {
                            debug!(venue = "frankfurter_ecb", "polling task paused");
                            self.paused = true;
                            self.emit_connection_lost(DisconnectKind::Paused);
                        }
                        Some(Command::Resume) => {
                            debug!(venue = "frankfurter_ecb", "polling task resumed");
                            self.paused = false;
                            self.connection_gen += 1;
                            self.delivery_ordinal = 0;
                            self.in_sync = false;

                            self.emit_resync_started();
                            self.execute_poll().await;
                        }
                        Some(Command::SetSymbols(new_quotes)) => {
                            debug!(venue = "frankfurter_ecb", ?new_quotes, "updating symbol quotes");
                            let old_set: HashSet<String> = self.active_quotes.iter().cloned().collect();
                            let new_set: HashSet<String> = new_quotes.iter().cloned().collect();

                            // Invalidate removed symbols
                            for removed_quote in old_set.difference(&new_set) {
                                if let Some(sym) = build_pair_symbol(&self.config.base_currency, removed_quote) {
                                    self.delivery_ordinal += 1;
                                    let ctx = EventContext {
                                        venue: VenueId::FrankfurterEcb,
                                        symbol: &sym,
                                        tag: SymbolTag::default(),
                                        exchange_ts: None,
                                        received_ts: mdc_core::UnixNanos::now(),
                                        connection_id: 1,
                                        delivery: DeliveryStamp::new(self.run_id, self.connection_gen, self.delivery_ordinal),
                                        source_sequence: None,
                                    };
                                    let _ = self.sink.alert(&ctx, IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Closed,
                                    });
                                }
                            }

                            self.active_quotes = new_quotes;
                            self.in_sync = false;

                            if !self.paused {
                                self.emit_resync_started();
                                self.execute_poll().await;
                            }
                        }
                        None => {
                            debug!(venue = "frankfurter_ecb", "control command channel closed");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(sleep_duration) => {
                    if !self.paused {
                        self.execute_poll().await;
                    }
                }
            }
        }
    }

    async fn execute_poll(&mut self) {
        let fetch_result = self
            .client
            .fetch_rates(&self.config.base_currency, &self.active_quotes)
            .await;

        let poll_resp = match fetch_result {
            Ok(resp) => {
                self.backoff_duration = Duration::from_secs(1);
                resp
            }
            Err(err) => {
                let (source_err, wait) = match err {
                    HttpPollError::HttpStatus {
                        status: 429,
                        retry_after,
                    } => {
                        let wait = retry_after
                            .unwrap_or(EMBARGO_FALLBACK_429)
                            .min(EMBARGO_SAFETY_CAP);
                        (SourceErrorKind::HttpStatus(429), Some(wait))
                    }
                    HttpPollError::HttpStatus { status, .. } if status >= 500 => {
                        let wait = self.backoff_duration;
                        self.backoff_duration = (self.backoff_duration * 2).min(MAX_BACKOFF);
                        (SourceErrorKind::HttpStatus(status), Some(wait))
                    }
                    HttpPollError::HttpStatus { status, .. } => {
                        (SourceErrorKind::HttpStatus(status), None)
                    }
                    HttpPollError::Transport
                    | HttpPollError::OversizedBody
                    | HttpPollError::ForeignRedirect => {
                        let wait = self.backoff_duration;
                        self.backoff_duration = (self.backoff_duration * 2).min(MAX_BACKOFF);
                        (SourceErrorKind::Transport, Some(wait))
                    }
                };

                self.emit_source_error(source_err);
                self.in_sync = false;
                self.retry_wait = wait;
                return;
            }
        };

        let body_str = match std::str::from_utf8(&poll_resp.body) {
            Ok(s) => s,
            Err(_) => {
                self.emit_source_error(SourceErrorKind::Parse);
                self.in_sync = false;
                return;
            }
        };

        let mut deserializer = serde_json::Deserializer::from_str(body_str);
        if FrankfurterScratchSeed::new(&mut self.scratch)
            .deserialize(&mut deserializer)
            .is_err()
        {
            self.emit_source_error(SourceErrorKind::Parse);
            self.in_sync = false;
            return;
        }

        if let Err(err_kind) = normalize_batch_from_scratch(
            &self.config.base_currency,
            &self.active_quotes,
            &self.scratch,
            &mut self.normalized_buf,
        ) {
            self.emit_source_error(err_kind);
            self.in_sync = false;
            return;
        }

        let needs_resync_completed = !self.in_sync;

        // Emit ReferenceRate in canonical symbol order
        for item in self.normalized_buf.iter() {
            self.delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::FrankfurterEcb,
                symbol: &item.symbol,
                tag: SymbolTag::default(),
                exchange_ts: Some(item.exchange_ts),
                received_ts: poll_resp.received_ts,
                connection_id: 1,
                delivery: DeliveryStamp::new(
                    self.run_id,
                    self.connection_gen,
                    self.delivery_ordinal,
                ),
                source_sequence: None,
            };
            let _ = self
                .sink
                .reference_rate(&ctx, &ReferenceRate::new(item.mid));
        }

        // If recovering or initial delivery, emit ResyncCompleted for each delivered symbol
        if needs_resync_completed {
            for item in self.normalized_buf.iter() {
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::FrankfurterEcb,
                    symbol: &item.symbol,
                    tag: SymbolTag::default(),
                    exchange_ts: Some(item.exchange_ts),
                    received_ts: poll_resp.received_ts,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(
                        self.run_id,
                        self.connection_gen,
                        self.delivery_ordinal,
                    ),
                    source_sequence: None,
                };
                let _ = self.sink.alert(
                    &ctx,
                    IntegrityAlert::ResyncCompleted {
                        last_update_id: None,
                    },
                );
            }
            self.in_sync = true;
        }
    }

    fn emit_resync_started(&mut self) {
        let now = mdc_core::UnixNanos::now();
        for quote in &self.active_quotes {
            if let Some(sym) = build_pair_symbol(&self.config.base_currency, quote) {
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::FrankfurterEcb,
                    symbol: &sym,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: now,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(
                        self.run_id,
                        self.connection_gen,
                        self.delivery_ordinal,
                    ),
                    source_sequence: None,
                };
                let _ = self.sink.alert(&ctx, IntegrityAlert::ResyncStarted);
            }
        }
    }

    fn emit_source_error(&mut self, kind: SourceErrorKind) {
        let now = mdc_core::UnixNanos::now();
        for quote in &self.active_quotes {
            if let Some(sym) = build_pair_symbol(&self.config.base_currency, quote) {
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::FrankfurterEcb,
                    symbol: &sym,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: now,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(
                        self.run_id,
                        self.connection_gen,
                        self.delivery_ordinal,
                    ),
                    source_sequence: None,
                };
                let _ = self.sink.alert(&ctx, IntegrityAlert::SourceError { kind });
            }
        }
    }

    fn emit_connection_lost(&mut self, reason: DisconnectKind) {
        let now = mdc_core::UnixNanos::now();
        for quote in &self.active_quotes {
            if let Some(sym) = build_pair_symbol(&self.config.base_currency, quote) {
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::FrankfurterEcb,
                    symbol: &sym,
                    tag: SymbolTag::default(),
                    exchange_ts: None,
                    received_ts: now,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(
                        self.run_id,
                        self.connection_gen,
                        self.delivery_ordinal,
                    ),
                    source_sequence: None,
                };
                let _ = self
                    .sink
                    .alert(&ctx, IntegrityAlert::ConnectionLost { reason });
            }
        }
    }
}
