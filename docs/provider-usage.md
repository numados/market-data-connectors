# Provider usage

This guide describes the public integration boundary in the current workspace and the consumer contract around it. If you are integrating from a separate Rust application, start with [Getting started from another Rust application](getting-started.md), then use this guide for sink contracts, projections, alerts, and provider selection.

## 1. Two integrity layers

The connector and the application have different responsibilities.

| Layer | Owned by | What it does |
|---|---|---|
| Provider integrity | `mdc-providers` and `mdc-core` | Parses a venue feed, applies the venue's sequence/timestamp/checksum/heartbeat rules, emits `IntegrityAlert` values, and performs the provider's recovery flow. |
| Application projection | Your application or optional `mdc-sinks` | Builds a book, rejects crossed or incomplete quotes, applies maximum-age rules, stores data, and runs business logic. |

`TopOfBookProjector` is optional. You can consume provider events directly and implement a different book or freshness algorithm. Skipping the projector does not remove provider-side sequence, checksum, heartbeat, or transport handling.

## 2. Direct event consumption

`MarketDataSink` receives owned `MarketEvent<MarketDataKind>` values. `FnSink` is a closure adapter. For builders that require a cloneable sink, wrap `FnSink` in `Arc`. The method for owned sinks differs between [provider API families](getting-started.md#3-provider-apis-and-symbols).

This example uses the Binance API as currently implemented and selects only book-ticker and trade streams. With `depth: false`, Binance does not request REST depth snapshots or run depth reconciliation. The transport watchdog still exists.

```rust,no_run
use std::sync::Arc;

use mdc_core::{FnSink, MarketDataKind, MarketEvent, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector, BinanceStreams};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = BinanceConfig::default();
    config.streams = BinanceStreams {
        depth: false,
        book_ticker: true,
        trade: true,
    };

    let sink = Arc::new(FnSink::new(|event: MarketEvent<MarketDataKind>| {
        match &event.kind {
            MarketDataKind::BookTicker(ticker) => {
                println!("{} bid={} ask={}", event.symbol, ticker.best_bid.price, ticker.best_ask.price);
            }
            MarketDataKind::Trade(trade) => {
                println!("{} trade {} @ {}", event.symbol, trade.volume, trade.price);
            }
            MarketDataKind::IntegrityAlert(alert) => {
                println!("{} alert: {:?}", event.symbol, alert);
            }
            _ => {}
        }
        Ok::<(), std::convert::Infallible>(())
    }));

    let cancel = CancellationToken::new();
    let (connector, _handle) = BinanceConnector::builder(config)
        .symbols([Symbol::from("BTCUSDT")])
        .sink(sink)
        .build()?;

    connector.run(cancel).await?;
    Ok(())
}
```

The callback runs synchronously on the connector task. It must not block. A sink error stops the producing connection. See [lifecycle and errors](getting-started.md#5-lifecycle-controls-and-errors) for task results and best-effort failure alerts. This example only demonstrates the callback; the complete application adds shutdown handling.

## 3. Low-allocation streaming sink

`StreamingSink` is the lower-allocation contract. It receives `begin_book`, one `level` callback per price level, and `end_book` for a book update. Trade, ticker, reference-rate, and alert callbacks receive borrowed payloads. The context contains the venue, symbol, timestamps, source sequence, connection ID, and delivery stamp.

For this implementation, add `rust_decimal = "1"` to the application's dependencies because its method signatures name `Decimal` directly. The no-op example shows the contract; replace its callbacks with application processing.

```rust,no_run
use mdc_core::{
    BookKind, BookTicker, EventContext, IntegrityAlert, ReferenceRate, SequenceRange, Side,
    StreamingSink, Trade,
};
use rust_decimal::Decimal;

#[derive(Clone, Default)]
struct ApplicationSink;

impl StreamingSink for ApplicationSink {
    type Error = std::convert::Infallible;

    fn begin_book(&mut self, _ctx: &EventContext<'_>, _kind: BookKind) -> Result<(), Self::Error> {
        Ok(())
    }

    fn level(&mut self, _side: Side, _price: Decimal, _volume: Decimal) -> Result<(), Self::Error> {
        Ok(())
    }

    fn end_book(
        &mut self,
        _ctx: &EventContext<'_>,
        _seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn trade(&mut self, _ctx: &EventContext<'_>, _trade: &Trade) -> Result<(), Self::Error> {
        Ok(())
    }

    fn book_ticker(
        &mut self,
        _ctx: &EventContext<'_>,
        _ticker: &BookTicker,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn reference_rate(
        &mut self,
        _ctx: &EventContext<'_>,
        _rate: &ReferenceRate,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn alert(
        &mut self,
        _ctx: &EventContext<'_>,
        _alert: IntegrityAlert,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
```

The provider implementations reuse parsing scratch buffers and the streaming contract avoids constructing an owned event for every individual book level. The exact allocation behavior is provider-specific; use the repository benchmarks before making latency claims for an application sink.

For checksum-capable feeds, implement `level_raw` when the consumer needs the venue's original price and volume tokens. The default method delegates to `level`.

## 4. Book projection and application-owned freshness

Use `TopOfBookProjector` when you want the supplied L2 book and quote checks:

1. while `ResyncStarted` is active, a recovery snapshot is staged until `ResyncCompleted`; an authoritative full snapshot can also recover the projector directly when no resync is active;
2. deltas are applied through `L2Book`;
3. crossed books, empty sides, invalid prices/volumes, and depth-limit failures produce `QuoteProjection::Unavailable`;
4. valid quotes produce `QuoteProjection::Available`;
5. `effective_ts` represents the older current bid/ask timestamp.

The projector does not start a timer or expire a quote by age. For an age decision at processing time, the consumer compares `effective_ts` with its current clock; `received_ts` is the provider's ingestion/dispatch timestamp and can help measure transport delay. The exact capture point is provider-specific. The consumer should also use its own timer if a quote must expire while the feed is quiet.

```rust,no_run
use mdc_core::{MarketDataKind, MarketEvent, Symbol, SymbolTag, UnixNanos};
use mdc_sinks::{QuoteProjection, TopOfBookProjector};

fn process(events: &[MarketEvent<MarketDataKind>], now: UnixNanos) {
    let symbol = Symbol::from("BTCUSDT");
    let mut projector = TopOfBookProjector::new_l2(symbol, SymbolTag::default(), None);
    let max_age_ns = 5_000_000_000;

    for event in events {
        if let Some(projection) = projector.handle_event(event) {
            match projection {
                QuoteProjection::Available {
                    bid,
                    ask,
                    effective_ts,
                    ..
                } => {
                    let age_ns = now.0.saturating_sub(effective_ts.0);
                    if age_ns <= max_age_ns {
                        println!("live bid={bid} ask={ask}");
                    }
                }
                QuoteProjection::Unavailable { reason, .. } => {
                    println!("withhold quote: {reason:?}");
                }
            }
        }
    }
}
```

For native `BookTicker` events, use `TopOfBookProjector::new_ticker`. `ReferenceRate` events are not projected by `TopOfBookProjector`; handle them directly.

## 5. Asynchronous application logic

Connector callbacks cannot await database writes or HTTP calls. `loss_intolerant_channel` bridges them to an async consumer.
The [complete application](getting-started.md#2-run-a-complete-application) shows the manifest, channel creation, event loop, and bounded shutdown.
Use `stream.recv().await` directly. No `futures-util` dependency is needed for this method.

The channel contract is:

1. Cloned senders share one bounded FIFO queue. Events from different connection tasks can interleave in that queue.
2. The second argument to `loss_intolerant_channel` is a diagnostic label for terminal faults, not a connection filter.
3. On overflow, the sender records one terminal `DeliveryFault` and cancels the producer token immediately.
4. The receiver can then drain the accepted prefix, receive the fault once, and reach EOF. No further events are accepted after the fault.
5. Dropping the receiver immediately cancels the producer. A subsequent send reports the receiver-closed fault; a dropped receiver cannot observe it.
6. After all senders are dropped, the receiver drains any remaining events and reaches EOF.

The application chooses whether to drain or discard pending events on local shutdown.
A terminal delivery fault invalidates the affected run's derived state, even if some accepted events remain queued.
Cancellation alone does not tell the application whether delivery overflowed. Observe the stream fault and task result.
Async application work still needs bounded execution time so that shutdown and event consumption can proceed.

## 6. Alerts and validity

`IntegrityAlert` is exhaustive. Its current variants describe these transitions:

| Alert | Consumer implication |
|---|---|
| `GapDetected` | Sequence continuity failed. Withhold book-derived quotes until recovery. |
| `ChecksumMismatch` | The local book differs from the venue's checksum. Invalidate the derived book. |
| `SilenceTimeout`, `HeartbeatTimeout` | Transport or heartbeat health failed. Do not assume retained data is current. |
| `ResyncStarted` | Recovery is in progress. Stage snapshots and withhold derived quotes. |
| `ResyncCompleted` | Provider recovery completed. Projection validation and application age limits still apply. |
| `Quarantined` | Recovery is deferred. Keep the book invalid during the retry interval. |
| `SourceError` | The polling response or source failed. Withhold affected data until valid delivery. |
| `ConnectionLost` | The symbol's connection stopped, paused, or closed. Invalidate live state. |
| `SinkFailure` | Delivery failed. Notification is best effort because it uses the sink that failed. |

`ConnectionLost` carries `DisconnectKind`. Match it and other non-exhaustive types such as `SourceErrorKind` and `MdcError` with a wildcard fallback.
A provider emits only alerts relevant to its protocol and state.
Trade, ticker, and reference-rate payloads are borrowed in `StreamingSink`; alerts are passed by value.
See the [alert definitions](../crates/mdc-core/src/model/alert.rs) for payload fields.

## 7. Provider selection

The provider feature names and public configuration types are listed in [`crates/mdc-providers/Cargo.toml`](../crates/mdc-providers/Cargo.toml). The following table is a short selection guide; the linked module rustdoc and configuration types are authoritative for exact fields and defaults.

| Provider | Feature | Streams or polling output | Main provider-side behavior |
|---|---|---|---|
| Binance Spot | `binance` | `depth`, `book_ticker`, `trade` | REST depth snapshots, `[U, u]` reconciliation, silence watchdog |
| Kraken v2 | `kraken` | `book`, `trade` | CRC32 book verification, resubscription and quarantine |
| Coinbase Exchange | `coinbase` | `level2_batch`, `heartbeat`, `matches` | Heartbeat/sequence monitoring and level-2 resubscription |
| Coinbase Advanced Trade | `coinbase-advanced` | `level2`, `heartbeats`, `market_trades` | Connection-wide sequence tracking and resubscription |
| Bitfinex v2 | `bitfinex` | `book`, `trades` | Signed CRC32 book verification and channel recovery |
| Gate.io Spot v4 | `gateio` | `spot.obu`, `spot.trades` | Sequence continuity and in-place resubscription |
| Bitstamp v2 | `bitstamp` | `diff_order_book_*`, `live_trades_*` | Timestamp-gated snapshot reconciliation |
| Huobi / HTX Spot | `huobi` | MBP book and trades | Sequence continuity, in-band snapshot requests, bounded gzip handling |
| LMAX Digital | `lmax` | `ORDER_BOOK`, `TRADE` | Full-book replacement and silence monitoring |
| VWD / Infront | `vwd` | REST `SNAPQUOTE` polling | Validated indicative quotes, optional inverse and scaling transforms |
| Frankfurter | `frankfurter` | REST reference-rate polling | Complete configured currency batches and UTC date normalization |

For exact current builder methods, read the provider module and its `connector.rs`, `config.rs`, and `control.rs` files. The providers do not share one universal control-handle API. In particular, VWD and Frankfurter are polling connectors and do not expose the WebSocket reconnect commands used by several streaming providers.

## 8. Custom policies and boundaries

Some providers expose custom resynchronization policies through their builders. Kraken exposes a checksum strategy. These are provider-specific extension points; they do not form one universal policy API.

There is no general public switch that removes sequence/checksum validation from an L2 stream while preserving the same L2 stream. If an application does not need depth data, select a provider's ticker or trade stream where available. If it needs depth data, keep the provider's integrity path and implement custom book/freshness/business logic after the sink.

The connector does not provide order entry, persistence, a message broker, or an application-specific business workflow. VWD emits indicative FX quotes, and Frankfurter emits reference rates; neither represents executable order-book or trade data.

## 9. Local verification

`cargo doc` renders rustdoc, but does not compile its examples. `cargo test --doc` checks rustdoc examples. Neither command automatically checks Markdown fences in `README.md` or `docs/`. Compile those snippets in an external consumer crate with the selected features, as described in [Getting started](getting-started.md#6-consumer-policy-and-verification).

Run the repository checks before publishing an integration:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
cargo test --workspace --doc --all-features --locked
```
