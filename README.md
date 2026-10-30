# Market Data Connectors

`market-data-connectors` is a Rust workspace for consuming public cryptocurrency and FX market data from several venues through one set of domain types.

It is useful when an application needs to:

- connect to more than one venue without keeping a separate event model for each wire protocol;
- detect order-book gaps, checksum failures, heartbeat loss, and socket silence before using a book;
- recover a book from a fresh snapshot instead of silently continuing with an invalid state;
- choose between an owned event callback and a lower-allocation streaming sink;
- keep application-specific storage, pricing, freshness, and business rules outside the connector;
- move events to an asynchronous consumer through a bounded, loss-intolerant handoff.

The workspace is a connector and normalization layer. It does not place orders and does not provide a database or a business-rule engine.

## Why this project exists

Venue feeds differ in message format, symbol names, sequence rules, checksums, snapshots, heartbeats, reconnect behavior, and timestamps. The workspace puts those differences behind provider adapters and emits a common `MarketEvent<MarketDataKind>` envelope.

The integration boundary is:

```text
venue protocol
    ↓
provider parser and integrity state machine
    ↓  normalized events and in-band IntegrityAlert values
application sink
    ↓
optional book/quote projection and application-owned freshness policy
    ↓
application state, storage, analytics, or business logic
```

This lets an application use the connector's wire-level integrity handling without being forced to use the supplied top-of-book projection. It can also replace downstream book, freshness, and business processing with its own code.

## What the workspace provides

- **One event model.** Providers emit `L2Snapshot`, `L2Delta`, `BookTicker`, `Trade`, `ReferenceRate`, and `IntegrityAlert` through `MarketEvent`.
- **Provider-specific integrity handling.** Depending on the venue, the connector validates sequence continuity, timestamps, checksums, heartbeats, or complete polling responses. Provider integrity failures trigger in-band alerts and the provider's recovery path rather than being silently presented as healthy data.
- **Snapshot recovery.** Providers that need snapshots buffer or reconcile updates according to the venue protocol and emit `ResyncStarted` and `ResyncCompleted` lifecycle alerts.
- **Two sink contracts.** `MarketDataSink` and `FnSink` deliver owned events. `StreamingSink` receives book levels incrementally and is the low-allocation integration point.
- **Application-owned projections.** `mdc-sinks` contains `L2Book` and `TopOfBookProjector` for applications that want a ready-to-use book projection. Maximum quote age remains an application decision.
- **Bounded asynchronous delivery.** `loss_intolerant_channel` bridges synchronous connector callbacks to an asynchronous consumer. Overflow becomes a terminal `DeliveryFault`; events are not silently discarded.
- **Deterministic core state machines.** Integrity and resynchronization primitives accept time values from their caller, which makes them testable with controlled time.
- **Safe Rust.** Workspace crates forbid `unsafe` code.

## Workspace crates

| Crate | Role |
|---|---|
| [`mdc-core`](crates/mdc-core) | Domain models, event envelopes, sink traits, integrity primitives, resynchronization, catalog and REST-quote contracts. It has no I/O runtime dependency. |
| [`mdc-providers`](crates/mdc-providers) | Feature-gated WebSocket and REST connectors, provider parsing, recovery, polling, catalog discovery, and quote sources. |
| [`mdc-sinks`](crates/mdc-sinks) | `L2Book`, `TopOfBookProjector`, and the bounded loss-intolerant async handoff. |
| [`mdc-testkit`](crates/mdc-testkit) | Deterministic mock WebSocket/REST servers, fixtures, fault injection, and conformance helpers. |
| [`mdc-inspect`](crates/mdc-inspect) | Terminal application for inspecting provider sessions through the consumer path. |

## Supported providers

Enable providers through the feature flags in [`crates/mdc-providers/Cargo.toml`](crates/mdc-providers/Cargo.toml). Provider modules contain the current endpoint, stream, configuration, and integrity details.

| Provider | Feature | Main normalized data | Provider-side checks or recovery |
|---|---|---|---|
| Binance Spot | `binance` | Depth, book ticker, trades | Depth sequence reconciliation with REST snapshots and a silence watchdog |
| Kraken v2 | `kraken` | Book and trades | Book checksum verification, resubscription and timeout/quarantine handling |
| Coinbase Exchange | `coinbase` | Level 2 and matches | Heartbeat/sequence monitoring and level-2 resubscription |
| Coinbase Advanced Trade | `coinbase-advanced` | Level 2 and market trades | Connection-wide sequence monitoring and resubscription; optional heartbeat channel |
| Bitfinex v2 | `bitfinex` | Book and trades | Signed order-book checksum validation and channel recovery |
| Gate.io Spot v4 | `gateio` | Order-book updates and trades | Sequence continuity, in-place resubscription and watchdog handling |
| Bitstamp v2 | `bitstamp` | Order-book updates and trades | Timestamp-gated snapshot reconciliation and reconnect handling |
| Huobi / HTX Spot | `huobi` | MBP order book and trades | Sequence continuity, in-band snapshots and bounded decompression |
| LMAX Digital | `lmax` | Full order-book updates and trades | Validated book replacement and silence monitoring |
| VWD / Infront | `vwd` | Polled indicative FX quotes | Quote validation, optional inverse transformation and scaling |
| Frankfurter | `frankfurter` | Polled reference FX rates | Complete response validation and date normalization |

Feature names are declared in the provider manifest. Use each provider module's rustdoc for its exact builder and configuration API.

## Quickstart: receive normalized events

This example disables Binance depth updates and receives only native book-ticker and trade events. It therefore avoids depth snapshots and depth sequence reconciliation. It does not disable the connector's transport watchdog.

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

## Receive complete book events or stream levels

Use `MarketDataSink`/`FnSink` when complete owned `MarketEvent` values are convenient. Use `StreamingSink` when the consumer wants per-level callbacks and control over allocations. The streaming contract is defined in [`mdc-core/src/sink/streaming.rs`](crates/mdc-core/src/sink/streaming.rs); its callback runs synchronously on the connector task and must not block.

`received_ts` is the provider's ingestion/dispatch timestamp for the event; the exact capture point depends on the provider path. `exchange_ts`, when present, comes from the venue. `IntegrityAlert` values are part of the same ordered event path, so the consumer can invalidate its own state when a connection or book becomes unhealthy.

## Use the supplied book projection, or write your own

`TopOfBookProjector` is optional. During an active resynchronization it stages L2 snapshots until the recovery alert; an authoritative full snapshot can also recover the projector directly. It applies deltas through `L2Book`, rejects crossed or empty books, and returns either `QuoteProjection::Available` or `QuoteProjection::Unavailable`.

It tracks an `effective_ts` for the older current bid/ask side. It does not run a maximum-age timer. For a live age decision, compare `effective_ts` with the application's current clock; `received_ts` is the event's ingestion timestamp, not the time at which the application later processes it. Expire state on the application's own timer when no market event arrives.

An application can instead consume raw events and implement its own book, freshness, deduplication, storage, or business rules. Provider sequence and checksum handling remain provider responsibilities for the streams that use them.

## Run asynchronous business logic

Connector callbacks are synchronous. Do not perform blocking I/O or await database/network work inside them. Use the bounded channel when the consumer needs an asynchronous task:

```rust,no_run
use futures_util::StreamExt;
use mdc_core::{Symbol, SymbolTag};
use mdc_providers::binance::{BinanceConfig, BinanceConnector};
use mdc_sinks::{loss_intolerant_channel, QuoteProjection, TopOfBookProjector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let (sender, mut stream) = loss_intolerant_channel(4096, 1, cancel.clone());

    let (connector, _handle) = BinanceConnector::builder(BinanceConfig::default())
        .symbols([Symbol::from("BTCUSDT")])
        .sink(sender)
        .build()?;
    let connector_task = tokio::spawn(connector.run(cancel.clone()));

    let mut projector = TopOfBookProjector::new_l2(
        Symbol::from("BTCUSDT"),
        SymbolTag::default(),
        None,
    );

    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                if let Some(projection) = projector.handle_event(&event) {
                    match projection {
                        QuoteProjection::Available { bid, ask, effective_ts, .. } => {
                            let age_ns = mdc_providers::now_nanos().0.saturating_sub(effective_ts.0);
                            println!("live quote bid={bid} ask={ask} age_ns={age_ns}");
                        }
                        QuoteProjection::Unavailable { reason, .. } => {
                            println!("withhold quote: {reason:?}");
                        }
                    }
                }
            }
            Err(fault) => {
                eprintln!("delivery stopped: {fault}");
                break;
            }
        }
    }

    cancel.cancel();
    let _ = connector_task.await;
    Ok(())
}
```

The channel is bounded. If it cannot accept an event, it records a terminal delivery fault and cancels the producer rather than silently dropping the accepted event prefix. Dropping the receiver cancels the producer; a subsequent send reports the receiver-closed fault. The consumer must decide whether to stop, retry its own work, or recover application state.

## Integrity alerts

`IntegrityAlert` is exhaustive. Current variants are:

- `GapDetected`;
- `ChecksumMismatch`;
- `SilenceTimeout`;
- `HeartbeatTimeout`;
- `ResyncStarted` and `ResyncCompleted`;
- `Quarantined`;
- `SourceError`;
- `ConnectionLost`, whose `reason` can include shutdown, pause, closure, or transport failure;
- `SinkFailure`.

A provider may emit only the variants relevant to its protocol. Consumers should handle alerts before treating an L2-derived quote as usable.

## Scope and limits

- The workspace provides market-data ingestion, normalization, integrity handling, and delivery. It does not place orders.
- It does not choose a database, message broker, pricing model, or business workflow.
- VWD emits validated indicative FX quotes, not an executable order book.
- Frankfurter emits reference rates, not executable quotes or trades.
- `TopOfBookProjector` is for L2 and native book-ticker events. It does not project `ReferenceRate` events.
- Depth sequence/checksum logic is part of the provider path. To avoid a depth stream entirely, select a provider's non-depth stream when that provider exposes one; there is no general switch that removes correctness checks while retaining an L2 feed.

## Development and verification

The workspace uses Rust edition 2024 and declares Rust 1.85 as its MSRV. CI currently runs workspace tests on Rust 1.85.0 and stable, a no-default-features provider check, formatting, Clippy with warnings denied, documentation with warnings denied, and benchmark compilation.

Additional local checks are documented in [`CONTRIBUTING.md`](CONTRIBUTING.md), including `cargo hack`, `cargo deny`, and `gitleaks`. Those checks are not separate GitHub Actions jobs in the current workflow.

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
```

## Documentation

- [Provider usage](docs/provider-usage.md): integration patterns, projections, delivery, and provider selection.
- [Engineering standards](docs/engineering-standards.md): architecture and verification rules.
- [Contributing](CONTRIBUTING.md): development checks and provider contribution workflow.
- [Commit conventions](docs/commit-conventions.md): repository commit format.
