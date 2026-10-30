# Provider usage

This guide describes the public integration boundary in the current workspace and the consumer contract around it.

## 1. Two integrity layers

The connector and the application have different responsibilities.

| Layer | Owned by | What it does |
|---|---|---|
| Provider integrity | `mdc-providers` and `mdc-core` | Parses a venue feed, applies the venue's sequence/timestamp/checksum/heartbeat rules, emits `IntegrityAlert` values, and performs the provider's recovery flow. |
| Application projection | Your application or optional `mdc-sinks` | Builds a book, rejects crossed or incomplete quotes, applies maximum-age rules, stores data, and runs business logic. |

`TopOfBookProjector` is optional. You can consume provider events directly and implement a different book or freshness algorithm. Skipping the projector does not remove provider-side sequence, checksum, heartbeat, or transport handling.

## 2. Direct event consumption

`MarketDataSink` receives owned `MarketEvent<MarketDataKind>` values. `FnSink` is a closure adapter. Connector builders clone the sink for connection tasks, so wrap a `FnSink` in `Arc` when using a builder that accepts `.sink(...)`.

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

The callback runs synchronously on the connector task. It must not block. A sink error is fatal to the producing connection and is surfaced through the connector's error path.

## 3. Low-allocation streaming sink

`StreamingSink` is the lower-allocation contract. It receives `begin_book`, one `level` callback per price level, and `end_book` for a book update. Trade, ticker, reference-rate, and alert callbacks receive borrowed payloads. The context contains the venue, symbol, timestamps, source sequence, connection ID, and delivery stamp.

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

The connector callback is synchronous, while database writes, HTTP calls, and other application operations are often asynchronous. `loss_intolerant_channel` provides a bounded FIFO bridge:

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
                        QuoteProjection::Available { bid, ask, .. } => {
                            // Call application-owned async business logic here.
                            println!("usable quote bid={bid} ask={ask}");
                        }
                        QuoteProjection::Unavailable { reason, .. } => {
                            println!("quote unavailable: {reason:?}");
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

When the queue is full, the channel records a terminal `DeliveryFault`, cancels the producer token, and does not silently drop an accepted event prefix. Dropping the receiver cancels the producer immediately; a subsequent send reports the receiver-closed delivery fault. The application owns the response to that terminal condition.

## 6. Alerts and validity

The public `IntegrityAlert` variants are:

- `GapDetected`;
- `ChecksumMismatch`;
- `SilenceTimeout`;
- `HeartbeatTimeout`;
- `ResyncStarted`;
- `ResyncCompleted`;
- `Quarantined`;
- `SourceError`;
- `ConnectionLost`;
- `SinkFailure`.

`ConnectionLost` carries a `DisconnectKind`, such as `Closed`, `TransportError`, `Paused`, or `Shutdown`. A connector emits only the alerts relevant to its provider and current state. A consumer should not assume that every provider emits every variant. Trade, ticker, and reference-rate payloads are borrowed in `StreamingSink`; alerts are passed by value.

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

The public examples are checked by the workspace documentation build when they are Rust documentation examples. Markdown fences are not automatically compiled by Cargo. Run the repository checks before publishing an integration:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
```
