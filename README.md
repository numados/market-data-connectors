# Market Data Connectors

A modular, zero-unsafe Rust workspace providing deterministic, low-latency market data connectors, order book integrity state machines, and downstream delivery sinks for cryptocurrency and FX venues.

---

## Highlights

- **First-Class Book Integrity**: Built-in sequence continuity tracking ($U/u$, `prevSeqNum`, connection-wide sequences), CRC32 order book checksum verification (Kraken, Bitfinex), microsecond timestamp gating (Bitstamp), heartbeat monitors, and silence watchdogs.
- **In-Band Lifecycle & Fault Ordering**: Connection loss, resynchronization starts/completions, checksum mismatches, and sink failures are emitted in-band as [`IntegrityAlert`](crates/mdc-core/src/model/alert.rs) events ordered strictly with the market data they invalidate.
- **Hot-Path Zero Allocations**: Borrowing `serde` wire parsers (`RawValue`, `DeserializeSeed` into reused scratch buffers) and inline 16-byte [`Symbol`](crates/mdc-core/src/model/symbol.rs) identifiers achieve zero heap allocations per streaming frame after warm-up.
- **Bounded Resources & Structured Concurrency**: Every connection worker, snapshot task, and forwarder is owned by a `JoinSet` and tied to a `CancellationToken`. HTTP snapshot/catalog responses and gzip inflation streams enforce strict byte caps.
- **100% Safe Rust**: `#![forbid(unsafe_code)]` enforced across all workspace crates.

---

## Workspace Crates

| Crate | Description |
|---|---|
| [`mdc-core`](crates/mdc-core) | Pure domain models (`MarketEvent`, `L2Snapshot`, `L2Delta`, `BookTicker`, `Trade`, `ReferenceRate`), `StreamingSink` / `MarketDataSink` traits, sequence/checksum/embargo/watchdog integrity primitives, and self-healing `ResyncController`. Zero async or I/O dependencies. |
| [`mdc-providers`](crates/mdc-providers) | Feature-gated WebSocket and REST market data connectors, instrument catalog discovery (`InstrumentCatalog`), and REST quote snapshot clients (`RestQuoteSource`). |
| [`mdc-sinks`](crates/mdc-sinks) | Downstream adapters including [`L2Book`](crates/mdc-sinks/src/book.rs), [`TopOfBookProjector`](crates/mdc-sinks/src/top_of_book.rs), and bounded [`loss_intolerant_channel`](crates/mdc-sinks/src/loss_intolerant.rs) with a reserved terminal fault slot. |
| [`mdc-testkit`](crates/mdc-testkit) | Deterministic in-process mock WebSocket (`MockWsServer`) and REST (`MockRest`, `MockCatalogServer`, `MockRestQuoteServer`) harnesses, fault injectors, and end-to-end conformance runners. |

---

## Supported Venues & Feature Flags

Enable individual venues on `mdc-providers` (or `all` for every venue). TLS defaults to `native-tls` and supports `rustls`.

| Venue | Feature Flag | Transport | Book / Quote Streams | Trades | Integrity Mechanism | Discovery & REST Quote |
|---|---|---|---|---|---|---|
| **Binance Spot** | `binance` | WebSocket + REST | L2 diff (`@depth@100ms`) + REST snapshot, `@bookTicker` | `@trade` | $U/u$ sequence range + snapshot reconciliation | Yes |
| **Kraken v2** | `kraken` | WebSocket v2 + REST | L2 snapshot + delta (`book`) | `trade` | Top-10 CRC32 checksum + timeout quarantine | Yes |
| **Coinbase Exchange** | `coinbase` | WebSocket + REST | L2 (`level2_batch`) + `heartbeat` | `matches` | Heartbeat sequence + silence monitor | Yes |
| **Coinbase Advanced** | `coinbase-advanced` | WebSocket | L2 (`level2` / `l2_data`) + `heartbeats` | `market_trades` | Connection-wide sequence tracker | — |
| **Bitfinex v2** | `bitfinex` | WebSocket v2 + REST | L2 (`book`, `P0`/`F0`) | `trades` (`te`/`tu`) | Top-25 signed CRC32 interleaved checksum | Yes |
| **Gate.io Spot v4** | `gateio` | WebSocket v4 + REST | L2 (`spot.obu`) | `spot.trades` | $U/u$ sequence continuity + in-place resubscribe | Yes |
| **Bitstamp v2** | `bitstamp` | WebSocket v2 + REST | L2 (`diff_order_book_*`) + REST snapshot | `live_trades_*` | Microsecond timestamp gating ($> T$) + buffered replay | Yes |
| **Huobi (HTX) Spot** | `huobi` | WebSocket (gzip) + REST | L2 (`market.*.mbp.*`) + in-band `req` snapshot | `market.*.trade.detail` | `prevSeqNum == seqNum` continuity + bounded gzip cap | Yes |
| **LMAX Digital** | `lmax` | WebSocket | L2 full top-20 snapshots (`ORDER_BOOK`) | `TRADE` | Validated full-book replacement + silence watchdog | — |
| **VWD (Infront)** | `vwd` | REST Polling | Top-of-book (`snapquote`) | — | Per-side freshness gating + inverse transformation validation | — |
| **Frankfurter (ECB)** | `frankfurter` | REST Polling | Reference FX rates (`/v1/latest`) | — | Complete batch validation + civil-date UTC normalization | — |

---

## Quickstart

### 1. Direct Stream (Without Downstream Stale/Book State Projection)

Pass any [`FnSink`](crates/mdc-core/src/sink.rs) or [`StreamingSink`](crates/mdc-core/src/sink.rs) directly to a connector when you want normalized `MarketEvent` frames (`Trade`, `BookTicker`, `L2Snapshot`, `L2Delta`, `ReferenceRate`) and in-band `IntegrityAlert` notifications without maintaining a stateful order book or staleness state machine in [`mdc-sinks`](crates/mdc-sinks):

```rust,no_run
use mdc_core::{FnSink, IntegrityAlert, MarketDataKind, MarketEvent, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink = FnSink::new(
        |event: MarketEvent<MarketDataKind>| {
            println!("{:?} {} {:?}", event.venue, event.symbol, event.data);
            Ok::<(), std::convert::Infallible>(())
        },
        |alert: IntegrityAlert| {
            eprintln!("alert: {:?}", alert);
            Ok::<(), std::convert::Infallible>(())
        },
    );

    let cancel = CancellationToken::new();
    let connector = BinanceConnector::builder(BinanceConfig::default())
        .symbols([Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")])
        .sink(sink)
        .build()?;

    connector.run(cancel).await?;
    Ok(())
}
```

### 2. Stateful Projection (With Stale & Book Integrity Algorithms)

Wrap your downstream sink in [`TopOfBookProjector`](crates/mdc-sinks/src/top_of_book.rs) (and optionally [`loss_intolerant_channel`](crates/mdc-sinks/src/loss_intolerant.rs)) so that out-of-order sequences, CRC32 mismatches, silence timeouts, or reconnects automatically transition affected instruments into `InstrumentHealth::Stale` and quarantine deltas until a fresh snapshot restores `InstrumentHealth::Healthy`:

```rust,no_run
use mdc_core::{FnSink, IntegrityAlert, MarketEvent, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector};
use mdc_sinks::{TopOfBookEvent, TopOfBookProjector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inner = FnSink::new(
        |event: MarketEvent<TopOfBookEvent>| {
            match &event.data {
                TopOfBookEvent::BookTicker(bbo) => {
                    println!("HEALTHY {} bid={} ask={}", event.symbol, bbo.bid_price, bbo.ask_price);
                }
                TopOfBookEvent::HealthChanged { symbol, health, reason } => {
                    println!("STATE {} -> {:?} ({:?})", symbol, health, reason);
                }
            }
            Ok::<(), std::convert::Infallible>(())
        },
        |_alert: IntegrityAlert| Ok::<(), std::convert::Infallible>(),
    );

    let projector = TopOfBookProjector::new(inner);
    let cancel = CancellationToken::new();

    let connector = BinanceConnector::builder(BinanceConfig::default())
        .symbols([Symbol::from("BTCUSDT")])
        .sink(projector)
        .build()?;

    connector.run(cancel).await?;
    Ok(())
}
```

---

## Using Connectors With vs. Without Stale Algorithms

Market data integrity operates at two composable layers:

1. **Downstream Sink Layer (Common Across All Providers)**:
   - **With Stale Projection**: Use [`TopOfBookProjector`](crates/mdc-sinks/src/top_of_book.rs), [`L2Book`](crates/mdc-sinks/src/book.rs), and [`loss_intolerant_channel`](crates/mdc-sinks/src/loss_intolerant.rs) to maintain a state machine per symbol (`Healthy` vs. `Stale`), suppress quotes from uninitialized/crossed/faulted books, and trigger `HealthChangeReason` transitions on `IntegrityAlert` events.
   - **Without Stale Projection**: Implement [`StreamingSink`](crates/mdc-core/src/sink.rs) or [`FnSink`](crates/mdc-core/src/sink.rs) directly to consume raw events and handle (or ignore) `on_alert` notifications yourself.
2. **Provider Transport Layer (Per-Venue Configuration)**:
   - Each provider exposes channel selection (`enable_depth`, `subscribe_book`, `subscribe_trades`) and watchdog/staleness knobs (`read_timeout`, `heartbeat_timeout`, `stale_threshold`, `silence_timeout_ms`, `max_quote_age`) on its config struct.

| Provider | How to Use **With** Integrity / Stale Algorithms | How to Use **Without** Stale / Book Algorithms |
|---|---|---|
| **Binance** (`binance`) | Enable `enable_depth = true` (default) for `@depth@100ms` + REST `/api/v3/depth` snapshot reconciliation ($U \le \text{lastUpdateId}+1 \le u$) and `read_timeout` silence detection. | Set `enable_depth = false` and `enable_book_ticker = true` / `enable_trades = true` for stateless `@bookTicker` / `@trade` streams without REST depth snapshots. |
| **Kraken** (`kraken`) | Subscribe to `book` (`depth`: `10`/`25`/`100`/`500`/`1000`) for top-10 CRC32 verification on every delta and `read_timeout` watchdog quarantine. | Pass a direct `FnSink` or consume `trade` events; raise `read_timeout` on illiquid pairs. |
| **Coinbase Exchange** (`coinbase`) | Set `subscribe_level2 = true` and `subscribe_heartbeat = true` (defaults) for per-product sequence gap detection and `heartbeat_timeout` silence alerts. | Set `subscribe_heartbeat = false` and `subscribe_level2 = false` (`subscribe_matches = true`) to stream trades without heartbeat or L2 sequence tracking. |
| **Coinbase Advanced** (`coinbase-advanced`) | Keep `subscribe_heartbeats = true` (default) for connection-wide `sequence_num` continuity and `heartbeat_timeout` silence detection. | Set `subscribe_heartbeats = false` and `subscribe_level2 = false` (`subscribe_trades = true`) to stream `market_trades` without heartbeat enforcement. |
| **Bitfinex** (`bitfinex`) | Keep `subscribe_book = true` (default) so the connector sends `conf` (`flags: 131072`) and verifies the signed 32-bit CRC32 checksum on every book frame (`cs`). | Set `subscribe_book = false` and `subscribe_trades = true` on `BitfinexConnector::builder()` to stream `trades` without book state or CRC32 checks. |
| **Gate.io** (`gateio`) | Keep `subscribe_depth = true` (default) for `spot.obu` $U/u$ sequence validation, automatic in-place channel resubscription on gaps, and `read_timeout` monitoring. | Set `subscribe_depth = false` and `subscribe_trades = true` to stream `spot.trades` without order book sequence state. |
| **Bitstamp** (`bitstamp`) | Keep `subscribe_book = true` (default) for REST `/api/v2/order_book/` snapshots + strictly increasing microsecond delta gating ($> T$). | Set `subscribe_book = false` and `subscribe_trades = true` to stream `live_trades_*` without HTTP snapshot reconciliation. |
| **Huobi / HTX** (`huobi`) | Keep `subscribe_depth = true` (default) for in-band `req` snapshots, `prevSeqNum == seqNum` continuity checks, and `stale_threshold` (default `5s`) silence alerts. | Set `subscribe_depth = false`, `subscribe_trades = true`, and raise `stale_threshold` to stream trades without MBP book state. |
| **LMAX Digital** (`lmax`) | Keep `subscribe_book = true` and `silence_timeout_ms` (default `5000 ms`) for validated top-20 book replacement and silence watchdog alerts. | Set `subscribe_book = false` (`subscribe_trades = true`) or raise `silence_timeout_ms` when downstream handles staleness externally. |
| **VWD / Infront** (`vwd`) | Configure `max_quote_age = Some(Duration::from_secs(300))` on `VwdPairMapping` so per-side timestamps (`zeit_bid`, `zeit_ask`) older than the threshold emit `IntegrityAlert::Custom` and drop stale quotes. | Set `max_quote_age = None` on `VwdPairMapping` to accept the latest polled quotes regardless of venue timestamp age. |
| **Frankfurter** (`frankfurter`) | Pair with [`TopOfBookProjector`](crates/mdc-sinks/src/top_of_book.rs) or inspect `on_alert` to react to HTTP 429/5xx backoff, schema failures, or missing expected target currencies. | Pass a direct `FnSink` and ignore `on_alert` if your consumer only needs the latest successfully polled ECB reference rates. |

See [**docs/provider-usage.md**](docs/provider-usage.md) for complete, copy-pasteable code examples for every provider and pattern.

---

## Rust Version Policy

The minimum supported Rust version (MSRV) is **1.85.0**. Crates use Rust edition
2024; consuming applications may use an earlier edition with a compatible
compiler. The workspace uses Cargo resolver 3.

MSRV is declared in `Cargo.toml` and inherited by each workspace member. CI tests
the workspace on both MSRV and stable Rust. The supported minimum covers the
library and its tests; dependency updates must preserve that compatibility.

MSRV does not increase automatically when Rust releases a new stable version.
Raise it only when required by a language feature, a fix, or a maintained
dependency. Announce increases in release notes and make them in a minor release,
not a patch release. The new minimum must have been available for at least six
months, except for an explicitly documented urgent security fix.

---

## Verification & Local Checks

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo +1.85.0 test --workspace --all-features --locked
cargo hack check -p mdc-providers --each-feature --no-dev-deps
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
cargo bench --workspace --no-run --locked
cargo deny check
gitleaks detect --no-git --source .
```

Install the MSRV toolchain (`rustup toolchain install 1.85.0`) before running its test command. Additional architecture invariants and development protocols are documented in [docs/engineering-standards.md](docs/engineering-standards.md), [docs/commit-conventions.md](docs/commit-conventions.md), and [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
