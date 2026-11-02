# Use connectors in your Rust application

Start here for dependencies, startup, shutdown, and provider-specific APIs.
[Provider usage](provider-usage.md) defines the event, sink, delivery, and projection contracts.
The [provider reference](#3-provider-apis-and-symbols) links to each provider's current source.

## 1. Create an external project

Use Rust 1.85 or newer. The connector crates use edition 2024.
Keep the full connector checkout because its crates inherit workspace configuration.
For the paths below, use this directory layout:

```text
parent/
├── market-data-connectors/   # this repository
└── feed-app/
    ├── Cargo.toml
    └── src/main.rs
```

Create `feed-app/Cargo.toml`:

```toml
[package]
name = "feed-app"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"

[dependencies]
mdc-core = { path = "../market-data-connectors/crates/mdc-core" }
mdc-sinks = { path = "../market-data-connectors/crates/mdc-sinks" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
tokio-util = "0.7"

[dependencies.mdc-providers]
path = "../market-data-connectors/crates/mdc-providers"
default-features = false
features = ["binance", "rustls"]
```

The checkout must exist at that relative path on every build machine, including CI.
This repository does not establish crates.io availability. Do not assume a published version from the workspace version number.

Choose the crates and features for your application:

- `mdc-core`: event types, symbols, timestamps, sink traits, and integrity alerts.
- `mdc-providers`: connectors. No provider is enabled by default.
- `mdc-sinks`: optional books, quote projection, and async delivery. Its default `tokio` feature enables the channel used below.
- `mdc-testkit`: optional dev-dependency for mock-server and fault-injection tests.

The manifest selects Binance and the Rustls TLS backend.
For native TLS, replace `rustls` with `native-tls` or enable the provider crate's defaults.
Cargo features are additive. Adding `rustls` without disabling defaults also enables native TLS.
Check the complete feature graph if another dependency enables the same crate with different features.

## 2. Run a complete application

Save this example as `feed-app/src/main.rs`.
It receives Binance ticker and trade events through a bounded async channel.
Depth is disabled, so this example does not request depth snapshots.
The transport watchdog remains enabled.

```rust,no_run
use std::error::Error;
use std::io;
use std::time::Duration;

use mdc_core::{MarketDataKind, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector, BinanceStreams};
use mdc_sinks::loss_intolerant_channel;
use tokio_util::sync::CancellationToken;

type AppError = Box<dyn Error>;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let mut config = BinanceConfig::default();
    config.streams = BinanceStreams {
        depth: false,
        book_ticker: true,
        trade: true,
    };

    let cancel = CancellationToken::new();
    // 4096 events; 1 is a diagnostic connection label, not a routing filter.
    let (sender, mut events) = loss_intolerant_channel(4096, 1, cancel.clone());
    let (connector, _handle) = BinanceConnector::builder(config)
        .symbols([Symbol::from("BTCUSDT")])
        .sink(sender)
        .build()?;
    let mut task = tokio::spawn(connector.run(cancel.clone()));
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    let consumer_result: Result<(), AppError> = loop {
        tokio::select! {
            signal = &mut interrupt => break signal.map_err(Into::into),
            item = events.recv() => match item {
                Some(Ok(event)) => {
                    match event.kind {
                        MarketDataKind::BookTicker(ticker) => {
                            println!("{} observed bid={} ask={}",
                                event.symbol, ticker.best_bid.price, ticker.best_ask.price);
                        }
                        MarketDataKind::Trade(trade) => {
                            println!("{} trade {} @ {}", event.symbol, trade.volume, trade.price);
                        }
                        MarketDataKind::IntegrityAlert(alert) => {
                            eprintln!("{} alert: {alert:?}", event.symbol);
                        }
                        _ => {}
                    }
                    // Application-owned async processing belongs here.
                }
                Some(Err(fault)) => break Err(fault.into()),
                None => break Err(io::Error::other("market-data stream ended unexpectedly").into()),
            },
        }
    };

    // Also clean up if signal registration or event delivery failed.
    cancel.cancel();
    drop(events); // This example discards any remaining queue on local shutdown.
    let task_result: Result<(), AppError> =
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(Ok(result)) => result.map_err(Into::into),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(io::Error::new(io::ErrorKind::TimedOut, "connector shutdown timed out").into())
            }
        };
    if let Err(error) = &task_result {
        eprintln!("connector task failed: {error}");
    }
    consumer_result?;
    task_result
}
```

From `feed-app/`, run:

```sh
cargo check
cargo run
```

`cargo run` connects to a live public feed. It needs network access and provider availability.
Press Ctrl-C to cancel and await the connector. The five-second shutdown timeout is an application choice.
A delivery fault or unexpected stream closure makes this example return an error.

The printed ticker is an observation, not a promise that a quote is usable for trading.
For a maintained book and quote validation, see [projection and freshness](provider-usage.md#4-book-projection-and-application-owned-freshness).
Apply your own age limit and quiet-feed expiry timer before using prices.
For async work, size the queue for your workload. Slow processing can cause a terminal delivery fault.

## 3. Provider APIs and symbols

The event types are shared. The construction APIs and native symbol formats are not.
Choose a provider feature from [Provider usage](provider-usage.md#7-provider-selection), then use the matching API family:

| Providers | Owned events (`MarketDataSink`) | Per-level callbacks (`StreamingSink`) | Start |
|---|---|---|---|
| Binance, Kraken, Coinbase Exchange, Gate.io, Bitstamp, LMAX | `.sink(sink)` | `.streaming_sink(sink)` | `build()?` returns `(connector, handle)`; spawn or await `connector.run(cancel)` |
| Huobi, Coinbase Advanced Trade | `.owned_sink(sink)` | `.sink(sink)` or `.streaming_sink(sink)` | Same build/run shape |
| Bitfinex | `Owned::new(sink)` at start | Sink directly at start | `build()?` returns the connector; `run(sink, cancel)` returns `(handle, future)`; `spawn(sink, cancel)` returns `(handle, task)` |
| VWD, Frankfurter | `Owned::new(sink)` at start | Sink directly at start | `build()?` returns the connector; `run(sink, cancel)` immediately returns `(handle, task)` |

Call task-spawning methods inside a Tokio runtime.
Bitfinex's `run` future does nothing until polled or spawned.
VWD and Frankfurter's `run` methods spawn internally. Do not call `tokio::spawn(connector.run(...))` on their tuple results.
The examples below use owned events. Replace the construction/startup block in section 2 and retain its consumer and shutdown handling.

### Huobi and Coinbase Advanced Trade

Enable `huobi` or `coinbase-advanced` instead of `binance` in the manifest.
For example, Huobi requires `.owned_sink(sender)` for the channel sender:

```rust,no_run
use mdc_providers::huobi::{HuobiConfig, HuobiConnector};
use mdc_sinks::LossIntolerantSender;
use tokio_util::sync::CancellationToken;

async fn consume_huobi(sender: LossIntolerantSender, cancel: CancellationToken)
    -> Result<(), mdc_core::MdcError>
{
    let (connector, _handle) = HuobiConnector::builder(HuobiConfig::default())
        .symbols(["btcusdt"])
        .owned_sink(sender)
        .build()?;
    connector.run(cancel).await
}
```

For Coinbase Advanced Trade, use `CoinbaseAdvancedConfig`, `CoinbaseAdvancedConnector`, and a native product such as `BTC-USD`.
It uses the same `.owned_sink(sender)` method:

```rust,no_run
use mdc_providers::coinbase_advanced::{CoinbaseAdvancedConfig, CoinbaseAdvancedConnector};
use mdc_sinks::LossIntolerantSender;
use tokio_util::sync::CancellationToken;

async fn consume_coinbase_advanced(sender: LossIntolerantSender, cancel: CancellationToken)
    -> Result<(), mdc_core::MdcError>
{
    let (connector, _handle) = CoinbaseAdvancedConnector::builder(CoinbaseAdvancedConfig::default())
        .symbols(["BTC-USD"])
        .owned_sink(sender)
        .build()?;
    connector.run(cancel).await
}
```

### Bitfinex

Enable `bitfinex`. Its builder accepts symbols directly rather than a configuration argument:

```rust,no_run
use mdc_core::Owned;
use mdc_providers::bitfinex::BitfinexConnector;
use mdc_sinks::LossIntolerantSender;
use tokio_util::sync::CancellationToken;

async fn consume_bitfinex(sender: LossIntolerantSender, cancel: CancellationToken)
    -> Result<(), mdc_core::MdcError>
{
    let connector = BitfinexConnector::builder().symbols(["tBTCUSD"]).build()?;
    let (_handle, future) = connector.run(Owned::new(sender), cancel);
    future.await
}
```

### Frankfurter

Enable `frankfurter`. Configure currencies rather than exchange product identifiers:

```rust,no_run
use mdc_core::{MdcError, Owned};
use mdc_providers::frankfurter::{ConnectorHandle, FrankfurterConfig, FrankfurterConnector};
use mdc_sinks::LossIntolerantSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type ReferenceSession = (ConnectorHandle, JoinHandle<Result<(), MdcError>>);

fn start_reference_rates(sender: LossIntolerantSender, cancel: CancellationToken)
    -> Result<ReferenceSession, Box<dyn std::error::Error>>
{
    let config = FrankfurterConfig::builder()
        .base_currency("EUR")
        .quote_currencies(vec!["USD".into(), "GBP".into()])
        .build()?;
    let connector = FrankfurterConnector::builder(config).build()?;
    Ok(connector.run(Owned::new(sender), cancel))
}
```

In section 2, replace the Binance build and spawn statements with
`let (_handle, mut task) = start_reference_rates(sender, cancel.clone())?;`.
Keep its event loop and shutdown handling. Add a `MarketDataKind::ReferenceRate(rate)` arm to process the rates.
Frankfurter emits `ReferenceRate`, not executable bid/ask quotes. Consume these events directly rather than through `TopOfBookProjector`.

### Provider reference

Use native venue symbols. Do not assume one spelling works across exchanges.
These links lead to the current provider module, configuration, and controls:

| Provider module | Native input example | Configuration | Controls |
|---|---|---|---|
| [Binance](../crates/mdc-providers/src/binance.rs) | `BTCUSDT` | [config](../crates/mdc-providers/src/binance/config.rs) | [control](../crates/mdc-providers/src/binance/control.rs) |
| [Kraken](../crates/mdc-providers/src/kraken.rs) | `BTC/USD` | [config](../crates/mdc-providers/src/kraken/config.rs) | [control](../crates/mdc-providers/src/kraken/control.rs) |
| [Coinbase Exchange](../crates/mdc-providers/src/coinbase.rs) | `BTC-USD` | [config](../crates/mdc-providers/src/coinbase/config.rs) | [control](../crates/mdc-providers/src/coinbase/control.rs) |
| [Coinbase Advanced Trade](../crates/mdc-providers/src/coinbase_advanced.rs) | `BTC-USD` | [config](../crates/mdc-providers/src/coinbase_advanced/config.rs) | [control](../crates/mdc-providers/src/coinbase_advanced/control.rs) |
| [Bitfinex](../crates/mdc-providers/src/bitfinex.rs) | `tBTCUSD` | [config](../crates/mdc-providers/src/bitfinex/config.rs) | [control](../crates/mdc-providers/src/bitfinex/control.rs) |
| [Gate.io](../crates/mdc-providers/src/gateio.rs) | `BTC_USDT` | [config](../crates/mdc-providers/src/gateio/config.rs) | [control](../crates/mdc-providers/src/gateio/control.rs) |
| [Bitstamp](../crates/mdc-providers/src/bitstamp.rs) | `btcusd` | [config](../crates/mdc-providers/src/bitstamp/config.rs) | [control](../crates/mdc-providers/src/bitstamp/control.rs) |
| [Huobi / HTX](../crates/mdc-providers/src/huobi.rs) | `btcusdt` | [config](../crates/mdc-providers/src/huobi/config.rs) | [control](../crates/mdc-providers/src/huobi/control.rs) |
| [LMAX](../crates/mdc-providers/src/lmax.rs) | `btc-usd` | [config](../crates/mdc-providers/src/lmax/config.rs) | [control](../crates/mdc-providers/src/lmax/control.rs) |
| [VWD](../crates/mdc-providers/src/vwd.rs) | `EURUSD` | [config](../crates/mdc-providers/src/vwd/config.rs) | [control](../crates/mdc-providers/src/vwd/control.rs) |
| [Frankfurter](../crates/mdc-providers/src/frankfurter.rs) | base `EUR`, quotes `USD`, `GBP` | [config](../crates/mdc-providers/src/frankfurter/config.rs) | [control](../crates/mdc-providers/src/frankfurter/control.rs) |

The input examples illustrate syntax, not current listing availability or account entitlements.
Each provider directory also contains `connector.rs` for exact builder and startup signatures.
The connectors do not select or normalize application-owned instrument identifiers for you.

## 4. VWD credentials and startup

Enable `vwd` instead of `binance`. VWD requires a paid Infront subscription, an account path, a username, and a password.
Load credentials from application secret storage. This fragment uses application-defined environment variables:

```rust,no_run
use mdc_core::{MdcError, Owned};
use mdc_providers::vwd::{ConnectorHandle, VwdConfig, VwdConnector, VwdCredentials, VwdSymbol};
use mdc_sinks::LossIntolerantSender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type VwdSession = (ConnectorHandle, JoinHandle<Result<(), MdcError>>);

fn start_vwd(sender: LossIntolerantSender, cancel: CancellationToken)
    -> Result<VwdSession, Box<dyn std::error::Error>>
{
    let config = VwdConfig::builder()
        .account_path(std::env::var("VWD_ACCOUNT_PATH")?)
        .credentials(VwdCredentials::new(
            std::env::var("VWD_USERNAME")?,
            std::env::var("VWD_PASSWORD")?,
        ))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .build()?;
    let connector = VwdConnector::builder(config).build()?;
    Ok(connector.run(Owned::new(sender), cancel))
}
```

Replace section 2's Binance build and spawn with `let (_handle, mut task) = start_vwd(sender, cancel.clone())?;`.
VWD emits L2 snapshots by default. Add an `L2Snapshot` event arm or use the L2 projector to consume its quotes.

The builder rejects missing or empty credentials before network I/O.
`VwdCredentials` and `VwdConfig` redact their secret fields in `Debug` output.
This redaction does not protect arbitrary application logs or values explicitly extracted from secrets.
Do not log credentials, authorization headers, or URLs containing the account path.
Keep credentials out of source files and version control.
VWD emits indicative FX prices, not an executable order book.

## 5. Lifecycle, controls, and errors

Keep the cancellation token, control handle, and running task under application ownership.
To stop, cancel first, then await the task. Dropping a Tokio join handle alone detaches the task rather than stopping it.
For polling providers, the startup functions above return the internally spawned task so the application can supervise it directly.

WebSocket providers retry transport failures using provider-specific backoff and book recovery.
A retrying connection is not necessarily healthy. Consume the [integrity alerts](provider-usage.md#6-alerts-and-validity) in order.
To start a new run, construct another connector. The previous run consumes its connector.

All current handles expose `pause()` and `resume()`. Other commands differ:

| Providers | Additional commands |
|---|---|
| Binance, Kraken, Coinbase Exchange | `request_resync(symbol)`, `request_resync_all()`, `reconnect(reason)` |
| Coinbase Advanced Trade | `request_resync(symbol)`, `request_resync_all()`, `reconnect()` |
| Gate.io, Bitstamp, LMAX, Huobi | `request_resync(symbol)`, `reconnect()` |
| Bitfinex | `resync_symbol(symbol)`, `resync_all()`, `reconnect()` |
| VWD | `poll_now()`, `request_resync()` (alias for an immediate poll) |
| Frankfurter | VWD's polling commands plus `set_symbols(Vec<String>)` for quote currencies |

Commands are asynchronous. A successful send is not acknowledgement of completed recovery.
Wait for the relevant events before treating application state as valid.
A closed control channel returns an error. Do not assume that all providers share the same error type.

Observe all three error paths:

| Signal | Application response |
|---|---|
| `IntegrityAlert` event | Update or invalidate derived state according to the alert, then observe recovery. |
| `Err(DeliveryFault)` from the channel | Delivery stopped. Stop using the affected run's state and decide how to rebuild it. |
| Task completion or error | Inspect both the join result and connector result. Unexpected completion is not proof of a healthy feed. |

For connectors using the shared supervisor, `MdcError::Sink` is returned only when every connection bucket fails its sink.
One failed bucket does not necessarily stop the others.
Custom sinks need their own failure notification if the application needs immediate global shutdown.
The loss-intolerant channel instead cancels the token shared with the producer on a delivery fault.
`SinkFailure` notification uses the same failed sink and is best effort.

VWD and Frankfurter can log a warning and finish with `Ok(())` if HTTP-client initialization fails.
VWD can also emit a `SourceError` alert with `SourceErrorKind::HttpStatus(401)` and continue polling with rejected credentials.
Install your application's `tracing` subscriber if you need connector diagnostics.
A successful builder or a still-running task does not establish source health.

## 6. Consumer policy and verification

Keep storage, deduplication, pricing, freshness, and business rules in your application.
For the callback and backpressure contracts, use [Provider usage](provider-usage.md#5-asynchronous-application-logic).
For book validation and age limits, use [projection and freshness](provider-usage.md#4-book-projection-and-application-owned-freshness).
Use one projector per venue/symbol stream, and invalidate application state after delivery stops.

To verify an external consumer without connecting to a venue:

```sh
cargo check
cargo test
```

For repeatable builds, keep the application's `Cargo.lock` and use `--locked`.
Add `--offline` when all required dependencies are cached locally.
Run your own mock-based tests for recovery, consumer failure, overflow, and shutdown.
Compilation does not prove provider availability or credential validity.

From the connector checkout, render rustdoc and separately compile/test its examples:

```sh
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
cargo test --workspace --doc --all-features --locked
```

`cargo doc` renders documentation. It does not compile or execute its example blocks.
`cargo test --doc` checks rustdoc examples. These commands do not automatically check Markdown fences in `README.md` or `docs/`.
Copy the complete manifest and main into an external crate to check those examples with your selected features.
The family snippets are function definitions, not standalone executables. Compile them as modules in a consumer with the matching provider features.
