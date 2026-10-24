# Connector & Stale Algorithm Usage Guide

This guide explains how to configure and run market data connectors **with** and **without** staleness/integrity algorithms, both at the common sink/projection layer (`mdc-core`, `mdc-sinks`) and per supported venue (`mdc-providers`).

---

## Table of Contents

1. [Architecture Overview: Two Layers of Staleness & Integrity](#1-architecture-overview-two-layers-of-staleness--integrity)
2. [Common Usage: Without Stale Projection (Raw Event Stream)](#2-common-usage-without-stale-projection-raw-event-stream)
3. [Common Usage: With Stale & Integrity Projection (`TopOfBookProjector` + `L2Book`)](#3-common-usage-with-stale--integrity-projection-topofbookprojector--l2book)
4. [Common Usage: Bounded Loss-Intolerant Delivery (`loss_intolerant_channel`)](#4-common-usage-bounded-loss-intolerant-delivery-loss_intolerant_channel)
5. [Provider-Specific Configuration & Examples](#5-provider-specific-configuration--examples)
   - [5.1 Binance Spot (`binance`)](#51-binance-spot-binance)
   - [5.2 Kraken WebSocket v2 (`kraken`)](#52-kraken-websocket-v2-kraken)
   - [5.3 Coinbase Exchange (`coinbase`)](#53-coinbase-exchange-coinbase)
   - [5.4 Coinbase Advanced Trade (`coinbase-advanced`)](#54-coinbase-advanced-trade-coinbase-advanced)
   - [5.5 Bitfinex WebSocket v2 (`bitfinex`)](#55-bitfinex-websocket-v2-bitfinex)
   - [5.6 Gate.io Spot v4 (`gateio`)](#56-gateio-spot-v4-gateio)
   - [5.7 Bitstamp WebSocket v2 (`bitstamp`)](#57-bitstamp-websocket-v2-bitstamp)
   - [5.8 Huobi / HTX Spot (`huobi`)](#58-huobi--htx-spot-huobi)
   - [5.9 LMAX Digital (`lmax`)](#59-lmax-digital-lmax)
   - [5.10 VWD / Infront FX REST (`vwd`)](#510-vwd--infront-fx-rest-vwd)
   - [5.11 Frankfurter ECB Reference Rates (`frankfurter`)](#511-frankfurter-ecb-reference-rates-frankfurter)
6. [Runtime Operator Control (`ConnectorHandle`)](#6-runtime-operator-control-connectorhandle)

---

## 1. Architecture Overview: Two Layers of Staleness & Integrity

Market data staleness and integrity are separated into two composable layers so you can choose the exact level of protection your application needs:

| Layer | Component | Responsibility | Can Be Bypassed / Relaxed? |
|---|---|---|---|
| **Layer 1: Connector Wire Integrity** | `mdc-providers` + `mdc-core::integrity` | Detects sequence gaps ($U/u$, `prevSeqNum`), CRC32 checksum mismatches, stale diffs ($t \le T$), heartbeat loss, and socket silence. Emits in-band `IntegrityAlert` events and triggers self-healing snapshot resynchronization. | **Yes** — disable L2 depth (use `@bookTicker` or trades only), set `ChecksumPolicy::Optional` / `NoChecksum`, set `SequencePolicy::Optional`, or widen watchdog/heartbeat timeouts. |
| **Layer 2: Downstream Stale & Book Projection** | `mdc-sinks::TopOfBookProjector` & `L2Book` | Consumes `MarketEvent` streams, immediately withdraws quotes (`QuoteProjection::Unavailable`) on any invalidating `IntegrityAlert`, stages recovery books until `ResyncCompleted`, and tracks older-side `effective_ts` for quote age checks. | **Yes** — consume events directly via `FnSink`, `VecSink`, or custom `StreamingSink` / `MarketDataSink` without `TopOfBookProjector`. |

---

## 2. Common Usage: Without Stale Projection (Raw Event Stream)

Use this mode when you want direct access to every normalized event (`L2Snapshot`, `L2Delta`, `BookTicker`, `Trade`, `ReferenceRate`, `IntegrityAlert`) — for example in market data recorders, trade analytics, or engines that maintain their own book state.

### 2.1 Owned Event Sink (`MarketDataSink` / `FnSink`)

```rust,no_run
use mdc_core::{FnSink, MarketDataKind, MarketEvent, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector, BinanceStreams};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ticker + trades only: no REST depth snapshots or L2 sequence reconciliation
    let mut config = BinanceConfig::default();
    config.streams = BinanceStreams {
        depth: false,
        book_ticker: true,
        trade: true,
    };

    let sink = FnSink::new(|event: MarketEvent<MarketDataKind>| {
        match &event.data {
            MarketDataKind::BookTicker(t) => {
                println!("{} bid={} ask={}", event.symbol, t.best_bid.price, t.best_ask.price);
            }
            MarketDataKind::Trade(tr) => {
                println!("{} trade {} @ {}", event.symbol, tr.volume, tr.price);
            }
            MarketDataKind::IntegrityAlert(alert) => {
                println!("{} alert: {:?}", event.symbol, alert);
            }
            _ => {}
        }
        Ok::<(), std::convert::Infallible>(())
    });

    let cancel = CancellationToken::new();
    let (connector, _handle) = BinanceConnector::builder(config)
        .symbols(["BTCUSDT", "ETHUSDT"].map(Symbol::from))
        .sink(sink)
        .build()?;

    connector.run(cancel).await?;
    Ok(())
}
```

### 2.2 Zero-Allocation Hot-Path Sink (`StreamingSink`)

Implement [`StreamingSink`](../crates/mdc-core/src/sink/streaming.rs) and pass it via `.streaming_sink(my_sink)` to receive price levels one by one (`begin_book` → `level` → `end_book`) with **zero heap allocations per frame** after scratch buffer warm-up:

```rust
use mdc_core::{
    BookKind, BookTicker, EventContext, IntegrityAlert, RawLevelText, ReferenceRate, SequenceRange,
    Side, StreamingSink, Trade,
};
use rust_decimal::Decimal;

#[derive(Clone, Default)]
struct HotPathSink;

impl StreamingSink for HotPathSink {
    type Error = std::convert::Infallible;

    fn begin_book(&mut self, _ctx: &EventContext<'_>, _kind: BookKind) -> Result<(), Self::Error> {
        Ok(())
    }

    fn level(
        &mut self,
        _ctx: &EventContext<'_>,
        _side: Side,
        _price: Decimal,
        _volume: Decimal,
        _raw: Option<RawLevelText<'_>>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn end_book(
        &mut self,
        _ctx: &EventContext<'_>,
        _kind: BookKind,
        _sequence: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn trade(&mut self, _ctx: &EventContext<'_>, _trade: Trade) -> Result<(), Self::Error> {
        Ok(())
    }

    fn book_ticker(
        &mut self,
        _ctx: &EventContext<'_>,
        _ticker: BookTicker,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn reference_rate(
        &mut self,
        _ctx: &EventContext<'_>,
        _rate: ReferenceRate,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn alert(&mut self, _ctx: &EventContext<'_>, _alert: IntegrityAlert) -> Result<(), Self::Error> {
        Ok(())
    }
}
```

---

## 3. Common Usage: With Stale & Integrity Projection (`TopOfBookProjector` + `L2Book`)

Use [`TopOfBookProjector`](../crates/mdc-sinks/src/top_of_book.rs) when downstream pricing or execution logic requires **guaranteed non-stale, uncrossed quotes**:

1. **Immediate Stale Withdrawal**: Any invalidating `IntegrityAlert` (`SequenceGap`, `SequenceReset`, `ChecksumMismatch`, `SilenceTimeout`, `Disconnected`, `ResyncStarted`, `Quarantined`, `SourceError`, `SinkFailure`, `Shutdown`) immediately transitions the projector to `QuoteProjection::Unavailable { reason: UnavailableReason::IntegrityAlert(...) }`.
2. **Candidate Book Staging**: During resynchronization, snapshots and replayed deltas are staged in a candidate book and only promoted to active when `IntegrityAlert::ResyncCompleted` arrives (or a valid full L2 snapshot replaces an out-of-sync book).
3. **Crossed-Book & Empty-Side Guards**: Automatically withdraws quotes with `UnavailableReason::CrossedBook` (`best_bid >= best_ask`) or `UnavailableReason::EmptySide`.
4. **Effective Freshness Timestamp (`effective_ts`)**: Across single-sided book updates, `effective_ts` retains the **older** of the best bid and best ask timestamps, allowing you to enforce an age threshold (`received_ts.0.saturating_sub(effective_ts.0) > max_age_nanos`).

```rust
use mdc_core::{MarketDataKind, MarketEvent, Symbol, SymbolTag};
use mdc_sinks::{QuoteProjection, TopOfBookProjector};

fn process_with_stale_protection(events: &[MarketEvent<MarketDataKind>]) {
    let symbol = Symbol::from("BTCUSDT");
    // L2 projection mode with a 1000-level depth cap (use TopOfBookProjector::new_ticker for @bookTicker)
    let mut projector = TopOfBookProjector::new_l2(symbol, SymbolTag::default(), Some(1000));

    let max_quote_age_ns: u64 = 5_000_000_000; // 5 seconds

    for event in events {
        if let Some(projection) = projector.handle_event(event) {
            match projection {
                QuoteProjection::Available {
                    symbol,
                    bid,
                    ask,
                    effective_ts,
                    received_ts,
                    ..
                } => {
                    let age_ns = received_ts.0.saturating_sub(effective_ts.0);
                    if age_ns > max_quote_age_ns {
                        println!("{symbol} STALE by age ({age_ns} ns)");
                    } else {
                        println!("{symbol} LIVE bid={bid} ask={ask}");
                    }
                }
                QuoteProjection::Unavailable { symbol, reason, .. } => {
                    // Quote withdrawn due to gap, checksum error, disconnect, crossed book, etc.
                    println!("{symbol} UNAVAILABLE: {reason:?}");
                }
            }
        }
    }
}
```

---

## 4. Common Usage: Bounded Loss-Intolerant Delivery (`loss_intolerant_channel`)

To bridge a synchronous connector sink to an async consumer task **without silent packet drops**, pair [`loss_intolerant_channel`](../crates/mdc-sinks/src/loss_intolerant.rs) with [`TopOfBookProjector`](../crates/mdc-sinks/src/top_of_book.rs). If the bounded queue fills up, the channel automatically cancels the connector's `CancellationToken` and yields a terminal `Err(DeliveryFault)` after draining queued events:

```rust,no_run
use futures_util::StreamExt;
use mdc_core::{Symbol, SymbolTag};
use mdc_providers::binance::{BinanceConfig, BinanceConnector};
use mdc_sinks::{TopOfBookProjector, loss_intolerant_channel};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let (sender, mut stream) = loss_intolerant_channel(4096, 1, cancel.clone());

    let (connector, _handle) = BinanceConnector::builder(BinanceConfig::default())
        .symbols([Symbol::from("BTCUSDT")])
        .sink(sender)
        .build()?;

    let conn_task = tokio::spawn(connector.run(cancel.clone()));

    let mut projector = TopOfBookProjector::new_l2(Symbol::from("BTCUSDT"), SymbolTag::default(), Some(1000));
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                if let Some(quote) = projector.handle_event(&event) {
                    println!("{quote:?}");
                }
            }
            Err(fault) => {
                eprintln!("Terminal delivery fault (consumer lagged or closed): {fault}");
                break;
            }
        }
    }

    cancel.cancel();
    let _ = conn_task.await;
    Ok(())
}
```

---

## 5. Provider-Specific Configuration & Examples

### Summary Matrix: Integrity & Stale Controls by Provider

| Provider | Default Integrity / Stale Mechanism | How to Tune / Customize Integrity | How to Run Without Book Stale Algo |
|---|---|---|---|
| **Binance** | `@depth@100ms` $U/u$ sequence tracker + REST snapshot reconciliation (`ResyncController`) + `SilenceWatchdog` + HTTP 429/418 `EmbargoTracker` | `.resync_policy(...)`, `resync_buffer_capacity`, `snapshot_retry_cooldown`, `watchdog_ping_after`, `watchdog_reconnect_after` | Set `streams.depth = false`, `streams.book_ticker = true` (uses `@bookTicker` with zero REST snapshot calls) |
| **Kraken** | Top-10 CRC32 book checksum (`KrakenChecksum`) + `TimeoutQuarantinePolicy` + subscription rejection escalation + `SilenceWatchdog` | `.checksum_strategy(...)`, `checksum_policy`, `resubscribe_timeout`, `quarantine_duration`, `rejection_policy` | Set `checksum_policy = ChecksumPolicy::Optional` or `.checksum_strategy(Arc::new(NoChecksum))`, or `streams.depth = false` |
| **Coinbase Exchange** | `heartbeat` channel sequence jump detector (`sequence_gap_threshold = Some(3000)`) + `HeartbeatMonitor` (`5s`) + `SilenceWatchdog` | `heartbeat_timeout`, `sequence_gap_threshold`, `resubscribe_timeout`, `quarantine_duration` | Set `sequence_gap_threshold = None` to disable heartbeat jump resyncs, or `streams.depth = false, streams.matches = true` |
| **Coinbase Advanced** | Connection-wide `GlobalSequenceTracker` + sequence reset detector (`reset_threshold = 2`) + `SilenceWatchdog` | `sequence_policy`, `reset_threshold`, `resync_timeout`, `silence_timeout` | Set `sequence_policy = SequencePolicy::Optional` to accept frames without sequence numbers, or `.level2(false).market_trades(true)` |
| **Bitfinex** | Top-25 signed CRC32 interleaved checksum (`BitfinexChecksum`) + per-channel quarantine FSM + `te`/`tu` trade deduplication | `watchdog_ping_after`, `watchdog_reconnect_after`, `resubscribe_timeout`, `quarantine_duration` | Set `.enable_depth(false).enable_trades(true)` for trade-only streaming |
| **Gate.io** | `spot.obu` $U/u$ sequence tracker + `ResyncController` + in-place resubscribe cooldown + `spot.ping`/`spot.pong` watchdog | `.resync_policy(...)`, `resync_buffer_capacity`, `resync_timeout`, `watchdog_ping_after`, `watchdog_reconnect_after` | Set `streams.depth = false, streams.trades = true` |
| **Bitstamp** | Microsecond timestamp gate ($t > T_{\text{snapshot}}$) + buffered diff replay + REST snapshot embargo + `bts:request_reconnect` | `.resync_policy(...)`, `.buffer_capacity(...)`, `snapshot_retry_cooldown`, `request_timeout` | Set `streams.depth = false, streams.trades = true` |
| **Huobi (HTX)** | Gzip `mbp.{levels}` `prevSeqNum == seqNum` continuity + in-band WebSocket `req` snapshot reconciliation + decompressed frame cap | `.resync_policy(...)`, `resync_buffer_capacity`, `resync_timeout`, `snapshot_cooldown`, `silence_timeout` | Set `streams.mbp = false, streams.trades = true` |
| **LMAX Digital** | Validated top-20 full `ORDER_BOOK` replacement + `ControlPacing` rate-limit backoff + `SilenceWatchdog` | `.control_pacing(...)`, `silence_timeout`, `ping_interval` | Full snapshots on every tick (no delta drift); set `streams.book = false, streams.trades = true` for trades only |
| **VWD (Infront)** | Per-side bid/ask timestamp extraction (ignores `last.dateTime`), crossed/zero-price rejection, inverse quote revalidation, `SourceError` → `ResyncCompleted` alerts | `poll_interval`, `request_timeout`, `VwdSymbol::direct` vs `VwdSymbol::with_inverse` vs `VwdSymbol::with_scale` | Set `emit_book_ticker = true` to receive direct `BookTicker` events on each poll |
| **Frankfurter (ECB)** | Full batch completeness check (all configured quotes must be present and positive, identity pairs skipped) + civil-date UTC normalization | `poll_interval`, `request_timeout`, `body_cap_bytes` | Emits `ReferenceRate` events directly per poll |

---

### 5.1 Binance Spot (`binance`)

**With full L2 integrity & custom resync policy:**
```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Symbol, resync::CooldownPolicy};
use mdc_providers::binance::{BinanceConfig, BinanceConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = BinanceConfig::default();
    config.watchdog_ping_after = Duration::from_secs(5);
    config.watchdog_reconnect_after = Duration::from_secs(5);
    config.resync_buffer_capacity = 5_000;

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let cancel = CancellationToken::new();

    let (connector, _handle) = BinanceConnector::builder(config)
        .symbols([Symbol::from("BTCUSDT")])
        .resync_policy(CooldownPolicy::new(Duration::from_secs(2)))
        .sink(sink)
        .build()?;

    connector.run(cancel).await?;
    Ok(())
}
```

**Without L2 snapshot/resync overhead (`@bookTicker` only):**
```rust,no_run
use mdc_core::{FnSink, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnector, BinanceStreams};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = BinanceConfig {
        streams: BinanceStreams {
            depth: false,       // Disables @depth@100ms and REST snapshot reconciliation
            book_ticker: true,  // Streams low-latency @bookTicker directly
            trade: false,
        },
        ..BinanceConfig::default()
    };

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = BinanceConnector::builder(config)
        .symbols([Symbol::from("BTCUSDT")])
        .sink(sink)
        .build()?;

    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.2 Kraken WebSocket v2 (`kraken`)

**With strict CRC32 top-10 checksum & quarantine (default) vs. without checksum enforcement:**
```rust,no_run
use std::sync::Arc;
use mdc_core::{ChecksumPolicy, FnSink, NoChecksum, Symbol};
use mdc_providers::kraken::{KrakenConfig, KrakenConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Default strict mode: ChecksumPolicy::Required + KrakenChecksum (CRC32 over top 10 levels)
    let strict_config = KrakenConfig::default();

    // 2. Without checksum verification: allow missing checksums and bypass CRC32 calculation
    let mut relaxed_config = KrakenConfig::default();
    relaxed_config.checksum_policy = ChecksumPolicy::Optional;

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = KrakenConnector::builder(relaxed_config)
        .symbols([Symbol::from("BTC/USD")])
        .checksum_strategy(Arc::new(NoChecksum))
        .sink(sink)
        .build()?;

    let _ = strict_config;
    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.3 Coinbase Exchange (`coinbase`)

> **Note:** Coinbase Exchange validates that `streams.heartbeat` is `true` whenever `streams.depth` is `true`.

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Symbol};
use mdc_providers::coinbase::{CoinbaseConfig, CoinbaseConnector, CoinbaseStreams};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Strict mode (default): 5s heartbeat silence monitor + 3,000 sequence-jump threshold
    let mut config = CoinbaseConfig::default();
    config.heartbeat_timeout = Duration::from_secs(5);
    config.sequence_gap_threshold = Some(3_000);

    // To disable sequence-jump resyncs while keeping depth, set:
    // config.sequence_gap_threshold = None;

    // For trade-only mode without depth/heartbeat resync FSM:
    let trade_only_config = CoinbaseConfig {
        streams: CoinbaseStreams {
            depth: false,
            heartbeat: false,
            matches: true,
        },
        ..CoinbaseConfig::default()
    };

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = CoinbaseConnector::builder(config)
        .symbols([Symbol::from("BTC-USD")])
        .sink(sink)
        .build()?;

    let _ = trade_only_config;
    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.4 Coinbase Advanced Trade (`coinbase-advanced`)

**With strict connection-wide sequence tracking (default) vs. relaxed `SequencePolicy::Optional`:**
```rust,no_run
use mdc_core::{FnSink, SequencePolicy, Symbol};
use mdc_providers::coinbase_advanced::{CoinbaseAdvancedConfig, CoinbaseAdvancedConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Strict mode (default): SequencePolicy::Required + reset_threshold = 2
    let strict_config = CoinbaseAdvancedConfig::default();

    // Relaxed mode: SequencePolicy::Optional
    let relaxed_config = CoinbaseAdvancedConfig::builder()
        .with_sequence_policy(SequencePolicy::Optional)
        .with_reset_threshold(5)
        .build()?;

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = CoinbaseAdvancedConnector::builder(relaxed_config)
        .symbols([Symbol::from("BTC-USD")])
        .owned_sink(sink)
        .build()?;

    let _ = strict_config;
    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.5 Bitfinex WebSocket v2 (`bitfinex`)

Bitfinex negotiates `conf` flags `163840` (`SEQ_ALL | CHECKSUM`) and validates top-25 signed CRC32 interleaved checksums per book channel. Note that `BitfinexConnector::run` takes `(sink, cancel)` and returns `(BitfinexHandle, impl Future)`:

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Owned, Symbol};
use mdc_providers::bitfinex::BitfinexConnector;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // With L2 checksum & quarantine FSM (set .enable_depth(false).enable_trades(true) for trades only)
    let connector = BitfinexConnector::builder()
        .symbols([Symbol::from("tBTCUSD")])
        .orderbook_depth(25)?
        .resubscribe_timeout(Duration::from_secs(5))
        .quarantine_duration(Duration::from_secs(30))
        .build()?;

    let sink = Owned::new(FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(())));
    let (_handle, fut) = connector.run(sink, CancellationToken::new());
    fut.await?;
    Ok(())
}
```

---

### 5.6 Gate.io Spot v4 (`gateio`)

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Symbol, resync::CooldownPolicy};
use mdc_providers::gateio::{GateioConfig, GateioConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = GateioConnector::builder(GateioConfig::default())
        .symbols([Symbol::from("BTC_USDT")])
        .orderbook_levels(50)?
        .resync_policy(CooldownPolicy::new(Duration::from_secs(3)))
        .sink(sink)
        .build()?;

    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.7 Bitstamp WebSocket v2 (`bitstamp`)

Bitstamp gates incremental diffs by microsecond timestamp ($t > T_{\text{snapshot}}$) and replays diffs buffered while a REST snapshot is in flight:

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Symbol, resync::CooldownPolicy};
use mdc_providers::bitstamp::{BitstampConfig, BitstampConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = BitstampConnector::builder(BitstampConfig::default())
        .symbols([Symbol::from("btcusd")])
        .buffer_capacity(2048)
        .resync_policy(CooldownPolicy::new(Duration::from_secs(5)))
        .sink(sink)
        .build()?;

    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.8 Huobi / HTX Spot (`huobi`)

Huobi streams gzip-compressed `mbp.{levels}` frames, enforces `prevSeqNum == seqNum` continuity, and requests in-band snapshots over the WebSocket on sequence gaps:

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Symbol, resync::CooldownPolicy};
use mdc_providers::huobi::{HuobiConfig, HuobiConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HuobiConfig::builder()
        .mbp_levels(150)
        .silence_timeout(Duration::from_secs(15))
        .build()?;

    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = HuobiConnector::builder(config)
        .symbols([Symbol::from("btcusdt")])
        .resync_policy(CooldownPolicy::new(Duration::from_secs(5)))
        .sink(sink)
        .build()?;

    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.9 LMAX Digital (`lmax`)

LMAX publishes full top-20 `ORDER_BOOK` snapshots on every update (eliminating delta drift) and enforces control command pacing:

```rust,no_run
use mdc_core::{FnSink, Symbol};
use mdc_providers::lmax::{LmaxConfig, LmaxConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink = FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(()));
    let (connector, _handle) = LmaxConnector::builder(LmaxConfig::default())
        .symbols([Symbol::from("btc-usd")])
        .sink(sink)
        .build()?;

    connector.run(CancellationToken::new()).await?;
    Ok(())
}
```

---

### 5.10 VWD / Infront FX REST (`vwd`)

VWD polls authenticated `SNAPQUOTE` endpoints, extracts per-side `bid.dateTime` and `ask.dateTime` provenance (never trusting `last.dateTime`), and validates direct, inverse (`allow_inverse`), and scaled quotes:

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Owned};
use mdc_providers::vwd::{VwdConfig, VwdConnector, VwdCredentials, VwdSymbol};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = VwdConfig::builder()
        .account_path("customer-account")
        .credentials(VwdCredentials::new("username", "password"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .add_symbol(VwdSymbol::with_inverse("USDJPY"))
        .poll_interval(Duration::from_secs(5))
        .emit_book_ticker(true) // Also emit BookTicker alongside single-level L2Snapshot
        .build()?;

    let connector = VwdConnector::builder(config).build()?;
    let sink = Owned::new(FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(())));
    let (_handle, join) = connector.run(sink, CancellationToken::new());
    join.await??;
    Ok(())
}
```

---

### 5.11 Frankfurter ECB Reference Rates (`frankfurter`)

Polls European Central Bank reference FX rates, enforces full batch completeness across all configured quote currencies, and normalizes civil dates to UTC midnight timestamps:

```rust,no_run
use std::time::Duration;
use mdc_core::{FnSink, Owned};
use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = FrankfurterConfig::builder()
        .base_currency("EUR")
        .quote_currencies(["USD", "GBP", "CHF", "JPY"])
        .poll_interval(Duration::from_secs(3600))
        .build()?;

    let connector = FrankfurterConnector::builder(config).build()?;
    let sink = Owned::new(FnSink::new(|_ev| Ok::<(), std::convert::Infallible>(())));
    let (_handle, join) = connector.run(sink, CancellationToken::new());
    join.await??;
    Ok(())
}
```

---

## 6. Runtime Operator Control (`ConnectorHandle`)

Every connector returns a cloneable `ConnectorHandle` (or `BitfinexHandle`) that allows you to trigger manual resynchronizations, socket reconnects, or pause/resume cycles at runtime:

- **Pause / Resume**: `handle.pause().await` cleanly closes or pauses the feed and emits `IntegrityAlert::Disconnected { kind: DisconnectKind::Paused }`; `handle.resume().await` reconnects and forces a fresh snapshot resynchronization.
- **Manual Resync**: `handle.resync_symbol(Symbol::from("BTCUSDT")).await` / `handle.resync_all().await` invalidates in-memory book state and requests a fresh snapshot.
- **Forced Reconnect**: `handle.reconnect().await` drops the active socket and reconnects immediately.
