#![cfg(feature = "all")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Shutdown and connect-timeout bounds against a peer that accepts TCP but
//! never completes the WebSocket handshake.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mdc_core::Symbol;
use mdc_testkit::RecordingSink;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use url::Url;

const SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// Accepts TCP connections and holds them open without ever replying.
struct Blackhole {
    url: Url,
    accepted: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Blackhole {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let (a, n) = (accepted.clone(), notify.clone());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
                a.fetch_add(1, Ordering::SeqCst);
                n.notify_waiters();
            }
        });
        Self {
            url: Url::parse(&format!("ws://127.0.0.1:{port}")).expect("url"),
            accepted,
            notify,
        }
    }

    /// Waits until at least `target` connections were accepted.
    async fn wait_accepted(&self, target: usize) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.accepted.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }
}

async fn assert_shutdown_is_bounded<F>(run: F, hole: &Blackhole, cancel: CancellationToken)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let task = tokio::spawn(run);
    tokio::time::timeout(SHUTDOWN_BOUND, hole.wait_accepted(1))
        .await
        .expect("connector reached the blackholed handshake");
    cancel.cancel();
    let joined = tokio::time::timeout(SHUTDOWN_BOUND, task)
        .await
        .expect("shutdown bounded");
    joined.expect("connector task must not panic");
}

macro_rules! shutdown_test {
    ($name:ident, $feature:literal, $url_expr:expr, $builder:expr, $config:ty, $sym:literal, $url_field:ident) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        async fn $name() {
            let hole = Blackhole::spawn().await;
            let mut config = <$config>::default();
            config.$url_field = ($url_expr)(&hole.url);
            let (connector, _handle) = ($builder)(config)
                .symbols(vec![Symbol::from($sym)])
                .streaming_sink(RecordingSink::new())
                .build()
                .expect("build");
            let cancel = CancellationToken::new();
            let child = cancel.child_token();
            assert_shutdown_is_bounded(
                async move {
                    let _ = connector.run(child).await;
                },
                &hole,
                cancel,
            )
            .await;
        }
    };
}

shutdown_test!(
    binance_shutdown_during_blackholed_connect,
    "binance",
    |u: &Url| u.clone(),
    mdc_providers::binance::BinanceConnectorBuilder::new,
    mdc_providers::binance::BinanceConfig,
    "BTCUSDT",
    ws_base_url
);
shutdown_test!(
    kraken_shutdown_during_blackholed_connect,
    "kraken",
    |u: &Url| u.clone(),
    mdc_providers::kraken::KrakenConnectorBuilder::new,
    mdc_providers::kraken::KrakenConfig,
    "BTC/USD",
    ws_base_url
);
shutdown_test!(
    gateio_shutdown_during_blackholed_connect,
    "gateio",
    |u: &Url| u.clone(),
    mdc_providers::gateio::GateioConnectorBuilder::new,
    mdc_providers::gateio::GateioConfig,
    "BTC_USDT",
    ws_url
);
shutdown_test!(
    huobi_shutdown_during_blackholed_connect,
    "huobi",
    |u: &Url| u.clone(),
    mdc_providers::huobi::HuobiConnectorBuilder::new,
    mdc_providers::huobi::HuobiConfig,
    "btcusdt",
    ws_url
);
shutdown_test!(
    lmax_shutdown_during_blackholed_connect,
    "lmax",
    |u: &Url| u.clone(),
    mdc_providers::lmax::LmaxConnectorBuilder::new,
    mdc_providers::lmax::LmaxConfig,
    "btc-usd",
    ws_url
);
shutdown_test!(
    bitstamp_shutdown_during_blackholed_connect,
    "bitstamp",
    |u: &Url| u.clone(),
    mdc_providers::bitstamp::BitstampConnectorBuilder::new,
    mdc_providers::bitstamp::BitstampConfig,
    "btcusd",
    ws_url
);
shutdown_test!(
    coinbase_shutdown_during_blackholed_connect,
    "coinbase",
    |u: &Url| u.to_string(),
    mdc_providers::coinbase::CoinbaseConnectorBuilder::new,
    mdc_providers::coinbase::CoinbaseConfig,
    "BTC-USD",
    ws_base_url
);
shutdown_test!(
    coinbase_advanced_shutdown_during_blackholed_connect,
    "coinbase-advanced",
    |u: &Url| u.clone(),
    mdc_providers::coinbase_advanced::CoinbaseAdvancedConnectorBuilder::new,
    mdc_providers::coinbase_advanced::CoinbaseAdvancedConfig,
    "BTC-USD",
    ws_url
);

#[cfg(feature = "bitfinex")]
#[tokio::test]
async fn bitfinex_shutdown_during_blackholed_connect() {
    let hole = Blackhole::spawn().await;
    let connector = mdc_providers::bitfinex::BitfinexConnector::builder()
        .ws_url(hole.url.to_string())
        .symbol("tBTCUSD")
        .build()
        .expect("build");
    let cancel = CancellationToken::new();
    let (_handle, runner) = connector.run(RecordingSink::new(), cancel.child_token());
    assert_shutdown_is_bounded(
        async move {
            let _ = runner.await;
        },
        &hole,
        cancel,
    )
    .await;
}

/// A blackholed handshake must be abandoned after `connection_timeout` so the
/// connector retries without any operator action.
#[cfg(feature = "binance")]
#[tokio::test]
async fn binance_connect_timeout_triggers_retry() {
    use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};

    let hole = Blackhole::spawn().await;
    let mut config = BinanceConfig::default();
    config.ws_base_url = hole.url.clone();
    config.connection_timeout = Duration::from_millis(100);
    config.initial_backoff = Duration::from_millis(10);
    config.max_backoff = Duration::from_millis(20);
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(RecordingSink::new())
        .build()
        .expect("build");
    let cancel = CancellationToken::new();
    let child = cancel.child_token();
    let task = tokio::spawn(async move { connector.run(child).await });
    tokio::time::timeout(SHUTDOWN_BOUND, hole.wait_accepted(2))
        .await
        .expect("second connect attempt after timeout");
    cancel.cancel();
    tokio::time::timeout(SHUTDOWN_BOUND, task)
        .await
        .expect("bounded")
        .expect("no panic")
        .expect("clean shutdown");
}

#[cfg(feature = "kraken")]
#[tokio::test]
async fn kraken_connect_timeout_triggers_retry() {
    use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};

    let hole = Blackhole::spawn().await;
    let mut config = KrakenConfig::default();
    config.ws_base_url = hole.url.clone();
    config.connection_timeout = Duration::from_millis(100);
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(RecordingSink::new())
        .build()
        .expect("build");
    let cancel = CancellationToken::new();
    let child = cancel.child_token();
    let task = tokio::spawn(async move { connector.run(child).await });
    tokio::time::timeout(Duration::from_secs(10), hole.wait_accepted(2))
        .await
        .expect("second connect attempt after timeout");
    cancel.cancel();
    tokio::time::timeout(SHUTDOWN_BOUND, task)
        .await
        .expect("bounded")
        .expect("no panic")
        .expect("clean shutdown");
}
