#![cfg(feature = "all")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Runs the shared end-to-end conformance scenarios against production connectors.

use mdc_core::Symbol;
use mdc_testkit::conformance::{self, ConnectorRun, Scenario};
use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint, MockWsServer};
use tokio_util::sync::CancellationToken;

/// Normal recording sink, except for the sink-fault scenario which fails on the first callback.
fn sink_for(scenario: Scenario) -> FaultInjectingSink {
    match scenario {
        Scenario::SinkFault => FaultInjectingSink::fail_at_callback(1),
        _ => FaultInjectingSink::new(FaultInjectionPoint::Never),
    }
}

/// Runs every standard scenario for one connector. `$start` builds the connector from the
/// mock server `$ws`, the scenario sink and a cancellation token, and returns its run future.
macro_rules! conformance_case {
    ($name:ident, $feature:literal, |$ws:ident, $sink:ident, $cancel:ident| $start:expr) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        async fn $name() {
            let report = conformance::run(
                |$ws: &MockWsServer, $sink: FaultInjectingSink| {
                    let $cancel = CancellationToken::new();
                    let stop = $cancel.clone();
                    ConnectorRun {
                        future: Box::pin($start),
                        stop: Box::new(move || stop.cancel()),
                    }
                },
                sink_for,
            )
            .await;
            assert!(report.alerts_verified > 0, "scenarios must record alerts");
        }
    };
}

conformance_case!(binance_conformance, "binance", |ws, sink, cancel| {
    let config = mdc_providers::binance::BinanceConfig::for_test(
        ws.ws_url().unwrap(),
        "http://127.0.0.1:9".parse().unwrap(),
    );
    let (connector, _handle) = mdc_providers::binance::BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(kraken_conformance, "kraken", |ws, sink, cancel| {
    let mut config = mdc_providers::kraken::KrakenConfig::default();
    config.ws_base_url = ws.ws_url().unwrap();
    config.initial_backoff = std::time::Duration::from_millis(20);
    let (connector, _handle) = mdc_providers::kraken::KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(coinbase_conformance, "coinbase", |ws, sink, cancel| {
    let mut config = mdc_providers::coinbase::CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().unwrap().to_string();
    let (connector, _handle) = mdc_providers::coinbase::CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(gateio_conformance, "gateio", |ws, sink, cancel| {
    let mut config = mdc_providers::gateio::GateioConfig::default();
    config.ws_url = ws.ws_url().unwrap();
    let (connector, _handle) = mdc_providers::gateio::GateioConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC_USDT")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(huobi_conformance, "huobi", |ws, sink, cancel| {
    let mut config = mdc_providers::huobi::HuobiConfig::default();
    config.ws_url = ws.ws_url().unwrap();
    let (connector, _handle) = mdc_providers::huobi::HuobiConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusdt")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(bitstamp_conformance, "bitstamp", |ws, sink, cancel| {
    let mut config = mdc_providers::bitstamp::BitstampConfig::default();
    config.ws_url = ws.ws_url().unwrap();
    let (connector, _handle) = mdc_providers::bitstamp::BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink)
        .build()
        .unwrap();
    async move { connector.run(cancel).await }
});

conformance_case!(
    coinbase_advanced_conformance,
    "coinbase-advanced",
    |ws, sink, cancel| {
        let mut config = mdc_providers::coinbase_advanced::CoinbaseAdvancedConfig::default();
        config.ws_url = ws.ws_url().unwrap();
        let (connector, _handle) =
            mdc_providers::coinbase_advanced::CoinbaseAdvancedConnectorBuilder::new(config)
                .symbols(vec![Symbol::from("BTC-USD")])
                .streaming_sink(sink)
                .build()
                .unwrap();
        async move { connector.run(cancel).await }
    }
);

conformance_case!(bitfinex_conformance, "bitfinex", |ws, sink, cancel| {
    let connector = mdc_providers::bitfinex::BitfinexConnector::builder()
        .ws_url(ws.ws_url().unwrap().to_string())
        .symbol("tBTCUSD")
        .build()
        .unwrap();
    let (handle, runner) = connector.run(sink, cancel);
    // Dropping the control handle stops a Bitfinex connector, so keep it alive.
    async move {
        let _handle = handle;
        runner.await
    }
});
