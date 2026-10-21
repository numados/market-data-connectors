#![cfg(feature = "all")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Every venue builder must delegate generic configuration rules to the
//! shared validators in `mdc_providers::config`.
//!
//! Each case asserts the builder's error text equals the shared validator's,
//! so a builder that stops calling a shared rule (or re-implements it with
//! different wording) fails here.

use mdc_core::{MdcError, Symbol, SymbolTag};
use mdc_providers::config;
use mdc_testkit::RecordingSink;

fn message(res: Result<(), MdcError>) -> String {
    match res {
        Err(MdcError::Config(msg)) => msg,
        other => panic!("expected MdcError::Config, got {other:?}"),
    }
}

fn expected_empty_symbols() -> String {
    message(config::validate_symbols(&[] as &[(Symbol, SymbolTag)]))
}

fn expected_zero_per_connection() -> String {
    message(config::validate_symbols_per_connection(0))
}

fn expected_zero_buffer() -> String {
    message(config::validate_resync_buffer_capacity(0))
}

fn err<T>(res: Result<T, MdcError>) -> String {
    message(res.map(|_| ()))
}

macro_rules! bucketed_rules {
    ($name:ident, $feature:literal, $builder:expr, $config:expr, $sym:literal, $per_conn:ident) => {
        #[cfg(feature = $feature)]
        #[test]
        fn $name() {
            let make = |mutate: &dyn Fn(&mut _), symbols: Vec<&str>| {
                let mut cfg = $config;
                mutate(&mut cfg);
                ($builder)(cfg)
                    .symbols(symbols.into_iter().map(Symbol::from))
                    .streaming_sink(RecordingSink::new())
                    .build()
            };
            assert_eq!(err(make(&|_| {}, vec![])), expected_empty_symbols());
            assert_eq!(
                err(make(&|c| c.$per_conn = 0, vec![$sym])),
                expected_zero_per_connection()
            );
        }
    };
}

bucketed_rules!(
    binance_delegates_shared_rules,
    "binance",
    mdc_providers::binance::BinanceConnectorBuilder::new,
    mdc_providers::binance::BinanceConfig::default(),
    "BTCUSDT",
    symbols_per_connection
);
bucketed_rules!(
    kraken_delegates_shared_rules,
    "kraken",
    mdc_providers::kraken::KrakenConnectorBuilder::new,
    mdc_providers::kraken::KrakenConfig::default(),
    "BTC/USD",
    symbols_per_connection
);
bucketed_rules!(
    gateio_delegates_shared_rules,
    "gateio",
    mdc_providers::gateio::GateioConnectorBuilder::new,
    mdc_providers::gateio::GateioConfig::default(),
    "BTC_USDT",
    symbols_per_connection
);
bucketed_rules!(
    huobi_delegates_shared_rules,
    "huobi",
    mdc_providers::huobi::HuobiConnectorBuilder::new,
    mdc_providers::huobi::HuobiConfig::default(),
    "btcusdt",
    symbols_per_connection
);
bucketed_rules!(
    lmax_delegates_shared_rules,
    "lmax",
    mdc_providers::lmax::LmaxConnectorBuilder::new,
    mdc_providers::lmax::LmaxConfig::default(),
    "btc-usd",
    symbols_per_connection
);
bucketed_rules!(
    bitstamp_delegates_shared_rules,
    "bitstamp",
    mdc_providers::bitstamp::BitstampConnectorBuilder::new,
    mdc_providers::bitstamp::BitstampConfig::default(),
    "btcusd",
    symbols_per_connection
);
bucketed_rules!(
    coinbase_delegates_shared_rules,
    "coinbase",
    mdc_providers::coinbase::CoinbaseConnectorBuilder::new,
    mdc_providers::coinbase::CoinbaseConfig::default(),
    "BTC-USD",
    products_per_connection
);
bucketed_rules!(
    coinbase_advanced_delegates_shared_rules,
    "coinbase-advanced",
    mdc_providers::coinbase_advanced::CoinbaseAdvancedConnectorBuilder::new,
    mdc_providers::coinbase_advanced::CoinbaseAdvancedConfig::default(),
    "BTC-USD",
    symbols_per_connection
);

#[cfg(feature = "binance")]
#[test]
fn binance_delegates_buffer_capacity() {
    use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
    let mut cfg = BinanceConfig::default();
    cfg.resync_buffer_capacity = 0;
    let res = BinanceConnectorBuilder::new(cfg)
        .symbols([Symbol::from("BTCUSDT")])
        .streaming_sink(RecordingSink::new())
        .build();
    assert_eq!(err(res), expected_zero_buffer());
}

#[cfg(feature = "gateio")]
#[test]
fn gateio_delegates_buffer_capacity() {
    use mdc_providers::gateio::{GateioConfig, GateioConnectorBuilder};
    let mut cfg = GateioConfig::default();
    cfg.resync_buffer_capacity = 0;
    let res = GateioConnectorBuilder::new(cfg)
        .symbols([Symbol::from("BTC_USDT")])
        .streaming_sink(RecordingSink::new())
        .build();
    assert_eq!(err(res), expected_zero_buffer());
}

#[cfg(feature = "huobi")]
#[test]
fn huobi_delegates_buffer_capacity() {
    use mdc_providers::huobi::{HuobiConfig, HuobiConnectorBuilder};
    let mut cfg = HuobiConfig::default();
    cfg.resync_buffer_capacity = 0;
    let res = HuobiConnectorBuilder::new(cfg)
        .symbols([Symbol::from("btcusdt")])
        .streaming_sink(RecordingSink::new())
        .build();
    assert_eq!(err(res), expected_zero_buffer());
}

#[cfg(feature = "bitstamp")]
#[test]
fn bitstamp_delegates_buffer_capacity() {
    use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder};
    let mut cfg = BitstampConfig::default();
    cfg.buffer_capacity = 0;
    let res = BitstampConnectorBuilder::new(cfg)
        .symbols([Symbol::from("btcusd")])
        .streaming_sink(RecordingSink::new())
        .build();
    assert_eq!(err(res), expected_zero_buffer());
}

#[cfg(feature = "kraken")]
#[test]
fn kraken_delegates_batch_size_and_depth() {
    use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
    let build = |mutate: &dyn Fn(&mut KrakenConfig)| {
        let mut cfg = KrakenConfig::default();
        mutate(&mut cfg);
        KrakenConnectorBuilder::new(cfg)
            .symbols([Symbol::from("BTC/USD")])
            .streaming_sink(RecordingSink::new())
            .build()
    };
    assert_eq!(
        err(build(&|c| c.resubscribe_batch_size = 0)),
        message(config::validate_resubscribe_batch_size(0))
    );
    assert_eq!(
        err(build(&|c| c.depth = 7)),
        message(config::validate_depth_in_set(
            7,
            &[10, 25, 100, 500, 1000],
            "Kraken"
        ))
    );
}

#[cfg(feature = "bitfinex")]
#[test]
fn bitfinex_delegates_shared_rules() {
    use mdc_providers::bitfinex::{BOOK_DEPTHS, BitfinexConnector};

    // Empty symbol list: same text as the shared validator.
    assert_eq!(
        err(BitfinexConnector::builder().build()),
        expected_empty_symbols()
    );

    // Depth: the exported validator uses the shared set validator; the legacy setter maps
    // that rejection back to `ConfigError` (see `bitfinex_public_config_error_api_is_stable`).
    let expected_depth = message(config::validate_depth_in_set(50, &BOOK_DEPTHS, "Bitfinex"));
    assert_eq!(
        message(mdc_providers::bitfinex::validate_book_depth(50)),
        expected_depth
    );
}

/// Compile-time and behavioural guard for the legacy public Bitfinex API: the exported
/// `ConfigError`, `orderbook_depth -> Result<Self, ConfigError>` and
/// `BitfinexConfig::validate -> Result<(), ConfigError>`.
#[cfg(feature = "bitfinex")]
#[test]
fn bitfinex_public_config_error_api_is_stable() {
    use mdc_providers::bitfinex::{
        BitfinexConfig, BitfinexConnector, BitfinexConnectorBuilder, ConfigError,
    };

    let rejected: Result<BitfinexConnectorBuilder, ConfigError> =
        BitfinexConnector::builder().orderbook_depth(50);
    match rejected {
        Err(ConfigError::InvalidDepth { depth }) => assert_eq!(depth, 50),
        other => panic!("expected InvalidDepth, got {:?}", other.map(|_| ())),
    }
    assert!(BitfinexConnector::builder().orderbook_depth(250).is_ok());

    let validated: Result<(), ConfigError> = BitfinexConfig::default().validate();
    assert_eq!(validated, Err(ConfigError::EmptySymbols));

    let mut config = BitfinexConfig::default();
    config.symbols.push(Symbol::from("tBTCUSD"));
    config.orderbook_depth = 7;
    assert_eq!(
        config.validate(),
        Err(ConfigError::InvalidDepth { depth: 7 })
    );
}
