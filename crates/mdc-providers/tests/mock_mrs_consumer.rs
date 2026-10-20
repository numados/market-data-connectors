#![cfg(feature = "binance")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Market Rate Service (MRS) consumer contract.
//!
//! Validates:
//! - Checked `ProviderCapabilities` and `ConsumerRequirements` pre-connect validation.
//! - Loss-intolerant bounded channel delivery with reserved terminal fault slot.
//! - Non-blocking delivery and strictly increasing delivery ordinals within connection generation.
//! - Proper quote projection release and withdrawal lifecycle.

use std::time::Duration;

use mdc_core::{CapabilityError, ConsumerRequirements, QuoteMode, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
use mdc_sinks::{QuoteProjection, loss_intolerant_channel};
use mdc_testkit::conformance::MrsConsumerHarness;
use mdc_testkit::{MockRest, MockWsServer, NoopStreamingSink, WsAction, load_fixture};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_mrs_capabilities_and_requirements_gate() {
    let config = BinanceConfig::default();
    let builder = BinanceConnectorBuilder::<NoopStreamingSink>::new(config);

    // Valid requirements for Binance (supports both NativeTicker and L2Projection with verified trades)
    let valid_req = ConsumerRequirements {
        quote_mode: QuoteMode::NativeTicker,
        require_stable_trades: true,
        require_verified_side: true,
    };
    assert!(
        builder
            .capabilities()
            .check_requirements(&valid_req)
            .is_ok()
    );

    // Valid L2 projection requirement
    let l2_req = ConsumerRequirements {
        quote_mode: QuoteMode::L2Projection,
        require_stable_trades: true,
        require_verified_side: true,
    };
    assert!(builder.capabilities().check_requirements(&l2_req).is_ok());

    // Unsupported quote mode for Binance (ReferenceRate is central bank fix only)
    let invalid_req = ConsumerRequirements {
        quote_mode: QuoteMode::ReferenceRate,
        require_stable_trades: false,
        require_verified_side: false,
    };
    let err = builder
        .capabilities()
        .check_requirements(&invalid_req)
        .unwrap_err();
    assert!(matches!(err, CapabilityError::UnsupportedQuoteMode { .. }));
}

#[cfg(feature = "lmax")]
#[tokio::test]
async fn test_lmax_unverified_trades_rejected_by_mrs() {
    use mdc_providers::lmax::{LmaxConfig, LmaxConnectorBuilder};

    let config = LmaxConfig::default();
    let builder = LmaxConnectorBuilder::<NoopStreamingSink>::new(config);

    // LMAX does not offer verified stable trades for MRS profile
    let trade_req = ConsumerRequirements {
        quote_mode: QuoteMode::L2Projection,
        require_stable_trades: true,
        require_verified_side: false,
    };
    let err = builder
        .capabilities()
        .check_requirements(&trade_req)
        .unwrap_err();
    assert!(matches!(err, CapabilityError::UnstableTrades { .. }));
}

#[tokio::test]
async fn test_mrs_consumer_end_to_end_conformance_with_binance() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let ticker_json = load_fixture("crates/mdc-providers/tests/fixtures/binance/book_ticker.json")
        .expect("load book_ticker fixture");

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.streams.depth = false;
    config.streams.book_ticker = true;

    let cancel = CancellationToken::new();
    let (tx, mut rx) = loss_intolerant_channel(100, 1, cancel.clone());

    let req = ConsumerRequirements {
        quote_mode: QuoteMode::NativeTicker,
        require_stable_trades: true,
        require_verified_side: true,
    };

    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .requirements(&req)
        .expect("requirements satisfied")
        .symbols(vec![Symbol::from("BTCUSDT")])
        .sink(tx)
        .build()
        .expect("build connector");

    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for connection and send book ticker
    ws.wait_for_handshakes(1).await;
    let _ = ws.broadcast_text(ticker_json);

    // Ingest events via MRS harness until an available quote is projected
    let mut harness = MrsConsumerHarness::new(req);
    harness.register_symbol(Symbol::from("BTCUSDT"), 0, None);

    let mut quote_seen = false;
    for _ in 0..10 {
        let Ok(res_opt) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await else {
            break;
        };
        let res = res_opt.expect("channel stream item");
        let ev = res.expect("ok market event");
        let proj = harness.ingest_event(&ev).expect("valid event");
        if let Some(QuoteProjection::Available { .. }) = proj {
            quote_seen = true;
            break;
        }
    }

    assert!(quote_seen, "Must project an available quote");
    assert!(harness.stats().delivered_events >= 1);
    assert!(harness.stats().available_quotes >= 1);

    // Depth is off: the connector must never touch the snapshot endpoint. The barrier proves
    // everything broadcast so far was processed before the request log is read.
    ws.sync_barrier().await;
    let http_calls = rest
        .inner()
        .received_requests()
        .await
        .expect("request recording enabled");
    assert!(
        http_calls.is_empty(),
        "depth-off connector must make zero HTTP calls, got {http_calls:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_mrs_delivery_fault_overflow_halts_connector() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let ticker_json = load_fixture("crates/mdc-providers/tests/fixtures/binance/book_ticker.json")
        .expect("load book_ticker fixture");

    let mut script = Vec::new();
    for _ in 0..10 {
        script.push(WsAction::SendText(ticker_json.clone()));
    }
    ws.push_script(script);

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.streams.depth = false;
    config.streams.book_ticker = true;

    let cancel = CancellationToken::new();
    // Tiny capacity of 1: second frame will overflow and cause terminal fault
    let (tx, mut rx) = loss_intolerant_channel(1, 42, cancel.clone());

    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .sink(tx)
        .build()
        .expect("build connector");

    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for the channel to cancel the producer on delivery overflow
    mdc_testkit::within(
        Duration::from_secs(5),
        "delivery overflow cancels producer",
        cancel.cancelled(),
    )
    .await;

    // The producer cancellation token must be cancelled by the channel
    assert!(cancel.is_cancelled());

    let mut harness = MrsConsumerHarness::new(ConsumerRequirements {
        quote_mode: QuoteMode::NativeTicker,
        require_stable_trades: false,
        require_verified_side: false,
    });
    harness.register_symbol(Symbol::from("BTCUSDT"), 0, None);

    let stats = harness.run_stream(&mut rx).await.expect("drain stream");
    assert!(
        stats.delivered_events <= 1,
        "At most 1 event queued before overflow"
    );
    assert_eq!(
        stats.delivery_faults, 1,
        "Terminal delivery fault must be delivered"
    );

    let _ = runner.await;
}

#[cfg(feature = "frankfurter")]
#[tokio::test]
async fn test_frankfurter_capabilities_and_requirements_gate() {
    use mdc_providers::frankfurter::FRANKFURTER_CAPABILITIES;

    // Valid MRS reference profile requirements
    let ref_req = ConsumerRequirements {
        quote_mode: QuoteMode::ReferenceRate,
        require_stable_trades: false,
        require_verified_side: false,
    };
    assert!(
        FRANKFURTER_CAPABILITIES
            .check_requirements(&ref_req)
            .is_ok()
    );

    // Fails if stable trades required
    let trade_req = ConsumerRequirements {
        quote_mode: QuoteMode::ReferenceRate,
        require_stable_trades: true,
        require_verified_side: false,
    };
    assert!(matches!(
        FRANKFURTER_CAPABILITIES
            .check_requirements(&trade_req)
            .unwrap_err(),
        CapabilityError::UnstableTrades { .. }
    ));

    // Fails if unsupported quote mode required (e.g. NativeTicker)
    let ticker_req = ConsumerRequirements {
        quote_mode: QuoteMode::NativeTicker,
        require_stable_trades: false,
        require_verified_side: false,
    };
    assert!(matches!(
        FRANKFURTER_CAPABILITIES
            .check_requirements(&ticker_req)
            .unwrap_err(),
        CapabilityError::UnsupportedQuoteMode { .. }
    ));
}
