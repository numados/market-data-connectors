#![cfg(all(feature = "binance", feature = "huobi"))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Delivery identity, generation allocation, and sink contract tests.

use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use mdc_core::{IntegrityAlert, MdcError, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
use mdc_providers::huobi::{HuobiConfig, HuobiConnectorBuilder, HuobiStreams};
use mdc_testkit::{
    FaultInjectingSink, FaultInjectionPoint, MockRest, MockWsServer, RecordingSink, WsAction,
    load_fixture,
};
use tokio_util::sync::CancellationToken;

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

#[tokio::test]
async fn test_multi_bucket_delivery_stamp_uniqueness() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;
    rest.mock_binance_depth_ok("ETHUSDT", &snapshot_json).await;

    // Send depth frames for both symbols
    let depth_btc = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":1,"u":2,"b":[["25000.00","1.5"]],"a":[["25001.00","2.5"]]}}"#;
    let depth_eth = r#"{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"ETHUSDT","U":1,"u":2,"b":[["1800.00","10.0"]],"a":[["1801.00","20.0"]]}}"#;

    ws.push_script(vec![
        WsAction::SendText(depth_btc.to_string()),
        WsAction::SendText(depth_eth.to_string()),
    ]);

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.initial_backoff = Duration::from_millis(20);
    // Force 1 symbol per connection bucket so we have 2 concurrent connection tasks
    config.symbols_per_connection = 1;

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for frames to be processed
    sink.wait_for_delivery_stamps(2).await;
    cancel.cancel();

    let _ = runner.await;

    let stamps = sink.delivery_stamps();
    assert!(!stamps.is_empty(), "expected recorded delivery stamps");

    let mut seen = HashSet::new();
    for stamp in &stamps {
        assert!(stamp.connection_id > 0, "connection_id must be assigned");
        assert!(stamp.run.0 != [0u8; 16], "run_id must be non-zero");

        let key = (
            stamp.run,
            stamp.connection_id,
            stamp.connection_generation,
            stamp.ordinal,
        );
        assert!(
            seen.insert(key),
            "duplicate delivery stamp (run, connection_id, generation, ordinal) found: {key:?}"
        );
    }
}

#[tokio::test]
async fn test_alert_fault_injection_terminal_sink_failure() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.initial_backoff = Duration::from_millis(20);

    // Injects fault specifically on alert emission
    let sink = FaultInjectingSink::new(FaultInjectionPoint::Alert);
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
    assert!(res.is_ok(), "connector should terminate upon sink failure");

    let run_res = res.unwrap().expect("runner task panicked");
    assert!(
        matches!(run_res, Err(MdcError::Sink(_))),
        "expected Err(MdcError::Sink(_)), got: {run_res:?}"
    );

    assert!(sink.is_tripped(), "fault point should have tripped");
    let alerts = sink.alerts();
    assert!(
        alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
        "expected terminal SinkFailure alert, got: {alerts:?}"
    );
}

#[tokio::test]
async fn test_huobi_mid_book_abort() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = HuobiStreams {
        mbp: true,
        trades: false,
    };

    // Fault on the 1st level
    let sink = FaultInjectingSink::new(FaultInjectionPoint::Level(1));
    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusdt")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Find the snapshot request ID sent by client
    let req_msg = ws
        .wait_for_client_message_matching(|msg| {
            serde_json::from_str::<serde_json::Value>(msg)
                .ok()
                .map(|v| v["req"] == "market.btcusdt.mbp.400")
                .unwrap_or(false)
        })
        .await;
    let v: serde_json::Value = serde_json::from_str(&req_msg).expect("valid json");
    let req_id = v["id"].as_str().expect("snapshot request id").to_string();

    // Send sub ack
    let sub_ack_raw = load_fixture("crates/mdc-providers/tests/fixtures/huobi/sub_ack.json")
        .expect("load sub_ack fixture");
    let _ = ws.broadcast_binary(gzip_compress(sub_ack_raw.as_bytes()));

    // Send snapshot response with matching ID and multiple price levels
    let snapshot_raw =
        load_fixture("crates/mdc-providers/tests/fixtures/huobi/order_book_snapshot.json")
            .expect("load snapshot fixture");
    let mut snap_val: serde_json::Value =
        serde_json::from_str(&snapshot_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
    assert!(res.is_ok(), "connector should terminate promptly");

    let run_res = res.unwrap().expect("runner task panicked");
    assert!(
        matches!(run_res, Err(MdcError::Sink(_))),
        "connector should exit with MdcError::Sink upon total failure"
    );

    // Verify mid-book abort: exactly 1 level was attempted
    assert_eq!(
        sink.level_invocations(), 1,
        "Huobi mid-book abort should stop on first failure without subsequent level iterations"
    );
}
