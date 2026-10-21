#![cfg(feature = "all")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Runs the shared end-to-end conformance scenarios against production connectors.

use std::time::Duration;

use mdc_core::Symbol;
use mdc_testkit::conformance::{
    self, ConnectorRun, Fault, Frame, Harness, PauseResume, Scenario, VenueScript,
};
use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint};
use tokio_util::sync::CancellationToken;

/// Normal recording sink, except for the sink-fault scenario which fails on the first callback.
fn sink_for(scenario: Scenario) -> FaultInjectingSink {
    match scenario {
        Scenario::SinkFault => FaultInjectingSink::fail_at_callback(1),
        _ => FaultInjectingSink::new(FaultInjectionPoint::Never),
    }
}

/// Pause/resume hooks driving a connector handle that exposes `pause()` and `resume()`.
macro_rules! pause_resume {
    ($handle:expr) => {{
        let pause_handle = $handle.clone();
        let resume_handle = $handle.clone();
        PauseResume {
            pause: Box::new(move || {
                let handle = pause_handle.clone();
                Box::pin(async move { handle.pause().await.unwrap() })
            }),
            resume: Box::new(move || {
                let handle = resume_handle.clone();
                Box::pin(async move { handle.resume().await.unwrap() })
            }),
        }
    }};
}

/// Runs every standard scenario for one connector and checks which ones it skipped.
///
/// `$start` builds the connector from the harness `$h` (mock servers and scenario), the scenario
/// sink and a cancellation token, and evaluates to `(run future, pause/resume hooks)`.
/// `$skipped` lists the scenarios the venue cannot drive; any drift from it fails the test.
macro_rules! conformance_case {
    (
        $name:ident, $feature:literal, $script:expr, skipped: [$($skipped:expr),* $(,)?],
        |$h:ident, $sink:ident, $cancel:ident| $start:expr
    ) => {
        #[cfg(feature = $feature)]
        #[tokio::test]
        async fn $name() {
            let script: VenueScript = $script;
            let report = conformance::run(
                &script,
                |$h: &Harness<'_>, $sink: FaultInjectingSink| {
                    let $cancel = CancellationToken::new();
                    let stop = $cancel.clone();
                    let (future, hooks) = $start;
                    let run = ConnectorRun::new(future, move || stop.cancel());
                    match hooks {
                        Some(hooks) => run.with_pause_resume(hooks),
                        None => run,
                    }
                },
                sink_for,
            )
            .await;
            let expected: Vec<Scenario> = vec![$($skipped),*];
            let mut actual: Vec<Scenario> =
                report.scenarios_skipped.iter().map(|(s, _)| *s).collect();
            let order = |s: &Scenario| Scenario::ALL.iter().position(|a| a == s);
            let mut expected_sorted = expected.clone();
            expected_sorted.sort_by_key(order);
            actual.sort_by_key(order);
            assert_eq!(
                actual,
                expected_sorted,
                "skipped scenarios drifted: {:?}",
                report.scenarios_skipped
            );
            assert!(report.alerts_verified > 0, "scenarios must record alerts");
            assert!(report.snapshots_verified > 0, "scenarios must publish snapshots");
            assert!(
                report.deltas_verified + report.trades_verified + report.tickers_verified > 0,
                "scenarios must publish live market data"
            );
        }
    };
}

/// Shorter watchdog for the silence scenario so the alert arrives within the bound.
fn silence_watchdog(scenario: Scenario) -> (Duration, Duration) {
    if scenario == Scenario::Silence {
        (Duration::from_millis(150), Duration::from_millis(400))
    } else {
        (Duration::from_secs(5), Duration::from_secs(5))
    }
}

#[cfg(feature = "binance")]
fn binance_script() -> VenueScript {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    // d1 bridges snapshot 100, d2 is contiguous, d3 leaves a gap (expected 111, received 120)
    // and bridges the recovery snapshot 122.
    let depth = |first: u64, last: u64, price: &str| {
        Frame::text(format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":{first},"u":{last},"b":[["25000.00","{price}"]],"a":[["25001.00","2.5"]]}}}}"#
        ))
    };
    let snapshot_body = |id: u64| {
        format!(
            r#"{{"lastUpdateId":{id},"bids":[["25000.00","1.0"]],"asks":[["25001.00","2.0"]]}}"#
        )
    };
    VenueScript::new(vec![depth(95, 105, "1.5")], vec![depth(106, 110, "1.6")])
        .with_gap(Fault {
            frames: vec![depth(120, 125, "1.7")],
            recovery: Some(Vec::new()),
        })
        .with_rest(
            &[Scenario::RateLimited, Scenario::StaleSnapshot],
            move |rest, scenario| {
                Box::pin(async move {
                    let json = |status: u16, body: String| {
                        ResponseTemplate::new(status).set_body_raw(body, "application/json")
                    };
                    let mount = |response: ResponseTemplate, times: Option<u64>| {
                        let mock = Mock::given(method("GET"))
                            .and(path("/api/v3/depth"))
                            .respond_with(response);
                        match times {
                            Some(n) => mock.up_to_n_times(n),
                            None => mock,
                        }
                    };
                    let (first, then) = match scenario {
                        Scenario::Gap => {
                            (json(200, snapshot_body(100)), json(200, snapshot_body(122)))
                        }
                        Scenario::RateLimited => (
                            ResponseTemplate::new(429)
                                .insert_header("retry-after", "1")
                                .set_body_string(
                                    r#"{"code":-1003,"msg":"Too much request rate."}"#,
                                ),
                            json(200, snapshot_body(100)),
                        ),
                        // Older than the first buffered delta (U=95), hence stale.
                        Scenario::StaleSnapshot => {
                            (json(200, snapshot_body(50)), json(200, snapshot_body(100)))
                        }
                        _ => (json(200, snapshot_body(100)), json(200, snapshot_body(100))),
                    };
                    mount(first, Some(1)).mount(rest.inner()).await;
                    mount(then, None).mount(rest.inner()).await;
                })
            },
        )
}

conformance_case!(
    binance_conformance,
    "binance",
    binance_script(),
    skipped: [Scenario::BadChecksum],
    |h, sink, cancel| {
        let mut config = mdc_providers::binance::BinanceConfig::for_test(
            h.ws.ws_url().unwrap(),
            h.rest.url().unwrap(),
        );
        (config.watchdog_ping_after, config.watchdog_reconnect_after) =
            silence_watchdog(h.scenario);
        config.snapshot_retry_cooldown = Duration::from_millis(10);
        let (connector, handle) = mdc_providers::binance::BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

/// Reads a text fixture below `crates/mdc-providers/tests/fixtures`.
#[cfg(any(
    feature = "kraken",
    feature = "coinbase",
    feature = "gateio",
    feature = "huobi",
    feature = "bitstamp",
    feature = "coinbase-advanced",
    feature = "bitfinex"
))]
fn fixture(name: &str) -> String {
    mdc_testkit::load_fixture(format!("crates/mdc-providers/tests/fixtures/{name}")).unwrap()
}

#[cfg(feature = "kraken")]
fn kraken_script() -> VenueScript {
    let mut bad_update: serde_json::Value =
        serde_json::from_str(&fixture("kraken/depth_update.json")).unwrap();
    bad_update["data"][0]["checksum"] = serde_json::json!(999_999_999);
    VenueScript::new(
        vec![Frame::text(fixture("kraken/order_book_snapshot.json"))],
        vec![Frame::text(fixture("kraken/depth_update.json"))],
    )
    .with_bad_checksum(Fault {
        frames: vec![Frame::text(bad_update.to_string())],
        recovery: None,
    })
}

conformance_case!(
    kraken_conformance,
    "kraken",
    kraken_script(),
    skipped: [
        Scenario::Gap,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::kraken::KrakenConfig::default();
        config.ws_base_url = h.ws.ws_url().unwrap();
        config.initial_backoff = Duration::from_millis(20);
        (config.watchdog_ping_after, config.watchdog_reconnect_after) =
            silence_watchdog(h.scenario);
        let (connector, handle) = mdc_providers::kraken::KrakenConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC/USD")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "coinbase")]
fn coinbase_script() -> VenueScript {
    VenueScript::new(
        vec![Frame::text(fixture("coinbase/order_book_snapshot.json"))],
        vec![Frame::text(fixture("coinbase/depth_update.json"))],
    )
}

conformance_case!(
    coinbase_conformance,
    "coinbase",
    coinbase_script(),
    skipped: [
        Scenario::Gap,
        Scenario::BadChecksum,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::coinbase::CoinbaseConfig::default();
        config.ws_base_url = h.ws.ws_url().unwrap().to_string();
        config.initial_backoff = Duration::from_millis(20);
        (config.watchdog_ping_after, config.watchdog_reconnect_after) =
            silence_watchdog(h.scenario);
        let (connector, handle) = mdc_providers::coinbase::CoinbaseConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC-USD")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "gateio")]
fn gateio_script() -> VenueScript {
    // The live update covers 123457..=123460; skipping ahead leaves a gap.
    let mut gapped: serde_json::Value =
        serde_json::from_str(&fixture("gateio/depth_update.json")).unwrap();
    gapped["result"]["U"] = serde_json::json!(123_470);
    gapped["result"]["u"] = serde_json::json!(123_471);
    VenueScript::new(
        vec![Frame::text(fixture("gateio/order_book_snapshot.json"))],
        vec![Frame::text(fixture("gateio/depth_update.json"))],
    )
    .with_gap(Fault {
        frames: vec![Frame::text(gapped.to_string())],
        recovery: None,
    })
}

conformance_case!(
    gateio_conformance,
    "gateio",
    gateio_script(),
    skipped: [
        Scenario::BadChecksum,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::gateio::GateioConfig::default();
        config.ws_url = h.ws.ws_url().unwrap();
        (config.watchdog_ping_after, config.watchdog_reconnect_after) =
            silence_watchdog(h.scenario);
        let (connector, handle) = mdc_providers::gateio::GateioConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC_USDT")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "huobi")]
fn huobi_script() -> VenueScript {
    use std::io::Write;

    fn gzip(value: &serde_json::Value) -> Frame {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(value.to_string().as_bytes()).unwrap();
        Frame::binary(encoder.finish().unwrap())
    }
    fn delta(prev: u64, seq: u64) -> Frame {
        let mut value: serde_json::Value =
            serde_json::from_str(&fixture("huobi/depth_update.json")).unwrap();
        value["tick"]["prevSeqNum"] = serde_json::json!(prev);
        value["tick"]["seqNum"] = serde_json::json!(seq);
        gzip(&value)
    }

    // The snapshot answers the client's `req` and must echo its id.
    let snapshot = Frame::computed(|messages| {
        let id = messages.iter().find_map(|message| {
            let value: serde_json::Value = serde_json::from_str(message).ok()?;
            (value["req"] == "market.btcusdt.mbp.400")
                .then(|| value["id"].as_str().map(str::to_owned))?
        })?;
        let mut value: serde_json::Value =
            serde_json::from_str(&fixture("huobi/order_book_snapshot.json")).unwrap();
        value["id"] = serde_json::Value::String(id);
        value["data"]["seqNum"] = serde_json::json!(100);
        Some(gzip(&value))
    });
    let ack: serde_json::Value = serde_json::from_str(&fixture("huobi/sub_ack.json")).unwrap();
    VenueScript::new(vec![gzip(&ack), snapshot], vec![delta(100, 101)]).with_gap(Fault {
        frames: vec![delta(105, 106)],
        recovery: None,
    })
}

conformance_case!(
    huobi_conformance,
    "huobi",
    huobi_script(),
    skipped: [
        Scenario::BadChecksum,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::huobi::HuobiConfig::default();
        config.ws_url = h.ws.ws_url().unwrap();
        config.snapshot_cooldown = Duration::from_millis(50);
        if h.scenario == Scenario::Silence {
            config.silence_timeout = Duration::from_millis(400);
        }
        let (connector, handle) = mdc_providers::huobi::HuobiConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("btcusdt")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "coinbase-advanced")]
fn coinbase_advanced_script() -> VenueScript {
    // The connection-wide sequence runs 1 (snapshot), 2 (live); skipping to 5 is a gap, and the
    // recovery snapshot continues the same connection-wide sequence at 6.
    let mut gapped: serde_json::Value =
        serde_json::from_str(&fixture("coinbase_advanced/depth_update.json")).unwrap();
    gapped["sequence_num"] = serde_json::json!(5);
    let mut recovery: serde_json::Value =
        serde_json::from_str(&fixture("coinbase_advanced/order_book_snapshot.json")).unwrap();
    recovery["sequence_num"] = serde_json::json!(6);
    VenueScript::new(
        vec![Frame::text(fixture(
            "coinbase_advanced/order_book_snapshot.json",
        ))],
        vec![Frame::text(fixture("coinbase_advanced/depth_update.json"))],
    )
    .with_gap(Fault {
        frames: vec![Frame::text(gapped.to_string())],
        recovery: Some(vec![Frame::text(recovery.to_string())]),
    })
}

conformance_case!(
    coinbase_advanced_conformance,
    "coinbase-advanced",
    coinbase_advanced_script(),
    skipped: [
        Scenario::BadChecksum,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::coinbase_advanced::CoinbaseAdvancedConfig::default();
        config.ws_url = h.ws.ws_url().unwrap();
        if h.scenario == Scenario::Silence {
            config.silence_timeout = Duration::from_millis(400);
        }
        let (connector, handle) =
            mdc_providers::coinbase_advanced::CoinbaseAdvancedConnectorBuilder::new(config)
                .symbols(vec![Symbol::from("BTC-USD")])
                .streaming_sink(sink)
                .build()
                .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "bitstamp")]
fn bitstamp_script() -> VenueScript {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    // Snapshot at microtimestamp ...100_000; diffs newer than it are replayed or emitted.
    let diff = |micros: u64, bid: &str| {
        Frame::text(
            serde_json::json!({
                "event": "data",
                "channel": "diff_order_book_btcusd",
                "data": {
                    "timestamp": "1700000000",
                    "microtimestamp": micros.to_string(),
                    "bids": [[bid, "1.00000000"]],
                    "asks": []
                }
            })
            .to_string(),
        )
    };
    VenueScript::new(
        vec![diff(1_700_000_000_200_000, "50000.10")],
        vec![diff(1_700_000_000_300_000, "50000.20")],
    )
    .with_rest(&[Scenario::RateLimited], |rest, scenario| {
        Box::pin(async move {
            let mount = |response: ResponseTemplate| {
                Mock::given(method("GET"))
                    .and(path("/order_book/btcusd/"))
                    .respond_with(response)
            };
            let ok = || {
                ResponseTemplate::new(200).set_body_raw(
                    fixture("bitstamp/order_book_snapshot.json"),
                    "application/json",
                )
            };
            if scenario == Scenario::RateLimited {
                mount(ResponseTemplate::new(429).insert_header("retry-after", "1"))
                    .up_to_n_times(1)
                    .mount(rest.inner())
                    .await;
            }
            mount(ok()).mount(rest.inner()).await;
        })
    })
    .skip(Scenario::Gap, "diff stream carries no sequence numbers")
    .skip(
        Scenario::StaleSnapshot,
        "timestamp-gated: older snapshots never roll the gate back",
    )
    .skip(Scenario::Silence, "bitstamp has no silence watchdog")
}

conformance_case!(
    bitstamp_conformance,
    "bitstamp",
    bitstamp_script(),
    skipped: [
        Scenario::Silence,
        Scenario::Gap,
        Scenario::BadChecksum,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let mut config = mdc_providers::bitstamp::BitstampConfig::default();
        config.ws_url = h.ws.ws_url().unwrap();
        config.rest_url = h.rest.url().unwrap();
        config.snapshot_retry_cooldown = Duration::from_millis(50);
        let (connector, handle) = mdc_providers::bitstamp::BitstampConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("btcusd")])
            .streaming_sink(sink)
            .build()
            .unwrap();
        (
            async move { connector.run(cancel).await },
            Some(pause_resume!(handle)),
        )
    }
);

#[cfg(feature = "bitfinex")]
fn bitfinex_script() -> VenueScript {
    // Channel 266343 is acknowledged, then snapshotted; resubscribing repeats both.
    let snapshot = vec![
        Frame::text(fixture("bitfinex/conf_ok.json")),
        Frame::text(fixture("bitfinex/subscribed_book.json")),
        Frame::text(fixture("bitfinex/order_book_snapshot.json")),
    ];
    VenueScript::new(
        snapshot,
        vec![Frame::text(fixture("bitfinex/depth_update.json"))],
    )
    .with_bad_checksum(Fault {
        frames: vec![Frame::text(r#"[266343, "cs", 999999999, 1614604253634]"#)],
        recovery: None,
    })
    .skip(Scenario::Gap, "book frames carry no sequence numbers")
}

conformance_case!(
    bitfinex_conformance,
    "bitfinex",
    bitfinex_script(),
    skipped: [
        Scenario::Gap,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
    ],
    |h, sink, cancel| {
        let (ping_after, reconnect_after) = silence_watchdog(h.scenario);
        let connector = mdc_providers::bitfinex::BitfinexConnector::builder()
            .ws_url(h.ws.ws_url().unwrap().to_string())
            .symbol("tBTCUSD")
            .enable_trades(false)
            .watchdog_ping_after(ping_after)
            .watchdog_reconnect_after(reconnect_after)
            .build()
            .unwrap();
        let (handle, runner) = connector.run(sink, cancel);
        let hooks = pause_resume!(handle);
        // Dropping the control handle stops a Bitfinex connector, so keep it alive.
        (
            async move {
                let _handle = handle;
                runner.await
            },
            Some(hooks),
        )
    }
);
