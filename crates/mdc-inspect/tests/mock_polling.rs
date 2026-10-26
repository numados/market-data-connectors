//! Offline E2E coverage for the two polling adapters.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(all(feature = "frankfurter", feature = "vwd"))]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use mdc_inspect::config::SessionConfig;
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus};
use mdc_inspect::provider::ProviderKind;
use mdc_inspect::provider::frankfurter::FrankfurterAdapter;
use mdc_inspect::provider::vwd::{EnvLookup, VwdAdapter};
use mdc_inspect::session::{SessionOutcome, run_session};
use mdc_testkit::{MockRest, load_fixture};
use ratatui::{Terminal, backend::TestBackend};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, ResponseTemplate};

const RATES: &str = "crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json";
const VWD: &str = "crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json";

#[derive(Clone)]
struct TestEnv(HashMap<String, String>);
impl EnvLookup for TestEnv {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

fn display(
    provider: &'static str,
) -> (
    watch::Sender<Arc<DisplaySnapshot>>,
    watch::Receiver<Arc<DisplaySnapshot>>,
) {
    watch::channel(Arc::new(DisplaySnapshot {
        provider,
        presentation: Default::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }))
}

async fn wait_for(
    rx: &mut watch::Receiver<Arc<DisplaySnapshot>>,
    status: DisplayStatus,
) -> Arc<DisplaySnapshot> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = rx.borrow_and_update().clone();
            if !snapshot.symbols.is_empty()
                && snapshot
                    .symbols
                    .iter()
                    .all(|symbol| symbol.status == status)
            {
                return snapshot;
            }
            assert_ne!(
                snapshot.session,
                DisplayStatus::Failed,
                "polling session failed while waiting for {status:?}: {snapshot:#?}"
            );
            rx.changed().await.unwrap_or_else(|error| {
                panic!("display channel closed while waiting for {status:?}: {error}")
            });
        }
    })
    .await
    .unwrap_or_else(|error| panic!("polling adapter did not reach {status:?}: {error}"))
}

async fn stop_session(root: CancellationToken, task: JoinHandle<SessionOutcome>, provider: &str) {
    root.cancel();
    assert!(root.is_cancelled());
    let mut task = task;
    let outcome = match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
        Ok(joined) => {
            joined.unwrap_or_else(|error| panic!("{provider} session join failed: {error}"))
        }
        Err(error) => {
            task.abort();
            let _ = task.await;
            panic!("{provider} session did not stop within the bound: {error}");
        }
    };
    assert_eq!(outcome.status, DisplayStatus::Stopped);
}

#[tokio::test(flavor = "current_thread")]
async fn frankfurter_reduces_reference_rate_without_book_fields() {
    let mock = MockRest::start().await;
    let fixture = load_fixture(RATES).expect("fixture");
    let failing = Arc::new(AtomicBool::new(false));
    let response_state = Arc::clone(&failing);
    Mock::given(method("GET"))
        .and(path("/v2/providers/ecb/rates"))
        .respond_with(move |_: &wiremock::Request| {
            if response_state.load(Ordering::SeqCst) {
                ResponseTemplate::new(500)
            } else {
                ResponseTemplate::new(200).set_body_raw(fixture.clone(), "application/json")
            }
        })
        .mount(mock.inner())
        .await;

    let config = SessionConfig::new(
        ProviderKind::Frankfurter,
        &["EUR".into(), "GBP".into(), "USD".into()],
    )
    .expect("config");
    let bounds = ProviderKind::Frankfurter.session_bounds();
    let (tx, mut rx) = display("Frankfurter/ECB");
    let root = CancellationToken::new();
    let task = tokio::spawn(run_session(
        Arc::new(FrankfurterAdapter::with_endpoints(mock.url().expect("url"))),
        config,
        bounds,
        4096,
        root.clone(),
        tx,
    ));

    let initial = wait_for(&mut rx, DisplayStatus::InSync).await;
    let initial_rate = initial.symbols[0].rate.as_ref().expect("initial rate");
    assert_eq!(initial_rate.rate.midpoint.to_string(), "0.8623");
    assert_eq!(
        initial_rate.rate.observation_ts.0,
        1_774_396_800_000_000_000
    );
    assert_eq!(
        initial_rate.rate.observation_date,
        initial_rate.rate.observation_ts
    );
    assert!(initial_rate.rate.received_ts.0 > 0);
    assert!(!initial_rate.historical);
    assert!(initial.symbols[0].quote.is_none());
    assert!(initial.symbols[0].bids.is_empty() && initial.symbols[0].asks.is_empty());

    failing.store(true, Ordering::SeqCst);
    let failed = wait_for(&mut rx, DisplayStatus::Unavailable).await;
    let failed_rate = failed.symbols[0].rate.as_ref().expect("historical rate");
    assert!(failed_rate.historical);
    assert!(
        failed
            .alerts
            .iter()
            .any(|alert| alert.summary == "source error (HttpStatus(500))")
    );

    failing.store(false, Ordering::SeqCst);
    let recovered = wait_for(&mut rx, DisplayStatus::InSync).await;
    let recovered_rate = recovered.symbols[0].rate.as_ref().expect("recovered rate");
    assert_eq!(recovered_rate.rate.midpoint.to_string(), "0.8623");
    assert_eq!(
        recovered_rate.rate.observation_ts.0,
        1_774_396_800_000_000_000
    );
    assert_eq!(
        recovered_rate.rate.observation_date,
        recovered_rate.rate.observation_ts
    );
    assert!(!recovered_rate.historical);
    assert_eq!(
        recovered_rate.rate.diagnostic.as_deref(),
        Some("source error (HttpStatus(500))")
    );
    let trace: Vec<&str> = recovered
        .alerts
        .iter()
        .filter(|alert| {
            alert
                .symbol
                .as_ref()
                .is_some_and(|symbol| symbol.as_str() == "EURGBP")
        })
        .map(|alert| alert.summary.as_str())
        .collect();
    let fault = trace
        .iter()
        .rposition(|summary| *summary == "source error (HttpStatus(500))")
        .expect("source failure observed");
    let recovery = trace
        .iter()
        .rposition(|summary| *summary == "resync completed")
        .expect("recovery completed");
    assert_eq!(trace.first(), Some(&"resync started"));
    assert!(fault < recovery, "{trace:?}");

    stop_session(root, task, "Frankfurter").await;
}

#[tokio::test(flavor = "current_thread")]
async fn vwd_uses_injected_credentials_and_synthetic_book() {
    let mock = MockRest::start().await;
    let fixture = load_fixture(VWD).expect("fixture");
    let failing = Arc::new(AtomicBool::new(false));
    let response_state = Arc::clone(&failing);
    Mock::given(method("GET"))
        .and(path("/account/JSON/QUOTE/SNAPQUOTE/EURUSD.XRATE.SPOT~X"))
        .and(header_exists("authorization"))
        .respond_with(move |_: &wiremock::Request| {
            if response_state.load(Ordering::SeqCst) {
                ResponseTemplate::new(401)
            } else {
                ResponseTemplate::new(200).set_body_raw(fixture.clone(), "application/json")
            }
        })
        .mount(mock.inner())
        .await;
    let env = TestEnv(HashMap::from([
        ("MDC_INSPECT_VWD_ACCOUNT_PATH".into(), "account".into()),
        ("MDC_INSPECT_VWD_USERNAME".into(), "user".into()),
        ("MDC_INSPECT_VWD_PASSWORD".into(), "password".into()),
    ]));
    let config = SessionConfig::new(ProviderKind::Vwd, &["EURUSD".into()]).expect("config");
    let (tx, mut rx) = display("VWD/Infront");
    let root = CancellationToken::new();
    let task = tokio::spawn(run_session(
        Arc::new(VwdAdapter::with_env(env, mock.url().expect("url"))),
        config,
        ProviderKind::Vwd.session_bounds(),
        4096,
        root.clone(),
        tx,
    ));

    let initial = wait_for(&mut rx, DisplayStatus::InSync).await;
    let initial_symbol = initial.symbols[0].clone();
    let initial_quote = initial_symbol.quote.as_ref().expect("initial quote");
    assert_eq!(initial_quote.quote.bid.to_string(), "1.0842");
    assert_eq!(initial_quote.quote.ask.to_string(), "1.0844");
    assert_eq!(initial_quote.quote.bid_size.to_string(), "200000000");
    assert_eq!(initial_quote.quote.ask_size.to_string(), "200000000");
    assert_eq!(
        initial_quote.quote.effective_ts.0,
        1_790_668_800_000_000_000
    );
    assert!(initial_quote.quote.received_ts.0 > 0);
    assert!(!initial_quote.historical);
    assert_eq!(initial_symbol.bids.len(), 1);
    assert_eq!(initial_symbol.asks.len(), 1);

    failing.store(true, Ordering::SeqCst);
    let failed = wait_for(&mut rx, DisplayStatus::Unavailable).await;
    let failed_quote = failed.symbols[0].quote.as_ref().expect("historical quote");
    assert!(failed_quote.historical);
    assert!(
        failed
            .alerts
            .iter()
            .any(|alert| alert.summary == "source error (HttpStatus(401))")
    );

    failing.store(false, Ordering::SeqCst);
    let recovered = wait_for(&mut rx, DisplayStatus::InSync).await;
    let recovered_quote = recovered.symbols[0]
        .quote
        .as_ref()
        .expect("recovered quote");
    assert_eq!(recovered_quote.quote.bid.to_string(), "1.0842");
    assert_eq!(recovered_quote.quote.ask.to_string(), "1.0844");
    assert_eq!(recovered_quote.quote.bid_size.to_string(), "200000000");
    assert_eq!(recovered_quote.quote.ask_size.to_string(), "200000000");
    assert!(!recovered_quote.historical);
    let trace: Vec<&str> = recovered
        .alerts
        .iter()
        .filter(|alert| {
            alert
                .symbol
                .as_ref()
                .is_some_and(|symbol| symbol.as_str() == "EURUSD")
        })
        .map(|alert| alert.summary.as_str())
        .collect();
    let fault = trace
        .iter()
        .rposition(|summary| *summary == "source error (HttpStatus(401))")
        .expect("authentication failure observed");
    let recovery = trace
        .iter()
        .rposition(|summary| *summary == "resync completed")
        .expect("recovery completed");
    assert!(fault < recovery, "{trace:?}");

    stop_session(root, task, "VWD").await;
}

#[test]
fn rate_view_does_not_require_a_terminal_book_renderer() {
    let backend = TestBackend::new(80, 24);
    let _ = Terminal::new(backend).expect("test backend");
}
