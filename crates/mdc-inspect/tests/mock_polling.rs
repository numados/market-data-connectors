//! Offline E2E coverage for the two polling adapters.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(all(feature = "frankfurter", feature = "vwd"))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use mdc_core::Symbol;
use mdc_inspect::config::SessionConfig;
use mdc_inspect::diagnostics::{DiagnosticsRing, DiagnosticsSnapshot, DiagnosticsSubscriber};
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus, UiState};
use mdc_inspect::provider::ProviderKind;
use mdc_inspect::provider::frankfurter::FrankfurterAdapter;
use mdc_inspect::provider::vwd::{EnvLookup, VwdAdapter};
use mdc_inspect::session::{SessionOutcome, run_session};
use mdc_inspect::view::draw_main;
use mdc_testkit::{MockRest, load_fixture};
use ratatui::{Terminal, backend::TestBackend};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, ResponseTemplate};

const RATES: &str = "crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json";
const VWD: &str = "crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json";
const ACCOUNT_PATH: &str = "synthetic-account-marker";
const AUTH_USERNAME: &str = "synthetic-user-marker";
const AUTH_PASSWORD: &str = "synthetic-password-marker";
const EXPECTED_AUTHORIZATION: &str =
    "Basic c3ludGhldGljLXVzZXItbWFya2VyOnN5bnRoZXRpYy1wYXNzd29yZC1tYXJrZXI=";

fn assert_no_credential_markers(label: &str, surfaces: &[(&str, String)]) {
    for (surface_name, surface) in surfaces {
        assert!(
            !surface.contains(ACCOUNT_PATH)
                && !surface.contains(AUTH_USERNAME)
                && !surface.contains(AUTH_PASSWORD)
                && !surface.contains(EXPECTED_AUTHORIZATION),
            "{label} {surface_name} exposed a credential marker"
        );
    }
}

static DIAGNOSTICS_RING: OnceLock<Arc<Mutex<DiagnosticsRing>>> = OnceLock::new();

fn diagnostics_ring() -> Arc<Mutex<DiagnosticsRing>> {
    DIAGNOSTICS_RING
        .get_or_init(|| {
            let ring = Arc::new(Mutex::new(DiagnosticsRing::new()));
            let subscriber = DiagnosticsSubscriber::from_ring(Arc::clone(&ring));
            tracing::subscriber::set_global_default(subscriber)
                .expect("mock polling diagnostics subscriber must install once");
            ring
        })
        .clone()
}

fn diagnostics_snapshot(ring: &Arc<Mutex<DiagnosticsRing>>) -> DiagnosticsSnapshot {
    ring.lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .snapshot()
}

fn diagnostics_surface(ring: &Arc<Mutex<DiagnosticsRing>>) -> String {
    format!("{:?}", diagnostics_snapshot(ring))
}

fn assert_actual_vwd_record(snapshot: &DiagnosticsSnapshot) {
    assert!(
        snapshot.records.iter().any(|record| {
            record.target == "mdc_providers::vwd::connection"
                && record
                    .fields
                    .iter()
                    .any(|(name, value)| name == "venue" && value == "vwd")
        }),
        "actual VWD provider diagnostic was not captured: {snapshot:?}"
    );
}

fn rendered_main_surface(snapshot: &DisplaySnapshot, ring: &Arc<Mutex<DiagnosticsRing>>) -> String {
    let diagnostics = diagnostics_snapshot(ring);
    let mut terminal = Terminal::new(TestBackend::new(120, 40)).expect("test backend");
    terminal
        .draw(|frame| draw_main(frame, snapshot, &UiState::new(), &diagnostics))
        .expect("render main view");
    let buffer = terminal.backend().buffer();
    let mut text = String::new();
    let mut last_y = 0;
    for (index, cell) in buffer.content.iter().enumerate() {
        let y = index as u16 / buffer.area.width;
        if y != last_y {
            text.push('\n');
            last_y = y;
        }
        text.push_str(cell.symbol());
    }
    text
}

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

async fn stop_session(
    root: CancellationToken,
    task: JoinHandle<SessionOutcome>,
    provider: &str,
) -> SessionOutcome {
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
    outcome
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

    let _ = stop_session(root, task, "Frankfurter").await;
}

#[tokio::test(flavor = "current_thread")]
async fn vwd_uses_exact_auth_path_and_redacts_all_app_surfaces() {
    let diagnostics = diagnostics_ring();
    let mock = MockRest::start().await;
    let fixture = load_fixture(VWD).expect("fixture");
    let failing = Arc::new(AtomicBool::new(false));
    let response_state = Arc::clone(&failing);
    let expected_path = format!("/{ACCOUNT_PATH}/JSON/QUOTE/SNAPQUOTE/EURUSD.XRATE.SPOT~X");
    Mock::given(method("GET"))
        .and(path(expected_path))
        .and(header("authorization", EXPECTED_AUTHORIZATION))
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
        ("MDC_INSPECT_VWD_ACCOUNT_PATH".into(), ACCOUNT_PATH.into()),
        ("MDC_INSPECT_VWD_USERNAME".into(), AUTH_USERNAME.into()),
        ("MDC_INSPECT_VWD_PASSWORD".into(), AUTH_PASSWORD.into()),
    ]));
    let config = SessionConfig::new(ProviderKind::Vwd, &["EURUSD".into()]).expect("config");
    let adapter = Arc::new(VwdAdapter::with_env(env, mock.url().expect("url")));
    let adapter_debug = format!("{adapter:?}");
    let config_debug = format!("{config:?}");

    let mut invalid_config = config.clone();
    invalid_config.base = Some(Symbol::from("invalid"));
    let (error_tx, error_rx) = display("VWD/Infront");
    let invalid_outcome = run_session(
        adapter.clone(),
        invalid_config,
        ProviderKind::Vwd.session_bounds(),
        4096,
        CancellationToken::new(),
        error_tx,
    )
    .await;
    let invalid_snapshot = { error_rx.borrow().clone() };
    assert_eq!(invalid_outcome.status, DisplayStatus::Failed);
    assert_no_credential_markers(
        "validation error",
        &[
            ("adapter debug", adapter_debug.clone()),
            ("config debug", config_debug.clone()),
            ("display", format!("{invalid_snapshot:?}")),
            ("diagnostics", diagnostics_surface(&diagnostics)),
            (
                "rendered main",
                rendered_main_surface(&invalid_snapshot, &diagnostics),
            ),
            ("outcome", format!("{invalid_outcome:?}")),
        ],
    );

    let (tx, mut rx) = display("VWD/Infront");
    let root = CancellationToken::new();
    let task = tokio::spawn(run_session(
        adapter.clone(),
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
    assert_no_credential_markers(
        "success",
        &[
            ("adapter debug", adapter_debug.clone()),
            ("config debug", config_debug.clone()),
            ("display", format!("{initial:?}")),
            ("diagnostics", diagnostics_surface(&diagnostics)),
            (
                "rendered main",
                rendered_main_surface(&initial, &diagnostics),
            ),
        ],
    );

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
    assert_no_credential_markers(
        "401",
        &[
            ("adapter debug", adapter_debug.clone()),
            ("config debug", config_debug.clone()),
            ("display", format!("{failed:?}")),
            ("diagnostics", diagnostics_surface(&diagnostics)),
            (
                "rendered main",
                rendered_main_surface(&failed, &diagnostics),
            ),
            ("outcome failure", format!("{:?}", failed.failure)),
        ],
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
    assert_no_credential_markers(
        "recovery",
        &[
            ("adapter debug", adapter_debug.clone()),
            ("config debug", config_debug.clone()),
            ("display", format!("{recovered:?}")),
            ("diagnostics", diagnostics_surface(&diagnostics)),
            (
                "rendered main",
                rendered_main_surface(&recovered, &diagnostics),
            ),
        ],
    );

    let stop_outcome = stop_session(root, task, "VWD").await;
    let stopped = { rx.borrow().clone() };
    let actual_diagnostics = diagnostics_snapshot(&diagnostics);
    assert_actual_vwd_record(&actual_diagnostics);
    assert_eq!(stopped.session, DisplayStatus::Stopped);
    assert!(stopped.symbols[0].quote.as_ref().unwrap().historical);
    assert_no_credential_markers(
        "stop",
        &[
            ("adapter debug", adapter_debug),
            ("config debug", config_debug),
            ("display", format!("{stopped:?}")),
            ("diagnostics", format!("{actual_diagnostics:?}")),
            (
                "rendered main",
                rendered_main_surface(&stopped, &diagnostics),
            ),
            ("outcome", format!("{stop_outcome:?}")),
        ],
    );
}

#[test]
fn reference_rate_view_has_no_book_fields_before_the_first_poll() {
    let (_tx, rx) = display(ProviderKind::Frankfurter.label());
    let mut snapshot = rx.borrow().as_ref().clone();
    snapshot.presentation = ProviderKind::Frankfurter.presentation();
    let (diagnostics, _subscriber) = DiagnosticsSubscriber::new();
    let rendered = rendered_main_surface(&snapshot, &diagnostics);
    assert!(rendered.contains("Reference rate"));
    assert!(!rendered.contains("Level 2"));
    assert!(!rendered.contains("Top of book"));
    assert!(!rendered.contains("max depth"));
    assert!(!rendered.contains("unbounded"));
}
