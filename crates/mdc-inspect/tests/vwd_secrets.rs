//! VWD environment-only credential and redaction checks.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(feature = "vwd")]

use std::collections::HashMap;

use mdc_inspect::config::SessionConfig;
use mdc_inspect::diagnostics::DiagnosticsSubscriber;
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus};
use mdc_inspect::provider::vwd::{EnvLookup, VwdAdapter};
use mdc_inspect::provider::{ProviderKind, SessionAdapter};
use mdc_sinks::loss_intolerant::loss_intolerant_channel;
use tokio_util::sync::CancellationToken;
use url::Url;

const ACCOUNT_MARKER: &str = "synthetic-account-marker";
const USER_MARKER: &str = "synthetic-user-marker";
const PASSWORD_MARKER: &str = "synthetic-password-marker";

fn assert_no_markers(surface: &str) {
    assert!(
        !surface.contains(ACCOUNT_MARKER),
        "account marker was exposed"
    );
    assert!(
        !surface.contains(USER_MARKER),
        "username marker was exposed"
    );
    assert!(
        !surface.contains(PASSWORD_MARKER),
        "password marker was exposed"
    );
}

fn diagnostics_surface() -> String {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(
            provider = "VWD/Infront",
            account_path = ACCOUNT_MARKER,
            username = USER_MARKER,
            password = PASSWORD_MARKER,
            "credential redaction"
        );
    });
    let snapshot = ring
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .snapshot();
    format!("{snapshot:?}")
}

#[derive(Clone)]
struct Env(HashMap<String, String>);
impl EnvLookup for Env {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

fn config() -> SessionConfig {
    SessionConfig::new(ProviderKind::Vwd, &["EURUSD".to_string()]).expect("valid pair")
}

#[test]
fn valid_credentials_are_absent_from_existing_formatted_surfaces() {
    let adapter = VwdAdapter::with_env(
        Env(HashMap::from([
            ("MDC_INSPECT_VWD_ACCOUNT_PATH".into(), ACCOUNT_MARKER.into()),
            ("MDC_INSPECT_VWD_USERNAME".into(), USER_MARKER.into()),
            ("MDC_INSPECT_VWD_PASSWORD".into(), PASSWORD_MARKER.into()),
        ])),
        Url::parse("http://127.0.0.1:1").expect("url"),
    );
    let config = config();
    adapter.validate(&config).expect("valid credentials");
    let display = DisplaySnapshot {
        provider: "VWD/Infront",
        presentation: Default::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    };
    for surface in [
        format!("{adapter:?}"),
        format!("{config:?}"),
        format!("{display:?}"),
        diagnostics_surface(),
    ] {
        assert_no_markers(&surface);
    }
}

#[test]
fn each_missing_variable_is_named_without_exposing_values() {
    for missing in [
        "MDC_INSPECT_VWD_ACCOUNT_PATH",
        "MDC_INSPECT_VWD_USERNAME",
        "MDC_INSPECT_VWD_PASSWORD",
    ] {
        let values = HashMap::from([
            (
                "MDC_INSPECT_VWD_ACCOUNT_PATH".into(),
                "account-secret".into(),
            ),
            ("MDC_INSPECT_VWD_USERNAME".into(), "username-secret".into()),
            ("MDC_INSPECT_VWD_PASSWORD".into(), "password-secret".into()),
        ]);
        let mut values = values;
        values.remove(missing);
        let adapter =
            VwdAdapter::with_env(Env(values), Url::parse("http://127.0.0.1:1").expect("url"));
        let error = adapter
            .validate(&config())
            .expect_err("missing credential")
            .to_string();
        assert_eq!(error, format!("invalid configuration: {missing}"));
        assert!(!error.contains("secret"));
    }
}

#[test]
fn empty_values_are_rejected_before_network_start() {
    let env = Env(HashMap::from([
        ("MDC_INSPECT_VWD_ACCOUNT_PATH".into(), String::new()),
        ("MDC_INSPECT_VWD_USERNAME".into(), "user".into()),
        ("MDC_INSPECT_VWD_PASSWORD".into(), "pass".into()),
    ]));
    let adapter = VwdAdapter::with_env(env, Url::parse("http://127.0.0.1:1").expect("url"));
    let error = adapter
        .validate(&config())
        .expect_err("empty account path")
        .to_string();
    assert!(error.contains("MDC_INSPECT_VWD_ACCOUNT_PATH"));
    assert!(!error.contains("user") && !error.contains("pass"));
}

#[test]
fn missing_credentials_do_not_construct_a_provider_run() {
    let adapter = VwdAdapter::with_env(
        Env(HashMap::new()),
        Url::parse("http://127.0.0.1:1").expect("url"),
    );
    let token = CancellationToken::new();
    let (sender, _stream) = loss_intolerant_channel(4, 1, token.clone());
    let error = match adapter.start(&config(), sender, token) {
        Ok(_) => panic!("missing credentials must fail before starting"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("MDC_INSPECT_VWD_ACCOUNT_PATH"));
}
