#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::{AdapterFactory, App, run_with_input};
use crate::config::SessionConfig;
use crate::diagnostics::DiagnosticsRing;
use crate::error::Error;
use crate::provider::{ProviderKind, ProviderRun, SessionAdapter, SessionBounds};
use crate::terminal::{TerminalGuard, TerminalLifecycle};
use tokio_util::sync::CancellationToken;

type CallLog = Arc<Mutex<Vec<(&'static str, bool)>>>;

struct FakeTerminalLifecycle {
    calls: CallLog,
    provider_dropped: Arc<AtomicBool>,
}

impl FakeTerminalLifecycle {
    fn new(provider_dropped: Arc<AtomicBool>) -> (CallLog, Self) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Self {
            calls: Arc::clone(&calls),
            provider_dropped,
        };
        (calls, lifecycle)
    }

    fn record(&mut self, step: &'static str) -> Result<(), Error> {
        self.calls
            .lock()
            .unwrap()
            .push((step, self.provider_dropped.load(Ordering::SeqCst)));
        Ok(())
    }

    fn record_restore(&mut self, step: &'static str) -> Result<(), Error> {
        assert!(
            self.provider_dropped.load(Ordering::SeqCst),
            "terminal restored before the provider session was reaped"
        );
        self.record(step)
    }
}

impl TerminalLifecycle for FakeTerminalLifecycle {
    fn enable_raw(&mut self) -> Result<(), Error> {
        self.record("enable_raw")
    }

    fn disable_raw(&mut self) -> Result<(), Error> {
        self.record_restore("disable_raw")
    }

    fn enter_alternate(&mut self) -> Result<(), Error> {
        self.record("enter_alt")
    }

    fn leave_alternate(&mut self) -> Result<(), Error> {
        self.record_restore("leave_alt")
    }

    fn hide_cursor(&mut self) -> Result<(), Error> {
        self.record("hide_cursor")
    }

    fn show_cursor(&mut self) -> Result<(), Error> {
        self.record_restore("show_cursor")
    }
}

struct ProviderProbe(Arc<AtomicBool>);

impl Drop for ProviderProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct SleepingAdapter {
    provider_dropped: Arc<AtomicBool>,
}

impl SessionAdapter for SleepingAdapter {
    fn bounds(&self) -> SessionBounds {
        SessionBounds {
            max_symbols: 25,
            projector_depth: 100,
            affected_connection: 1,
        }
    }

    fn start(
        &self,
        _config: &SessionConfig,
        sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
        _session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let probe = ProviderProbe(Arc::clone(&self.provider_dropped));
        Ok(ProviderRun::Future(Box::pin(async move {
            let _probe = probe;
            let _sink = sink;
            futures_util::future::pending::<Result<(), mdc_core::MdcError>>().await
        })))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn render_error_restores_terminal_after_provider_reaping() {
    let provider_dropped = Arc::new(AtomicBool::new(false));
    let factory: Box<AdapterFactory> = {
        let provider_dropped = Arc::clone(&provider_dropped);
        Box::new(move |_| {
            Ok(Arc::new(SleepingAdapter {
                provider_dropped: Arc::clone(&provider_dropped),
            }))
        })
    };
    let (calls, lifecycle) = FakeTerminalLifecycle::new(Arc::clone(&provider_dropped));
    let guard = TerminalGuard::activate(Box::new(lifecycle)).unwrap();

    let result = guard
        .run(run_with_input(
            App::new(Arc::new(Mutex::new(DiagnosticsRing::new()))),
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap(),
            Some(SessionConfig::new(ProviderKind::Test, &["BTCUSDT".to_string()]).unwrap()),
            &factory,
            futures_util::stream::empty(),
            |_terminal, _app| Err(io::Error::other("render failed")),
        ))
        .await;

    assert!(
        matches!(result, Err(Error::TerminalIo(error)) if error.to_string() == "render failed")
    );
    assert!(
        provider_dropped.load(Ordering::SeqCst),
        "session was not reaped"
    );
    let calls = calls.lock().unwrap().clone();
    assert_eq!(
        calls.iter().map(|(step, _)| *step).collect::<Vec<_>>(),
        vec![
            "enable_raw",
            "enter_alt",
            "hide_cursor",
            "show_cursor",
            "leave_alt",
            "disable_raw",
        ]
    );
    assert!(
        calls
            .iter()
            .skip(3)
            .all(|(_, provider_dropped)| *provider_dropped)
    );
}
