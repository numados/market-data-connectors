//! Terminal lifecycle tests: partial-init cleanup, normal/error cleanup,
//! panic unwind cleanup, and idempotence (requirement 142).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use super::*;

/// Shared call log so tests can inspect a fake owned by the guard.
type CallLog = Arc<Mutex<Vec<&'static str>>>;

/// Records lifecycle calls; fails a step when configured.
struct FakeLifecycle {
    calls: CallLog,
    fail_at: Option<&'static str>,
}

impl FakeLifecycle {
    fn new(fail_at: Option<&'static str>) -> (CallLog, Self) {
        let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
        let fake = Self {
            calls: Arc::clone(&calls),
            fail_at,
        };
        (calls, fake)
    }

    fn record(&mut self, step: &'static str) -> Result<(), Error> {
        self.calls.lock().unwrap().push(step);
        if self.fail_at == Some(step) {
            return Err(Error::Terminal(format!("simulated failure at {step}")));
        }
        Ok(())
    }
}

impl TerminalLifecycle for FakeLifecycle {
    fn enable_raw(&mut self) -> Result<(), Error> {
        self.record("enable_raw")
    }
    fn disable_raw(&mut self) -> Result<(), Error> {
        self.record("disable_raw")
    }
    fn enter_alternate(&mut self) -> Result<(), Error> {
        self.record("enter_alt")
    }
    fn leave_alternate(&mut self) -> Result<(), Error> {
        self.record("leave_alt")
    }
    fn hide_cursor(&mut self) -> Result<(), Error> {
        self.record("hide_cursor")
    }
    fn show_cursor(&mut self) -> Result<(), Error> {
        self.record("show_cursor")
    }
}

fn calls_of(log: &CallLog) -> Vec<&'static str> {
    log.lock().unwrap().clone()
}

#[test]
fn full_activation_and_normal_restore_reverses_steps() {
    let (log, fake) = FakeLifecycle::new(None);
    let mut guard = TerminalGuard::activate(Box::new(fake)).unwrap();
    assert!(!guard.is_restored());
    guard.restore();
    assert_eq!(
        calls_of(&log),
        vec![
            "enable_raw",
            "enter_alt",
            "hide_cursor",
            "show_cursor",
            "leave_alt",
            "disable_raw"
        ]
    );
}

#[test]
fn partial_activation_failure_restores_completed_steps_only() {
    let (log, fake) = FakeLifecycle::new(Some("hide_cursor"));
    let result = TerminalGuard::activate(Box::new(fake));
    assert!(result.is_err(), "hide_cursor failure must surface");
    // Raw mode and alternate screen were completed and must be restored;
    // the failing step itself was attempted (and logged) but never completed,
    // so no cursor restore happens.
    assert_eq!(
        calls_of(&log),
        vec![
            "enable_raw",
            "enter_alt",
            "hide_cursor",
            "leave_alt",
            "disable_raw"
        ]
    );
}

#[test]
fn drop_restores_and_is_idempotent() {
    let (log, fake) = FakeLifecycle::new(None);
    let mut guard = TerminalGuard::activate(Box::new(fake)).unwrap();
    guard.restore();
    guard.restore(); // Second explicit restore is a no-op.
    drop(guard); // Drop after restore adds nothing.
    assert_eq!(
        calls_of(&log),
        vec![
            "enable_raw",
            "enter_alt",
            "hide_cursor",
            "show_cursor",
            "leave_alt",
            "disable_raw"
        ]
    );
}

#[test]
fn drop_without_explicit_restore_still_restores() {
    let (log, fake) = FakeLifecycle::new(None);
    let guard = TerminalGuard::activate(Box::new(fake)).unwrap();
    drop(guard); // Panic-unwind path: Drop alone must restore.
    assert_eq!(
        calls_of(&log),
        vec![
            "enable_raw",
            "enter_alt",
            "hide_cursor",
            "show_cursor",
            "leave_alt",
            "disable_raw"
        ]
    );
}

#[test]
fn panic_while_guard_is_held_restores_via_unwind() {
    let (log, fake) = FakeLifecycle::new(None);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = TerminalGuard::activate(Box::new(fake)).unwrap();
        panic!("simulated panic inside the TUI");
    }));
    assert!(outcome.is_err());
    assert!(calls_of(&log).contains(&"disable_raw"));
    assert!(calls_of(&log).contains(&"leave_alt"));
    assert!(calls_of(&log).contains(&"show_cursor"));
}
