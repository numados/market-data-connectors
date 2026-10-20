//! Notification-driven readiness barrier shared by the mock servers and sinks.

use std::future::Future;
use std::time::Duration;

use tokio::sync::Notify;

/// Waits until `check` returns `Some`, re-evaluating it after every
/// `notify_waiters` call on `notify`.
///
/// The `Notified` future is created *before* `check` runs. `notify_waiters`
/// wakes every future created before the call, so a notification that lands
/// between registration and the check is never lost: the check either sees the
/// new state or the already-registered future completes immediately.
///
/// A barrier that never opens fails the test after [`BARRIER_TIMEOUT`] instead of hanging it.
pub async fn wait_until<T>(notify: &Notify, mut check: impl FnMut() -> Option<T>) -> T {
    let wait = async {
        loop {
            let notified = notify.notified();
            if let Some(value) = check() {
                return value;
            }
            notified.await;
        }
    };
    within(BARRIER_TIMEOUT, "readiness barrier", wait).await
}

/// Upper bound for any single readiness barrier.
pub const BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

/// Awaits `fut`, panicking with `what` if it does not finish within `bound`.
///
/// For tests that must fail loudly instead of hanging when a barrier never opens.
pub async fn within<T>(bound: Duration, what: &str, fut: impl Future<Output = T>) -> T {
    match tokio::time::timeout(bound, fut).await {
        Ok(value) => value,
        Err(_) => panic!("timed out after {bound:?} waiting for {what}"),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Deterministic lost-notification regression: the event lands after the
    /// waiter registered but before its check observes it. The waiter must
    /// neither hang nor skip the event.
    #[tokio::test]
    async fn notification_between_registration_and_check_is_not_lost() {
        let notify = Notify::new();
        let state = AtomicBool::new(false);
        let checks = AtomicUsize::new(0);

        let value = within(
            Duration::from_secs(2),
            "wait_until to observe the event",
            wait_until(&notify, || {
                if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                    // First check runs after registration. The event fires now,
                    // but this check still reports "not ready" (it read the old
                    // state), so only the registered future can wake the waiter.
                    state.store(true, Ordering::SeqCst);
                    notify.notify_waiters();
                    None
                } else {
                    state.load(Ordering::SeqCst).then_some(42)
                }
            }),
        )
        .await;

        assert_eq!(value, 42);
        assert_eq!(checks.load(Ordering::SeqCst), 2, "re-checked exactly once");
    }

    /// An event that happened before the wait began is observed by the first check.
    #[tokio::test]
    async fn already_satisfied_state_returns_without_notification() {
        let notify = Notify::new();
        let value = within(
            Duration::from_secs(2),
            "immediate readiness",
            wait_until(&notify, || Some("ready")),
        )
        .await;
        assert_eq!(value, "ready");
    }

    /// Every one of several concurrent waiters is released by one broadcast.
    #[tokio::test]
    async fn notify_waiters_releases_all_waiters() {
        let notify = Arc::new(Notify::new());
        let flag = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (notify, flag) = (Arc::clone(&notify), Arc::clone(&flag));
            tasks.push(tokio::spawn(async move {
                wait_until(&notify, || flag.load(Ordering::SeqCst).then_some(())).await;
            }));
        }
        tokio::task::yield_now().await;
        flag.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        for task in tasks {
            within(Duration::from_secs(2), "waiter release", task)
                .await
                .unwrap();
        }
    }

    /// A notification whose state does not satisfy the check does not release the waiter.
    #[tokio::test]
    async fn unsatisfied_notification_keeps_waiting() {
        let notify = Notify::new();
        let polls = AtomicUsize::new(0);
        let waiter = wait_until(&notify, || {
            (polls.fetch_add(1, Ordering::SeqCst) >= 2).then_some(())
        });
        tokio::pin!(waiter);
        // First poll registers and fails the check.
        assert!(futures_poll_once(&mut waiter).await.is_none());
        notify.notify_waiters(); // check #2 still unsatisfied
        assert!(futures_poll_once(&mut waiter).await.is_none());
        notify.notify_waiters(); // check #3 satisfied
        within(Duration::from_secs(2), "release", waiter).await;
    }

    async fn futures_poll_once<F: Future + Unpin>(fut: &mut F) -> Option<F::Output> {
        std::future::poll_fn(|cx| match std::pin::Pin::new(&mut *fut).poll(cx) {
            std::task::Poll::Ready(v) => std::task::Poll::Ready(Some(v)),
            std::task::Poll::Pending => std::task::Poll::Ready(None),
        })
        .await
    }
}
