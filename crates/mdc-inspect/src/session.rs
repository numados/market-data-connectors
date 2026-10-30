//! Session lifecycle: loss-intolerant channel creation, connector/reducer
//! concurrency, cancellation, and bounded join (PRD 31 §§3.4, 3.7, 3.10).
//!
//! One owned session drives the provider run future and continuously drains
//! the channel into the ordered [`Reducer`]. Display snapshots are coalesced
//! onto a latest-value boundary at most once per render cadence, with one
//! final publish before the session ends so terminal states are never lost.

use std::sync::Arc;

use futures_util::StreamExt;
use mdc_sinks::loss_intolerant::loss_intolerant_channel;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::{RENDER_CADENCE, SHUTDOWN_BOUND, SessionConfig};
use crate::display::{DisplaySnapshot, DisplayStatus};
use crate::provider::{AbortOnDropHandle, ProviderRun, SessionAdapter, SessionBounds};
use crate::reducer::Reducer;

/// Terminal outcome of a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutcome {
    /// Terminal session status: `Stopped` or `Failed`.
    pub status: DisplayStatus,
    /// Sanitized failure cause when the session failed.
    pub failure: Option<String>,
}

/// Why the session ended; decides the final reducer transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    /// Adapter rejected the start before any provider I/O.
    StartFailed,
    /// The channel delivered its reserved terminal delivery fault.
    DeliveryFault,
    /// The provider future returned an error.
    ConnectorError,
    /// The provider task panicked or was cancelled unexpectedly.
    TaskFailed,
    /// The stream ended; operator stop decides stopped vs failed.
    StreamEof,
    /// The bounded shutdown deadline passed.
    ShutdownDeadline,
}

/// Runs one provider session to its terminal outcome.
///
/// `root` is the application-root token; the session creates a child from it
/// so operator stop, channel overflow, and connector shutdown share one
/// lineage. `capacity` is the loss-intolerant channel capacity (the
/// application passes [`crate::config::CHANNEL_CAPACITY`]; tests shrink it).
pub async fn run_session(
    adapter: Arc<dyn SessionAdapter>,
    config: SessionConfig,
    bounds: SessionBounds,
    capacity: usize,
    root: CancellationToken,
    display: watch::Sender<Arc<DisplaySnapshot>>,
) -> SessionOutcome {
    run_session_controlled(
        adapter,
        config,
        bounds,
        capacity,
        root,
        CancellationToken::new(),
        display,
    )
    .await
}

/// Runs a session with a separate force-stop token used by the application
/// control path. The public [`run_session`] contract remains suitable for
/// integration callers that only need cooperative cancellation.
pub(crate) async fn run_session_controlled(
    adapter: Arc<dyn SessionAdapter>,
    config: SessionConfig,
    bounds: SessionBounds,
    capacity: usize,
    root: CancellationToken,
    force_stop: CancellationToken,
    display: watch::Sender<Arc<DisplaySnapshot>>,
) -> SessionOutcome {
    let session_token = root.child_token();
    let (sender, mut stream) =
        loss_intolerant_channel(capacity, bounds.affected_connection, session_token.clone());

    let mut reducer = Reducer::new(&config, bounds);
    let _ = display.send(Arc::new(reducer.snapshot()));

    let (mut provider_task, _ownership) =
        match adapter.start(&config, sender, session_token.clone()) {
            Ok(ProviderRun::Future(future)) => (AbortOnDropHandle::new(tokio::spawn(future)), None),
            Ok(ProviderRun::Spawned { task, ownership }) => (task, Some(ownership)),
            Err(error) => {
                reducer.apply_session_failure(error.to_string());
                return finish(reducer, display, Terminal::StartFailed);
            }
        };

    let mut dirty = false;
    let mut last_publish = Instant::now();
    let mut stop_requested = false;
    let mut force_requested = false;
    let mut provider_done = false;
    let mut terminal: Option<Terminal> = None;

    // Phase 1 — running: select over cancellation, stream items, provider
    // completion, and the publish ticker. Deferred publishes need a trigger:
    // without the ticker arm a dirty snapshot would sit unpublished until
    // the next event (requirement 52's cadence bound).
    let mut publish_ticker = tokio::time::interval(RENDER_CADENCE);
    publish_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = force_stop.cancelled(), if !force_requested => {
                stop_requested = true;
                force_requested = true;
                break;
            }
            _ = session_token.cancelled() => {
                // Operator stop, channel overflow, or root quit. Phase 2
                // drains the accepted prefix before anything else.
                stop_requested = true;
                break;
            }
            joined = &mut provider_task, if !provider_done => {
                record_provider_join(joined, &mut reducer, &mut terminal);
                provider_done = true;
                break;
            }
            item = stream.next() => match item {
                Some(Ok(event)) => {
                    reducer.apply_event(&event);
                    dirty = true;
                }
                Some(Err(fault)) => {
                    reducer.apply_fault(&fault);
                    terminal = Some(Terminal::DeliveryFault);
                    break;
                }
                None => {
                    if terminal.is_none() {
                        terminal = Some(Terminal::StreamEof);
                    }
                    break;
                }
            },
            _ = publish_ticker.tick() => {
                // Fall through: the cadence publish happens below.
            }
        }
        publish_if_dirty(&mut dirty, &mut last_publish, &reducer, &display);
    }

    // Phase 2 — draining: consume the accepted FIFO prefix until the stream
    // ends, bounded by the shutdown deadline. EOF is never a precondition:
    // a provider that ignores cancellation hits the deadline and is reaped.
    let deadline = Instant::now() + SHUTDOWN_BOUND;
    loop {
        if force_requested || terminal.is_some() {
            break;
        }
        tokio::select! {
            biased;
            _ = force_stop.cancelled() => {
                stop_requested = true;
                break;
            }
            item = stream.next() => match item {
                Some(Ok(event)) => {
                    reducer.apply_event(&event);
                    dirty = true;
                }
                Some(Err(fault)) => {
                    reducer.apply_fault(&fault);
                    terminal = Some(Terminal::DeliveryFault);
                }
                None => break,
            },
            joined = &mut provider_task, if !provider_done => {
                record_provider_join(joined, &mut reducer, &mut terminal);
                provider_done = true;
                // Continue through the stream arm so an open sender is
                // bounded by the shutdown deadline instead of being treated
                // as EOF merely because the queue is currently empty.
            }
            _ = tokio::time::sleep_until(deadline) => {
                if terminal.is_none() {
                    terminal = Some(Terminal::ShutdownDeadline);
                }
                break;
            }
        }
        publish_if_dirty(&mut dirty, &mut last_publish, &reducer, &display);
    }

    // Phase 3 — bounded teardown: abort an unfinished provider, await it
    // within the shutdown bound, then drain remaining items without blocking.
    if !provider_done {
        provider_task.abort();
        match timeout(SHUTDOWN_BOUND, &mut provider_task).await {
            Ok(joined) => record_provider_join(joined, &mut reducer, &mut terminal),
            Err(_) => {
                // Missed even abort+await: force-reap (requirement 114).
                provider_task.abort();
                let _ = (&mut provider_task).await;
                if terminal.is_none() {
                    reducer.apply_session_failure("session missed the shutdown bound".to_string());
                    terminal = Some(Terminal::ShutdownDeadline);
                }
            }
        }
    }
    while let Ok(Some(item)) = stream.try_recv() {
        match item {
            Ok(event) => reducer.apply_event(&event),
            Err(fault) => reducer.apply_fault(&fault),
        }
    }

    match terminal {
        Some(
            Terminal::StartFailed
            | Terminal::DeliveryFault
            | Terminal::ConnectorError
            | Terminal::TaskFailed,
        ) => {}
        Some(Terminal::StreamEof) | None => {
            reducer.apply_eof(stop_requested);
        }
        Some(Terminal::ShutdownDeadline) => {
            if stop_requested {
                reducer.apply_eof(true);
            } else {
                reducer.apply_session_failure("session shutdown deadline exceeded".to_string());
            }
        }
    }

    finish(reducer, display, terminal.unwrap_or(Terminal::StreamEof))
}

fn record_provider_join(
    joined: Result<Result<(), mdc_core::MdcError>, tokio::task::JoinError>,
    reducer: &mut Reducer,
    terminal: &mut Option<Terminal>,
) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            reducer.apply_session_failure(error.to_string());
            if terminal.is_none() {
                *terminal = Some(Terminal::ConnectorError);
            }
        }
        Err(join_error) if join_error.is_panic() => {
            reducer.apply_session_failure("provider task panicked".to_string());
            if terminal.is_none() {
                *terminal = Some(Terminal::TaskFailed);
            }
        }
        Err(_) => {}
    }
}

fn publish_if_dirty(
    dirty: &mut bool,
    last_publish: &mut Instant,
    reducer: &Reducer,
    display: &watch::Sender<Arc<DisplaySnapshot>>,
) {
    if *dirty && last_publish.elapsed() >= RENDER_CADENCE {
        let _ = display.send(Arc::new(reducer.snapshot()));
        *last_publish = Instant::now();
        *dirty = false;
    }
}

fn finish(
    mut reducer: Reducer,
    display: watch::Sender<Arc<DisplaySnapshot>>,
    terminal: Terminal,
) -> SessionOutcome {
    let _ = &mut reducer;
    let _ = terminal;
    // Final publish so terminal failure states are never lost (requirement 52).
    let _ = display.send(Arc::new(reducer.snapshot()));
    let snapshot = display.borrow().clone();
    SessionOutcome {
        status: snapshot.session,
        failure: snapshot.failure.clone(),
    }
}

#[cfg(test)]
mod tests;
