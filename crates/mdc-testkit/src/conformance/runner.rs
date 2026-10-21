//! End-to-end scenario runner: drives a production connector through the standard
//! conformance scenarios against the mock servers.
//!
//! The runner is venue-agnostic. Everything wire-specific lives in a [`VenueScript`]: the
//! frames that synchronize a book, one live data frame, and the optional fault frames. The
//! runner replays them through [`MockWsServer`] and judges the connector only by the events its
//! sink recorded (see [`validate`](super::validate)).

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, MdcError, StreamingSink};

use super::{ConformanceReport, validate};
use crate::rest::MockRest;
use crate::sink::RecordedEvent;
use crate::ws::MockWsServer;

/// Boxed future used by the scenario hooks.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A recording sink the scenario runner can read back after a scenario.
pub trait ConformanceSink: StreamingSink + Clone + Send + 'static {
    /// Returns every event the sink recorded so far.
    fn recorded_events(&self) -> Vec<RecordedEvent>;
}

impl ConformanceSink for crate::sink::RecordingSink {
    fn recorded_events(&self) -> Vec<RecordedEvent> {
        self.events()
    }
}

impl ConformanceSink for crate::sink::FaultInjectingSink {
    fn recorded_events(&self) -> Vec<RecordedEvent> {
        self.events()
    }
}

/// Standard end-to-end scenarios executed by [`run`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// Connect, then shut down on request; the connector must exit cleanly.
    StartupAndShutdown,
    /// Shut down on request while synchronized and publishing market data.
    Cancel,
    /// The server closes the socket after live data; the connector must alert, reconnect and
    /// resynchronize (`ResyncStarted`, snapshot book, `ResyncCompleted`) without publishing
    /// data in between.
    ServerCloseAndRecover,
    /// A sequence gap after live data must raise `GapDetected` and recover through a snapshot.
    Gap,
    /// A checksum mismatch after live data must raise `ChecksumMismatch` and recover.
    BadChecksum,
    /// No frames after live data: the watchdog must alert, reconnect and resynchronize.
    Silence,
    /// The REST snapshot endpoint answers 429 first; the connector must back off and recover.
    RateLimited,
    /// The first REST snapshot is stale against the buffered deltas; the connector must refetch.
    StaleSnapshot,
    /// Pause drops the connection without publishing; resume resynchronizes.
    PauseResume,
    /// The sink fails on its first callback; the connector must halt with an error.
    SinkFault,
}

impl Scenario {
    /// All standard scenarios, in execution order.
    pub const ALL: [Scenario; 10] = [
        Scenario::StartupAndShutdown,
        Scenario::Cancel,
        Scenario::ServerCloseAndRecover,
        Scenario::Gap,
        Scenario::BadChecksum,
        Scenario::Silence,
        Scenario::RateLimited,
        Scenario::StaleSnapshot,
        Scenario::PauseResume,
        Scenario::SinkFault,
    ];
}

/// A frame the mock server sends to the connector.
#[derive(Clone)]
pub enum Frame {
    /// A text frame.
    Text(String),
    /// A binary frame.
    Binary(Vec<u8>),
    /// A frame built from the client messages the connector sent since the previous computed
    /// frame was resolved (for venues whose snapshot answers echo a request id). The runner
    /// retries until it yields `Some`.
    Computed(ComputedFrame),
}

/// Resolver behind [`Frame::Computed`].
#[derive(Clone)]
pub struct ComputedFrame(Arc<Resolver>);

type Resolver = dyn Fn(&[String]) -> Option<Frame> + Send + Sync;

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Text(text) => f.debug_tuple("Text").field(text).finish(),
            Frame::Binary(bytes) => f.debug_tuple("Binary").field(&bytes.len()).finish(),
            Frame::Computed(_) => f.write_str("Computed(..)"),
        }
    }
}

impl Frame {
    /// Creates a text frame.
    pub fn text(payload: impl Into<String>) -> Self {
        Frame::Text(payload.into())
    }

    /// Creates a binary frame.
    pub fn binary(payload: impl Into<Vec<u8>>) -> Self {
        Frame::Binary(payload.into())
    }

    /// Creates a frame resolved from the client messages received since the previous computed
    /// frame; `resolve` returns `None` until the message it answers has arrived.
    pub fn computed(resolve: impl Fn(&[String]) -> Option<Frame> + Send + Sync + 'static) -> Self {
        Frame::Computed(ComputedFrame(Arc::new(resolve)))
    }

    fn broadcast(&self, ws: &MockWsServer) -> Result<usize, String> {
        match self {
            Frame::Text(text) => ws.broadcast_text(text.clone()),
            Frame::Binary(bytes) => ws.broadcast_binary(bytes.clone()),
            Frame::Computed(_) => return Err("computed frame was not resolved".to_string()),
        }
        .map_err(|e| format!("no connected client received the frame: {e}"))
    }
}

/// Frames that inject a fault after live data, plus the frames that recover from it.
#[derive(Debug, Clone, Default)]
pub struct Fault {
    /// Frames that make the connector detect the fault.
    pub frames: Vec<Frame>,
    /// Frames sent once the connector restarted its resynchronization. `None` replays
    /// [`VenueScript::snapshot`]; `Some(vec![])` is for venues whose recovery needs no frames
    /// (the REST snapshot bridges the frame that revealed the fault).
    pub recovery: Option<Vec<Frame>>,
}

/// Configures the REST mock for one scenario. Called before the connector starts.
type RestSetup = Box<dyn for<'a> Fn(&'a MockRest, Scenario) -> BoxFuture<'a, ()> + Send + Sync>;

/// Venue-specific wire frames the generic scenarios replay.
#[derive(Default)]
pub struct VenueScript {
    /// Frames that bring the book in sync, sent once `ResyncStarted` was observed.
    pub snapshot: Vec<Frame>,
    /// Frames, valid right after `snapshot`, that must publish market data.
    pub data: Vec<Frame>,
    /// Sequence gap fault; `None` when the venue has no sequence integrity.
    pub gap: Option<Fault>,
    /// Checksum fault; `None` when the venue has no checksum integrity.
    pub bad_checksum: Option<Fault>,
    rest: Option<RestSetup>,
    rest_faults: Vec<Scenario>,
    skipped: Vec<(Scenario, &'static str)>,
}

impl VenueScript {
    /// Creates a script from the synchronizing frames and one live data frame.
    pub fn new(snapshot: Vec<Frame>, data: Vec<Frame>) -> Self {
        Self {
            snapshot,
            data,
            ..Self::default()
        }
    }

    /// Adds a sequence-gap fault.
    pub fn with_gap(mut self, fault: Fault) -> Self {
        self.gap = Some(fault);
        self
    }

    /// Adds a checksum fault.
    pub fn with_bad_checksum(mut self, fault: Fault) -> Self {
        self.bad_checksum = Some(fault);
        self
    }

    /// Declares a REST-snapshot venue. `setup` mounts the REST responses for each scenario;
    /// `faults` lists the REST fault scenarios ([`Scenario::RateLimited`],
    /// [`Scenario::StaleSnapshot`]) that `setup` knows how to stage.
    pub fn with_rest<F>(mut self, faults: &[Scenario], setup: F) -> Self
    where
        F: for<'a> Fn(&'a MockRest, Scenario) -> BoxFuture<'a, ()> + Send + Sync + 'static,
    {
        self.rest = Some(Box::new(setup));
        self.rest_faults = faults.to_vec();
        self
    }

    /// Declares a scenario the venue cannot exhibit, with the reason it is skipped.
    pub fn skip(mut self, scenario: Scenario, reason: &'static str) -> Self {
        self.skipped.push((scenario, reason));
        self
    }

    fn unsupported(&self, scenario: Scenario) -> Option<&'static str> {
        if let Some((_, reason)) = self.skipped.iter().find(|(s, _)| *s == scenario) {
            return Some(reason);
        }
        match scenario {
            Scenario::Gap if self.gap.is_none() => Some("script has no gap fault"),
            Scenario::BadChecksum if self.bad_checksum.is_none() => {
                Some("script has no checksum fault")
            }
            Scenario::RateLimited | Scenario::StaleSnapshot
                if !self.rest_faults.contains(&scenario) =>
            {
                Some("script stages no REST fault for this scenario")
            }
            _ => None,
        }
    }
}

/// What a connector builder receives for one scenario.
pub struct Harness<'a> {
    /// The scenario about to run; builders shorten the watchdog for [`Scenario::Silence`].
    pub scenario: Scenario,
    /// The mock WebSocket server to connect to.
    pub ws: &'a MockWsServer,
    /// The mock REST server, configured by [`VenueScript::with_rest`].
    pub rest: &'a MockRest,
}

/// Pause and resume hooks of a running connector.
pub struct PauseResume {
    /// Pauses the connector.
    pub pause: Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
    /// Resumes the connector.
    pub resume: Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
}

/// A running connector under test.
pub struct ConnectorRun {
    /// The connector's `run` future.
    pub future: Pin<Box<dyn Future<Output = Result<(), MdcError>> + Send>>,
    /// Requests graceful shutdown.
    pub stop: Box<dyn FnOnce() + Send>,
    /// Pause/resume control, when the connector has one ([`Scenario::PauseResume`] needs it).
    pub pause_resume: Option<PauseResume>,
}

impl ConnectorRun {
    /// Creates a run without pause/resume control.
    pub fn new(
        future: impl Future<Output = Result<(), MdcError>> + Send + 'static,
        stop: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            future: Box::pin(future),
            stop: Box::new(stop),
            pause_resume: None,
        }
    }

    /// Attaches pause/resume hooks.
    pub fn with_pause_resume(mut self, hooks: PauseResume) -> Self {
        self.pause_resume = Some(hooks);
        self
    }
}

const SCENARIO_BOUND: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(2);

/// Executes the standard scenarios end-to-end against the connector `builder`
/// produces, using sinks from `sink_factory`, and returns the combined report.
///
/// `builder` wires a connector to the harness servers and sink and starts it; `sink_factory`
/// supplies a sink per scenario (a failing one for [`Scenario::SinkFault`]). Scenarios the
/// script or connector cannot drive are listed in [`ConformanceReport::scenarios_skipped`].
/// Every scenario validates the recorded stream: alert ordering, a snapshot book before
/// `ResyncCompleted`, and no market data after an alert that marks the book stale.
///
/// # Panics
///
/// Panics on any conformance violation, timeout, or unexpected connector result.
pub async fn run<S, B, F>(script: &VenueScript, builder: B, sink_factory: F) -> ConformanceReport
where
    S: ConformanceSink,
    B: for<'a> Fn(&Harness<'a>, S) -> ConnectorRun,
    F: Fn(Scenario) -> S,
{
    let mut total = ConformanceReport::default();
    for scenario in Scenario::ALL {
        let outcome = run_scenario(scenario, script, &builder, &sink_factory)
            .await
            .unwrap_or_else(|err| panic!("Conformance violation in {scenario:?}: {err}"));
        match outcome {
            Outcome::Skipped(reason) => total.scenarios_skipped.push((scenario, reason)),
            Outcome::Passed(report) => {
                total.scenarios_run.push(scenario);
                total.total_events += report.total_events;
                total.snapshots_verified += report.snapshots_verified;
                total.deltas_verified += report.deltas_verified;
                total.trades_verified += report.trades_verified;
                total.tickers_verified += report.tickers_verified;
                total.reference_rates_verified += report.reference_rates_verified;
                total.alerts_verified += report.alerts_verified;
            }
        }
    }
    total
}

enum Outcome {
    Passed(ConformanceReport),
    Skipped(&'static str),
}

async fn run_scenario<S, B, F>(
    scenario: Scenario,
    script: &VenueScript,
    builder: &B,
    sink_factory: &F,
) -> Result<Outcome, String>
where
    S: ConformanceSink,
    B: for<'a> Fn(&Harness<'a>, S) -> ConnectorRun,
    F: Fn(Scenario) -> S,
{
    if let Some(reason) = script.unsupported(scenario) {
        return Ok(Outcome::Skipped(reason));
    }
    let ws = MockWsServer::spawn()
        .await
        .map_err(|e| format!("mock server: {e}"))?;
    let rest = MockRest::start().await;
    if let Some(setup) = &script.rest {
        setup(&rest, scenario).await;
    }
    let sink = sink_factory(scenario);
    let observer = sink.clone();
    let ConnectorRun {
        future,
        stop,
        pause_resume,
    } = builder(
        &Harness {
            scenario,
            ws: &ws,
            rest: &rest,
        },
        sink,
    );
    let task = tokio::spawn(future);

    if scenario == Scenario::SinkFault {
        let result = join(task).await?;
        drop(stop);
        if result.is_ok() {
            return Err("connector returned Ok after an injected sink fault".to_string());
        }
        return validate(&observer.recorded_events()).map(Outcome::Passed);
    }
    if scenario == Scenario::PauseResume && pause_resume.is_none() {
        stop();
        join(task)
            .await?
            .map_err(|e| format!("connector failed while stopping: {e}"))?;
        return Ok(Outcome::Skipped("connector has no pause/resume control"));
    }

    let flow = Flow {
        ws: &ws,
        rest: &rest,
        observer: &observer,
        script,
        answered: Cell::new(0),
    };
    if let Err(e) = flow.drive(scenario, pause_resume.as_ref()).await {
        stop();
        return Err(format!(
            "{e}; recorded events: {:?}",
            observer.recorded_events()
        ));
    }
    stop();
    if let Err(e) = join(task).await? {
        return Err(format!("connector failed during {scenario:?}: {e}"));
    }
    validate(&observer.recorded_events()).map(Outcome::Passed)
}

async fn join(
    task: tokio::task::JoinHandle<Result<(), MdcError>>,
) -> Result<Result<(), MdcError>, String> {
    match tokio::time::timeout(SCENARIO_BOUND, task).await {
        Err(_) => Err("connector did not stop within the scenario bound".to_string()),
        Ok(Err(e)) => Err(format!("connector task panicked: {e}")),
        Ok(Ok(result)) => Ok(result),
    }
}

/// Drives one connector through a scenario, waiting on its recorded events.
struct Flow<'a, S> {
    ws: &'a MockWsServer,
    rest: &'a MockRest,
    observer: &'a S,
    script: &'a VenueScript,
    /// Client messages already consumed by resolved [`Frame::Computed`] frames.
    answered: Cell<usize>,
}

impl<S: ConformanceSink> Flow<'_, S> {
    async fn drive(
        &self,
        scenario: Scenario,
        pause_resume: Option<&PauseResume>,
    ) -> Result<(), String> {
        self.handshakes(1).await?;
        match scenario {
            Scenario::StartupAndShutdown | Scenario::SinkFault => {}
            Scenario::Cancel | Scenario::RateLimited | Scenario::StaleSnapshot => {
                let synced = self.resync(0, &self.script.snapshot).await?;
                if scenario != Scenario::Cancel {
                    // The first snapshot request failed or was rejected: a second one recovered.
                    self.rest_requests(2).await?;
                }
                self.live_data(synced).await?;
            }
            Scenario::ServerCloseAndRecover => {
                let live = self.synced_with_data().await?;
                self.ws
                    .broadcast_close()
                    .map_err(|e| format!("no connected client to close: {e}"))?;
                self.handshakes(2).await?;
                self.resync(live, &self.script.snapshot).await?;
            }
            Scenario::Gap | Scenario::BadChecksum => {
                let (fault, expect): (_, fn(&IntegrityAlert) -> bool) = if scenario == Scenario::Gap
                {
                    (self.script.gap.as_ref(), |a| {
                        matches!(a, IntegrityAlert::GapDetected { .. })
                    })
                } else {
                    (self.script.bad_checksum.as_ref(), |a| {
                        matches!(a, IntegrityAlert::ChecksumMismatch { .. })
                    })
                };
                let fault = fault.ok_or("scenario without a fault script")?;
                let live = self.synced_with_data().await?;
                self.send(&fault.frames).await?;
                let alerted = self
                    .wait(&format!("the {scenario:?} alert"), |events| {
                        find_alert(events, live, expect)
                    })
                    .await?;
                let recovery = fault.recovery.as_deref().unwrap_or(&self.script.snapshot);
                self.resync(alerted, recovery).await?;
            }
            Scenario::Silence => {
                let live = self.synced_with_data().await?;
                let silent = self
                    .wait("the silence alert", |events| {
                        find_alert(events, live, |a| {
                            matches!(
                                a,
                                IntegrityAlert::SilenceTimeout { .. }
                                    | IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout
                                    }
                            )
                        })
                    })
                    .await?;
                self.handshakes(2).await?;
                self.resync(silent, &self.script.snapshot).await?;
            }
            Scenario::PauseResume => {
                let hooks = pause_resume.ok_or("pause/resume hooks missing")?;
                let live = self.synced_with_data().await?;
                (hooks.pause)().await;
                let paused = self
                    .wait("the Paused disconnect", |events| {
                        find_alert(events, live, |a| {
                            matches!(
                                a,
                                IntegrityAlert::ConnectionLost {
                                    reason: DisconnectKind::Paused
                                }
                            )
                        })
                    })
                    .await?;
                (hooks.resume)().await;
                self.handshakes(2).await?;
                let synced = self.resync(paused, &self.script.snapshot).await?;
                self.live_data(synced).await?;
            }
        }
        Ok(())
    }

    /// Initial synchronization followed by one live data frame; returns the index of the
    /// first market-data event.
    async fn synced_with_data(&self) -> Result<usize, String> {
        let synced = self.resync(0, &self.script.snapshot).await?;
        self.live_data(synced).await
    }

    /// Waits for a `ResyncStarted` after `from`, sends `frames`, and requires the full
    /// `ResyncStarted` → snapshot book → `ResyncCompleted` cycle. Returns the index of
    /// `ResyncCompleted`.
    async fn resync(&self, from: usize, frames: &[Frame]) -> Result<usize, String> {
        let started = self
            .wait("ResyncStarted", |events| {
                find_alert(events, from, |a| matches!(a, IntegrityAlert::ResyncStarted))
            })
            .await?;
        self.send(frames).await?;
        self.wait(
            "ResyncStarted -> snapshot book -> ResyncCompleted",
            |events| find_resync_cycle(events, started),
        )
        .await
    }

    /// Sends the script's live data and waits for market data after `after`.
    async fn live_data(&self, after: usize) -> Result<usize, String> {
        self.send(&self.script.data).await?;
        self.wait("market data after synchronization", |events| {
            find_market_data(events, after + 1)
        })
        .await
    }

    async fn send(&self, frames: &[Frame]) -> Result<(), String> {
        for frame in frames {
            match frame {
                Frame::Computed(ComputedFrame(resolve)) => {
                    let resolved =
                        poll_until("the client message a computed frame answers", || {
                            let messages = self.ws.received_messages();
                            let resolved =
                                resolve(&messages[self.answered.get().min(messages.len())..])?;
                            self.answered.set(messages.len());
                            Some(resolved)
                        })
                        .await?;
                    resolved.broadcast(self.ws)?;
                }
                plain => {
                    plain.broadcast(self.ws)?;
                }
            }
        }
        Ok(())
    }

    async fn handshakes(&self, count: usize) -> Result<(), String> {
        tokio::time::timeout(SCENARIO_BOUND, self.ws.wait_for_handshakes(count))
            .await
            .map(drop)
            .map_err(|_| format!("timed out waiting for handshake #{count}"))
    }

    async fn rest_requests(&self, count: usize) -> Result<(), String> {
        self.rest
            .wait_for_requests_within(count, SCENARIO_BOUND)
            .await;
        Ok(())
    }

    /// Waits for a condition over the sink's recorded events.
    async fn wait<T>(
        &self,
        what: &str,
        find: impl Fn(&[RecordedEvent]) -> Option<T>,
    ) -> Result<T, String> {
        poll_until(what, || find(&self.observer.recorded_events())).await
    }
}

/// Polls `check` until it yields a value, failing after [`SCENARIO_BOUND`].
async fn poll_until<T>(what: &str, check: impl Fn() -> Option<T>) -> Result<T, String> {
    let poll = async {
        loop {
            if let Some(found) = check() {
                return found;
            }
            tokio::time::sleep(POLL).await;
        }
    };
    tokio::time::timeout(SCENARIO_BOUND, poll)
        .await
        .map_err(|_| format!("timed out waiting for {what}"))
}

/// Index of the first alert at or after `from` matching `matches`.
fn find_alert(
    events: &[RecordedEvent],
    from: usize,
    matches: impl Fn(&IntegrityAlert) -> bool,
) -> Option<usize> {
    events
        .iter()
        .enumerate()
        .skip(from)
        .find_map(|(idx, event)| match event {
            RecordedEvent::Alert { alert, .. } if matches(alert) => Some(idx),
            _ => None,
        })
}

/// Index of the `ResyncCompleted` that closes a `ResyncStarted` → snapshot book cycle
/// beginning at or after `from`; a later `ResyncStarted` restarts the cycle.
fn find_resync_cycle(events: &[RecordedEvent], from: usize) -> Option<usize> {
    enum Stage {
        Start,
        Snapshot,
        End,
        Completed,
    }
    let mut stage = Stage::Start;
    for (idx, event) in events.iter().enumerate().skip(from) {
        match (event, &stage) {
            (
                RecordedEvent::Alert {
                    alert: IntegrityAlert::ResyncStarted,
                    ..
                },
                _,
            ) => stage = Stage::Snapshot,
            (
                RecordedEvent::BeginBook {
                    kind: BookKind::Snapshot,
                    ..
                },
                Stage::Snapshot,
            ) => stage = Stage::End,
            (RecordedEvent::EndBook { .. }, Stage::End) => stage = Stage::Completed,
            (
                RecordedEvent::Alert {
                    alert: IntegrityAlert::ResyncCompleted { .. },
                    ..
                },
                Stage::Completed,
            ) => return Some(idx),
            _ => {}
        }
    }
    None
}

/// Index of the first market-data event at or after `from`.
fn find_market_data(events: &[RecordedEvent], from: usize) -> Option<usize> {
    events
        .iter()
        .enumerate()
        .skip(from)
        .find_map(|(idx, event)| match event {
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
            | RecordedEvent::Trade { .. }
            | RecordedEvent::BookTicker { .. } => Some(idx),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdc_core::Symbol;

    fn alert(alert: IntegrityAlert) -> RecordedEvent {
        RecordedEvent::Alert {
            symbol: Symbol::from("BTCUSDT"),
            alert,
        }
    }

    fn begin(kind: BookKind) -> RecordedEvent {
        RecordedEvent::BeginBook {
            symbol: Symbol::from("BTCUSDT"),
            kind,
        }
    }

    fn end() -> RecordedEvent {
        RecordedEvent::EndBook {
            symbol: Symbol::from("BTCUSDT"),
            seq: None,
        }
    }

    const COMPLETED: IntegrityAlert = IntegrityAlert::ResyncCompleted {
        last_update_id: None,
    };

    #[test]
    fn resync_cycle_requires_started_snapshot_end_completed_in_order() {
        let full = [
            alert(IntegrityAlert::ResyncStarted),
            begin(BookKind::Snapshot),
            end(),
            alert(COMPLETED),
        ];
        assert_eq!(find_resync_cycle(&full, 0), Some(3));

        // A delta book does not stand in for the snapshot.
        let delta_only = [
            alert(IntegrityAlert::ResyncStarted),
            begin(BookKind::Delta),
            end(),
            alert(COMPLETED),
        ];
        assert_eq!(find_resync_cycle(&delta_only, 0), None);

        // Completion without a preceding snapshot book does not count.
        let no_snapshot = [alert(IntegrityAlert::ResyncStarted), alert(COMPLETED)];
        assert_eq!(find_resync_cycle(&no_snapshot, 0), None);
    }

    #[test]
    fn resync_cycle_restarts_on_a_new_resync_started_and_honours_from() {
        let events = [
            alert(IntegrityAlert::ResyncStarted),
            begin(BookKind::Snapshot),
            end(),
            // The first cycle never completed; a second one begins and does.
            alert(IntegrityAlert::ResyncStarted),
            begin(BookKind::Snapshot),
            end(),
            alert(COMPLETED),
        ];
        assert_eq!(find_resync_cycle(&events, 0), Some(6));
        // Starting after the first ResyncStarted still finds the second cycle only.
        assert_eq!(find_resync_cycle(&events, 1), Some(6));
        // Starting past the second ResyncStarted finds no complete cycle.
        assert_eq!(find_resync_cycle(&events, 4), None);
    }

    #[test]
    fn market_data_ignores_snapshot_books_and_earlier_events() {
        let events = [
            begin(BookKind::Delta),
            begin(BookKind::Snapshot),
            begin(BookKind::Delta),
        ];
        assert_eq!(find_market_data(&events, 0), Some(0));
        assert_eq!(find_market_data(&events, 1), Some(2));
        assert_eq!(find_market_data(&events[..2], 1), None);
    }
}
