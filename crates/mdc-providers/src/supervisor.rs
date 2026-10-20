//! Shared task plumbing for connectors that spread symbols over several
//! connection tasks: command fan-out, per-connection command forwarding, and
//! the supervision loop with bounded shutdown.

use std::time::Duration;

use mdc_core::MdcError;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::sink_util::SinkError;

/// How long connection tasks get to exit on their own after cancellation
/// before they are aborted.
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Capacity of the broadcast channel feeding every per-connection forwarder.
pub(crate) const COMMAND_BROADCAST_CAPACITY: usize = 32;

/// Capacity of each per-connection command queue.
pub(crate) const CONNECTION_COMMAND_CAPACITY: usize = 16;

/// Forwards commands from the connector handle channel into the broadcast
/// channel until the handle side closes or `cancel` fires.
pub(crate) fn spawn_command_fanout<C: Clone + Send + 'static>(
    aux_tasks: &mut JoinSet<()>,
    mut command_rx: mpsc::Receiver<C>,
    broadcast_tx: broadcast::Sender<C>,
    cancel: CancellationToken,
) {
    aux_tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                cmd = command_rx.recv() => match cmd {
                    Some(cmd) => {
                        let _ = broadcast_tx.send(cmd);
                    }
                    None => break,
                }
            }
        }
    });
}

/// Forwards broadcast commands to one connection task's queue. Exits on
/// cancellation, when the broadcast closes, or when the connection's queue is
/// gone; lagged commands are dropped with a warning.
pub(crate) fn spawn_command_forwarder<C: Clone + Send + 'static>(
    aux_tasks: &mut JoinSet<()>,
    connection_id: u32,
    mut broadcast_rx: broadcast::Receiver<C>,
    task_cmd_tx: mpsc::Sender<C>,
    cancel: CancellationToken,
) {
    aux_tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                cmd = broadcast_rx.recv() => match cmd {
                    Ok(cmd) => {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            sent = task_cmd_tx.send(cmd) => {
                                if sent.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            connection_id,
                            skipped,
                            "Command broadcast lagged; commands dropped"
                        );
                    }
                }
            }
        }
    });
}

fn panic_error(e: &tokio::task::JoinError) -> MdcError {
    MdcError::Transport(Box::new(std::io::Error::other(format!(
        "Connection task panicked: {e}"
    ))))
}

/// Applies the all-connections-failed rule to a finished drain.
fn sink_verdict(sink_failures: usize, total_connections: usize) -> Result<(), MdcError> {
    if total_connections > 0 && sink_failures == total_connections {
        Err(MdcError::Sink(Box::new(SinkError)))
    } else {
        Ok(())
    }
}

/// Supervises connection tasks until they finish or `cancel` fires, then
/// aborts everything that is still running and waits for it to stop.
///
/// On cancellation the tasks get [`SHUTDOWN_GRACE`] to exit on their own; the
/// stragglers are aborted so a task blocked in an await that ignores the token
/// cannot hang shutdown. Sink failures reported while draining count toward
/// the all-connections-failed rule exactly as in normal operation.
pub(crate) async fn supervise_connections(
    mut conn_tasks: JoinSet<Result<(), SinkError>>,
    mut aux_tasks: JoinSet<()>,
    cancel: CancellationToken,
    total_connections: usize,
    grace: Duration,
) -> Result<(), MdcError> {
    let mut sink_failures = 0usize;
    let mut result = Ok(());
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let deadline = tokio::time::sleep(grace);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        _ = &mut deadline => {
                            conn_tasks.abort_all();
                            break;
                        }
                        res = conn_tasks.join_next() => match res {
                            Some(Ok(Err(SinkError))) => sink_failures += 1,
                            Some(Ok(Ok(()))) => {}
                            Some(Err(e)) => {
                                if !e.is_cancelled() {
                                    result = Err(panic_error(&e));
                                }
                            }
                            None => break,
                        }
                    }
                }
                while let Some(res) = conn_tasks.join_next().await {
                    match res {
                        Ok(Err(SinkError)) => sink_failures += 1,
                        Ok(Ok(())) => {}
                        Err(e) if !e.is_cancelled() => result = Err(panic_error(&e)),
                        Err(_) => {}
                    }
                }
                if result.is_ok() {
                    result = sink_verdict(sink_failures, total_connections);
                }
                break;
            }
            res = conn_tasks.join_next() => match res {
                Some(Ok(task_res)) => {
                    if task_res.is_err() {
                        sink_failures += 1;
                    }
                    if conn_tasks.is_empty() {
                        result = sink_verdict(sink_failures, total_connections);
                        break;
                    }
                }
                Some(Err(e)) => {
                    if !e.is_cancelled() {
                        result = Err(panic_error(&e));
                    }
                    break;
                }
                None => break,
            }
        }
    }
    conn_tasks.abort_all();
    aux_tasks.abort_all();
    while conn_tasks.join_next().await.is_some() {}
    while aux_tasks.join_next().await.is_some() {}
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn forwarder_preserves_command_order() {
        let (btx, _) = broadcast::channel::<u32>(8);
        let (tx, mut rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let mut aux = JoinSet::new();
        spawn_command_forwarder(&mut aux, 1, btx.subscribe(), tx, cancel.clone());
        for i in 0..5 {
            btx.send(i).unwrap();
        }
        for i in 0..5 {
            assert_eq!(rx.recv().await, Some(i));
        }
        cancel.cancel();
        while aux.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn forwarder_exits_when_broadcast_closes() {
        let (btx, brx) = broadcast::channel::<u32>(8);
        let (tx, _rx) = mpsc::channel(8);
        let mut aux = JoinSet::new();
        spawn_command_forwarder(&mut aux, 1, brx, tx, CancellationToken::new());
        drop(btx);
        assert!(aux.join_next().await.is_some());
    }

    #[tokio::test]
    async fn forwarder_exits_when_connection_queue_gone() {
        let (btx, brx) = broadcast::channel::<u32>(8);
        let (tx, rx) = mpsc::channel(8);
        let mut aux = JoinSet::new();
        spawn_command_forwarder(&mut aux, 1, brx, tx, CancellationToken::new());
        drop(rx);
        btx.send(1).unwrap();
        assert!(aux.join_next().await.is_some());
    }

    #[tokio::test]
    async fn forwarder_survives_lag_and_delivers_newest() {
        let (btx, brx) = broadcast::channel::<u32>(2);
        for i in 0..6 {
            btx.send(i).unwrap();
        }
        let (tx, mut rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let mut aux = JoinSet::new();
        spawn_command_forwarder(&mut aux, 1, brx, tx, cancel.clone());
        // Oldest commands were dropped; the forwarder keeps running.
        assert_eq!(rx.recv().await, Some(4));
        assert_eq!(rx.recv().await, Some(5));
        btx.send(6).unwrap();
        assert_eq!(rx.recv().await, Some(6));
        cancel.cancel();
        while aux.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn forwarder_exits_on_cancel_even_with_full_queue() {
        let (btx, brx) = broadcast::channel::<u32>(8);
        let (tx, _rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let mut aux = JoinSet::new();
        spawn_command_forwarder(&mut aux, 1, brx, tx, cancel.clone());
        for i in 0..3 {
            btx.send(i).unwrap();
        }
        tokio::task::yield_now().await;
        cancel.cancel();
        assert!(aux.join_next().await.is_some());
    }

    #[tokio::test]
    async fn fanout_broadcasts_and_exits_on_handle_close() {
        let (ctx, crx) = mpsc::channel::<u32>(4);
        let (btx, mut brx) = broadcast::channel(4);
        let mut aux = JoinSet::new();
        spawn_command_fanout(&mut aux, crx, btx, CancellationToken::new());
        ctx.send(7).await.unwrap();
        assert_eq!(brx.recv().await.unwrap(), 7);
        drop(ctx);
        assert!(aux.join_next().await.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_aborts_child_blocked_outside_token() {
        let mut conn = JoinSet::new();
        conn.spawn(async {
            // Ignores the cancel token entirely.
            std::future::pending::<()>().await;
            Ok(())
        });
        let cancel = CancellationToken::new();
        cancel.cancel();
        let res = tokio::time::timeout(
            SHUTDOWN_GRACE * 2,
            supervise_connections(conn, JoinSet::new(), cancel, 1, SHUTDOWN_GRACE),
        )
        .await;
        assert!(matches!(res, Ok(Ok(()))), "shutdown must be bounded");
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_racing_total_sink_failure_is_an_error() {
        let mut conn = JoinSet::new();
        for _ in 0..2 {
            conn.spawn(async { Err(SinkError) });
        }
        let cancel = CancellationToken::new();
        cancel.cancel();
        let res = supervise_connections(conn, JoinSet::new(), cancel, 2, SHUTDOWN_GRACE).await;
        assert!(matches!(res, Err(MdcError::Sink(_))));
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_with_partial_sink_failure_is_ok() {
        let mut conn = JoinSet::new();
        conn.spawn(async { Err(SinkError) });
        conn.spawn(async { Ok(()) });
        let cancel = CancellationToken::new();
        cancel.cancel();
        let res = supervise_connections(conn, JoinSet::new(), cancel, 2, SHUTDOWN_GRACE).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn normal_path_total_sink_failure_is_an_error() {
        let mut conn = JoinSet::new();
        conn.spawn(async { Err(SinkError) });
        let res = supervise_connections(
            conn,
            JoinSet::new(),
            CancellationToken::new(),
            1,
            SHUTDOWN_GRACE,
        )
        .await;
        assert!(matches!(res, Err(MdcError::Sink(_))));
    }
}
