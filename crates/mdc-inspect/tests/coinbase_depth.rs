#![cfg(feature = "coinbase")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Synthetic full-depth Coinbase frames exercise native parsing through display publication.

use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use mdc_inspect::config::{CHANNEL_CAPACITY, SessionConfig};
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus};
use mdc_inspect::provider::ProviderKind;
use mdc_inspect::provider::coinbase::CoinbaseAdapter;
use mdc_inspect::reducer::Reducer;
use mdc_inspect::session::{SessionOutcome, run_session};
use mdc_testkit::{MockWsServer, WsAction};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WAIT_BOUND: Duration = Duration::from_secs(10);
const BEST_BID: usize = 1_000_000;

fn snapshot_frame(depth: usize) -> String {
    let mut frame = String::from(r#"{"type":"snapshot","product_id":"BTC-USD","bids":["#);
    for index in 0..depth {
        if index != 0 {
            frame.push(',');
        }
        write!(frame, r#"["{}","1"]"#, BEST_BID - index).unwrap();
    }
    frame.push_str(r#"],"asks":["#);
    for index in 0..depth {
        if index != 0 {
            frame.push(',');
        }
        write!(frame, r#"["{}","2"]"#, BEST_BID + 1 + index).unwrap();
    }
    frame.push_str("]}");
    frame
}

struct TestSession {
    ws: MockWsServer,
    rx: watch::Receiver<Arc<DisplaySnapshot>>,
    task: JoinHandle<SessionOutcome>,
    cancel: CancellationToken,
}

impl TestSession {
    async fn start(frame: String) -> Self {
        let ws = MockWsServer::spawn().await.unwrap();
        ws.push_script(vec![WsAction::WaitClientMessage, WsAction::SendText(frame)]);
        let config = SessionConfig::new(ProviderKind::Coinbase, &["BTC-USD".into()]).unwrap();
        let bounds = ProviderKind::Coinbase.session_bounds();
        let adapter = Arc::new(CoinbaseAdapter::with_endpoints(
            ws.ws_url().unwrap(),
            "http://127.0.0.1:1".parse().unwrap(),
        ));
        let (tx, rx) = watch::channel(Arc::new(Reducer::new(&config, bounds).snapshot()));
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(run_session(
            adapter,
            config,
            bounds,
            CHANNEL_CAPACITY,
            task_cancel,
            tx,
        ));
        Self {
            ws,
            rx,
            task,
            cancel,
        }
    }

    async fn observe(
        &mut self,
        predicate: impl Fn(&DisplaySnapshot) -> bool,
    ) -> Arc<DisplaySnapshot> {
        let deadline = tokio::time::Instant::now() + WAIT_BOUND;
        loop {
            let snapshot = self.rx.borrow_and_update().clone();
            if predicate(&snapshot) {
                return snapshot;
            }
            assert_ne!(snapshot.session, DisplayStatus::Failed, "{snapshot:#?}");
            tokio::time::timeout_at(deadline, self.rx.changed())
                .await
                .expect("display publication before deadline")
                .expect("session still publishing");
        }
    }

    async fn stop(self) {
        self.cancel.cancel();
        let outcome = tokio::time::timeout(WAIT_BOUND, self.task)
            .await
            .expect("bounded shutdown")
            .expect("session joined");
        assert_eq!(outcome.status, DisplayStatus::Stopped);
        assert!(outcome.failure.is_none());
    }
}

#[tokio::test]
async fn full_snapshot_keeps_levels_beyond_the_original_depth_limit() {
    let mut session = TestSession::start(snapshot_frame(25_000)).await;
    let snapshot = session
        .observe(|snapshot| {
            snapshot
                .alerts
                .iter()
                .any(|alert| alert.summary.starts_with("resync completed"))
        })
        .await;
    let symbol = snapshot.symbols.first().unwrap();
    assert_eq!(
        symbol.status,
        DisplayStatus::InSync,
        "{:?}",
        symbol.unavailability
    );
    assert_eq!(symbol.bids.len(), 25_000);
    assert_eq!(symbol.asks.len(), 25_000);
    let quote = &symbol.quote.as_ref().unwrap().quote;
    assert_eq!(quote.bid, 1_000_000_i64.into());
    assert_eq!(quote.ask, 1_000_001_i64.into());

    let mut changes = String::new();
    for index in 0..1500 {
        if index != 0 {
            changes.push(',');
        }
        write!(
            changes,
            r#"["buy","{}","0"],["sell","{}","0"]"#,
            BEST_BID - index,
            BEST_BID + 1 + index
        )
        .unwrap();
    }
    session.ws.broadcast_text(format!(r#"{{"type":"l2update","product_id":"BTC-USD","time":"2024-01-01T00:00:01Z","changes":[{changes}]}}"#)).unwrap();
    let updated = session
        .observe(|snapshot| {
            snapshot
                .symbols
                .first()
                .is_some_and(|symbol| symbol.bids.len() == 23_500 && symbol.asks.len() == 23_500)
        })
        .await;
    let symbol = updated.symbols.first().unwrap();
    assert_eq!(symbol.status, DisplayStatus::InSync);
    let quote = &symbol.quote.as_ref().unwrap().quote;
    assert_eq!(quote.bid, 998_500_i64.into());
    assert_eq!(quote.ask, 1_001_501_i64.into());
    assert_eq!(symbol.bids.last().unwrap().price, 975_001_i64.into());
    assert_eq!(symbol.asks.last().unwrap().price, 1_025_000_i64.into());

    session.ws.broadcast_text(r#"{"type":"l2update","product_id":"BTC-USD","time":"2024-01-01T00:00:02Z","changes":[["buy","1000000","3"],["sell","1000001","4"]]}"#).unwrap();
    let inserted = session
        .observe(|snapshot| {
            snapshot
                .symbols
                .first()
                .is_some_and(|symbol| symbol.bids.len() == 23_501 && symbol.asks.len() == 23_501)
        })
        .await;
    let quote = &inserted
        .symbols
        .first()
        .unwrap()
        .quote
        .as_ref()
        .unwrap()
        .quote;
    assert_eq!(quote.bid, 1_000_000_i64.into());
    assert_eq!(quote.ask, 1_000_001_i64.into());
    assert_eq!(quote.bid_size, 3_i64.into());
    assert_eq!(quote.ask_size, 4_i64.into());
    session.stop().await;
}
