#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Tests for TopOfBookProjector and L2Book quote projection.

use mdc_core::{
    BookTicker, DeliveryStamp, IntegrityAlert, L2Delta, L2Snapshot, MarketDataKind, MarketEvent,
    PriceLevel, RunId, Symbol, UnixNanos, VenueId,
};
use mdc_sinks::{QuoteProjection, TopOfBookProjector, UnavailableReason};
use rust_decimal::Decimal;

fn make_event(
    kind: MarketDataKind,
    ex_ts: Option<u64>,
    rec_ts: u64,
) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: ex_ts.map(UnixNanos),
        received_ts: UnixNanos(rec_ts),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 1,
        delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1),
        source_sequence: None,
        kind,
    }
}

#[test]
fn test_staged_snapshot_and_resync_completed_release() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym, 1, Some(10));

    assert!(!proj.is_in_sync());

    let snap = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![PriceLevel::new(Decimal::from(50_000), Decimal::from(1))],
        asks: vec![PriceLevel::new(Decimal::from(50_100), Decimal::from(2))],
    };

    // Snapshot builds candidate state; does not release yet
    let res = proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap),
        Some(1_000),
        1_005,
    ));
    assert!(res.is_none());
    assert!(!proj.is_in_sync());

    // ResyncCompleted releases projection
    let resync_ev = make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(1),
        }),
        None,
        1_010,
    );
    let quote = proj.handle_event(&resync_ev).expect("released quote");

    match quote {
        QuoteProjection::Available {
            bid,
            ask,
            effective_ts,
            ..
        } => {
            assert_eq!(bid, Decimal::from(50_000));
            assert_eq!(ask, Decimal::from(50_100));
            assert_eq!(effective_ts, UnixNanos(1_000));
        }
        _ => panic!("Expected Available projection"),
    }

    assert!(proj.is_in_sync());
}

#[test]
fn test_older_side_effective_timestamp_freshness() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

    // Initial snapshot at exchange timestamp 1000
    let snap = L2Snapshot {
        last_update_id: None,
        bids: vec![
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(99), Decimal::from(5)),
        ],
        asks: vec![PriceLevel::new(Decimal::from(102), Decimal::from(1))],
    };
    proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap),
        Some(1_000),
        1_005,
    ));
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        }),
        None,
        1_010,
    ));

    // Deep delta at price 99 at timestamp 1100 -> does NOT change best bid (100) or best ask (102)
    let deep_delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(99), Decimal::from(10))],
        asks: vec![],
    };
    let q1 = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(deep_delta),
            Some(1_100),
            1_105,
        ))
        .expect("quote");
    match q1 {
        QuoteProjection::Available { effective_ts, .. } => {
            assert_eq!(
                effective_ts,
                UnixNanos(1_000),
                "Deeper delta must not advance top freshness"
            );
        }
        _ => panic!("Expected Available"),
    }

    // Top bid changes from 100 to 101 at timestamp 1200
    let top_bid_delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(101), Decimal::from(2))],
        asks: vec![],
    };
    let q2 = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(top_bid_delta),
            Some(1_200),
            1_205,
        ))
        .expect("quote");
    match q2 {
        QuoteProjection::Available {
            bid, effective_ts, ..
        } => {
            assert_eq!(bid, Decimal::from(101));
            // Bid was updated at 1200, ask remains at 1000 -> older side is 1000
            assert_eq!(
                effective_ts,
                UnixNanos(1_000),
                "Freshness must retain older side's age"
            );
        }
        _ => panic!("Expected Available"),
    }

    // Top ask changes from 102 to 103 at timestamp 1300
    let top_ask_delta = L2Delta {
        sequence: None,
        bids: vec![],
        asks: vec![
            PriceLevel::new(Decimal::from(102), Decimal::ZERO),
            PriceLevel::new(Decimal::from(103), Decimal::from(2)),
        ],
    };
    let q3 = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(top_ask_delta),
            Some(1_300),
            1_305,
        ))
        .expect("quote");
    match q3 {
        QuoteProjection::Available {
            ask, effective_ts, ..
        } => {
            assert_eq!(ask, Decimal::from(103));
            // Bid was updated at 1200, ask was updated at 1300 -> older side is 1200
            assert_eq!(
                effective_ts,
                UnixNanos(1_200),
                "Freshness advances to older side (bid=1200)"
            );
        }
        _ => panic!("Expected Available"),
    }
}

#[test]
fn test_crossed_book_and_empty_side_unavailability() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

    let snap = L2Snapshot {
        last_update_id: None,
        bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap),
        Some(1_000),
        1_000,
    ));
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        }),
        None,
        1_005,
    ));

    // Delta crosses the book (bid moves to 106 >= ask 105)
    let cross_delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(106), Decimal::from(1))],
        asks: vec![],
    };
    let q = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(cross_delta),
            Some(1_010),
            1_010,
        ))
        .expect("crossed projection");

    match q {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::CrossedBook);
        }
        _ => panic!("Expected CrossedBook"),
    }
    assert!(!proj.is_in_sync());
}

#[test]
fn test_native_ticker_mode() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_ticker(sym, 1);

    assert!(proj.is_in_sync());

    let ticker = BookTicker::new(
        PriceLevel::new(Decimal::from(50_000), Decimal::from(1)),
        PriceLevel::new(Decimal::from(50_050), Decimal::from(2)),
        None,
    );

    let q1 = proj
        .handle_event(&make_event(
            MarketDataKind::BookTicker(ticker),
            Some(100),
            105,
        ))
        .expect("available ticker");
    match q1 {
        QuoteProjection::Available {
            bid,
            ask,
            effective_ts,
            ..
        } => {
            assert_eq!(bid, Decimal::from(50_000));
            assert_eq!(ask, Decimal::from(50_050));
            assert_eq!(effective_ts, UnixNanos(100));
        }
        _ => panic!("Expected Available"),
    }

    // Crossed ticker
    let crossed_ticker = BookTicker::new(
        PriceLevel::new(Decimal::from(50_100), Decimal::from(1)),
        PriceLevel::new(Decimal::from(50_050), Decimal::from(1)),
        None,
    );
    let q2 = proj
        .handle_event(&make_event(
            MarketDataKind::BookTicker(crossed_ticker),
            Some(110),
            115,
        ))
        .expect("crossed ticker");
    match q2 {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::CrossedBook);
        }
        _ => panic!("Expected CrossedBook"),
    }

    // Fault alert invalidates ticker mode
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
            expected: 1,
            received: 5,
        }),
        None,
        120,
    ));
    assert!(!proj.is_in_sync());

    // Valid ticker arriving following alert recovers the projection immediately
    let valid_ticker = BookTicker::new(
        PriceLevel::new(Decimal::from(50_000), Decimal::from(1)),
        PriceLevel::new(Decimal::from(50_050), Decimal::from(1)),
        None,
    );
    let q3 = proj
        .handle_event(&make_event(
            MarketDataKind::BookTicker(valid_ticker),
            Some(125),
            125,
        ))
        .expect("recovered ticker");
    match q3 {
        QuoteProjection::Available { bid, ask, .. } => {
            assert_eq!(bid, Decimal::from(50_000));
            assert_eq!(ask, Decimal::from(50_050));
        }
        _ => panic!("Expected Available"),
    }
    assert!(proj.is_in_sync());
}

#[test]
fn test_snapshot_dedup_both_orders_and_overflow() {
    use mdc_sinks::L2Book;

    // Order 1: 100@1 + 100@2 => 100@3
    let mut book1 = L2Book::new(None);
    let snap1 = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(100), Decimal::from(2)),
        ],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    assert!(book1.apply_snapshot(&snap1).is_ok());
    assert_eq!(book1.bids().len(), 1);
    assert_eq!(book1.best_bid().unwrap().price, Decimal::from(100));
    assert_eq!(book1.best_bid().unwrap().volume, Decimal::from(3));

    // Order 2: 100@2 + 100@1 => 100@3
    let mut book2 = L2Book::new(None);
    let snap2 = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![
            PriceLevel::new(Decimal::from(100), Decimal::from(2)),
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
        ],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    assert!(book2.apply_snapshot(&snap2).is_ok());
    assert_eq!(book2.bids().len(), 1);
    assert_eq!(book2.best_bid().unwrap().price, Decimal::from(100));
    assert_eq!(book2.best_bid().unwrap().volume, Decimal::from(3));

    // Ask deduplication: 105@2 + 105@3 => 105@5
    let mut book_ask = L2Book::new(None);
    let snap_ask = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
        asks: vec![
            PriceLevel::new(Decimal::from(105), Decimal::from(2)),
            PriceLevel::new(Decimal::from(105), Decimal::from(3)),
        ],
    };
    assert!(book_ask.apply_snapshot(&snap_ask).is_ok());
    assert_eq!(book_ask.asks().len(), 1);
    assert_eq!(book_ask.best_ask().unwrap().price, Decimal::from(105));
    assert_eq!(book_ask.best_ask().unwrap().volume, Decimal::from(5));

    // Volume overflow on snapshot bid aggregation is rejected
    let mut book_overflow = L2Book::new(None);
    let snap_overflow = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![
            PriceLevel::new(Decimal::from(100), Decimal::MAX),
            PriceLevel::new(Decimal::from(100), Decimal::ONE),
        ],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    let err = book_overflow.apply_snapshot(&snap_overflow).unwrap_err();
    assert!(matches!(err, mdc_sinks::BookError::VolumeOverflow(p) if p == Decimal::from(100)));

    // Projector mapping of volume overflow snapshot
    let mut proj = TopOfBookProjector::new_l2(Symbol::from("BTCUSDT"), 1, None);
    // Move to in-sync first
    proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap1),
        Some(100),
        100,
    ));
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(1),
        }),
        None,
        105,
    ));
    assert!(proj.is_in_sync());

    let q = proj
        .handle_event(&make_event(
            MarketDataKind::L2Snapshot(snap_overflow),
            Some(110),
            110,
        ))
        .expect("unavailable");
    match q {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::InvalidPriceOrVolume);
        }
        _ => panic!("Expected Unavailable(InvalidPriceOrVolume)"),
    }
    assert!(!proj.is_in_sync());
}

#[test]
fn test_local_recovery_from_l2_snapshot_without_resync_completed() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

    // Initial snapshot + ResyncCompleted puts projector in sync
    let snap = L2Snapshot {
        last_update_id: Some(1),
        bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap),
        Some(1000),
        1000,
    ));
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(1),
        }),
        None,
        1005,
    ));
    assert!(proj.is_in_sync());

    // Crossed delta causes local error: best bid 106 >= best ask 105
    let crossed_delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(106), Decimal::from(1))],
        asks: vec![],
    };
    let q_err = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(crossed_delta),
            Some(1010),
            1010,
        ))
        .expect("crossed projection");
    match q_err {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::CrossedBook);
        }
        _ => panic!("Expected Unavailable(CrossedBook)"),
    }
    assert!(!proj.is_in_sync());

    // Out-of-sync projector receives a new valid snapshot WITHOUT ResyncCompleted: recovers locally!
    let new_snap = L2Snapshot {
        last_update_id: Some(2),
        bids: vec![PriceLevel::new(Decimal::from(102), Decimal::from(2))],
        asks: vec![PriceLevel::new(Decimal::from(104), Decimal::from(3))],
    };
    let q_recovered = proj
        .handle_event(&make_event(
            MarketDataKind::L2Snapshot(new_snap),
            Some(1020),
            1020,
        ))
        .expect("recovered quote");
    match q_recovered {
        QuoteProjection::Available { bid, ask, .. } => {
            assert_eq!(bid, Decimal::from(102));
            assert_eq!(ask, Decimal::from(104));
        }
        _ => panic!("Expected Available"),
    }
    assert!(proj.is_in_sync());

    // Subsequent delta works normally on recovered projector
    let delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(103), Decimal::from(1))],
        asks: vec![],
    };
    let q_delta = proj
        .handle_event(&make_event(
            MarketDataKind::L2Delta(delta),
            Some(1030),
            1030,
        ))
        .expect("delta quote");
    match q_delta {
        QuoteProjection::Available { bid, .. } => {
            assert_eq!(bid, Decimal::from(103));
        }
        _ => panic!("Expected Available"),
    }
    assert!(proj.is_in_sync());
}

#[test]
fn test_replay_delta_error_invalidates_candidate_book() {
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

    // Explicit resync cycle begins
    proj.handle_event(&make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
        None,
        1000,
    ));
    assert!(!proj.is_in_sync());

    // Staged snapshot
    let snap = L2Snapshot {
        last_update_id: Some(10),
        bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
        asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
    };
    let snap_res = proj.handle_event(&make_event(
        MarketDataKind::L2Snapshot(snap),
        Some(1005),
        1005,
    ));
    assert!(snap_res.is_none());
    assert!(!proj.is_in_sync());

    // Crossed replay delta (bid 110 >= ask 105) errors during candidate book replay
    let bad_replay_delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(110), Decimal::from(1))],
        asks: vec![],
    };
    let replay_res = proj.handle_event(&make_event(
        MarketDataKind::L2Delta(bad_replay_delta),
        Some(1010),
        1010,
    ));
    assert!(replay_res.is_none());
    assert!(!proj.is_in_sync());

    // ResyncCompleted must NOT publish quote derived from failed replay
    let comp_ev = make_event(
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(11),
        }),
        None,
        1015,
    );
    let comp_res = proj.handle_event(&comp_ev).expect("unavailable projection");
    match comp_res {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::NotSynchronized);
        }
        _ => panic!("Expected Unavailable(NotSynchronized)"),
    }
    assert!(!proj.is_in_sync());

    // New snapshot arrives after failed replay -> recovers locally
    let fresh_snap = L2Snapshot {
        last_update_id: Some(20),
        bids: vec![PriceLevel::new(Decimal::from(99), Decimal::from(1))],
        asks: vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
    };
    let fresh_q = proj
        .handle_event(&make_event(
            MarketDataKind::L2Snapshot(fresh_snap),
            Some(1020),
            1020,
        ))
        .expect("fresh quote");
    match fresh_q {
        QuoteProjection::Available { bid, ask, .. } => {
            assert_eq!(bid, Decimal::from(99));
            assert_eq!(ask, Decimal::from(101));
        }
        _ => panic!("Expected Available"),
    }
    assert!(proj.is_in_sync());
}
