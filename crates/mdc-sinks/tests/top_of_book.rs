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

    // Ticker arriving while invalidated produces Unavailable(NotSynchronized)
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
        .expect("not in sync ticker");
    match q3 {
        QuoteProjection::Unavailable { reason, .. } => {
            assert_eq!(reason, UnavailableReason::NotSynchronized);
        }
        _ => panic!("Expected NotSynchronized"),
    }
}
