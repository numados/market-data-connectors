//! L3 Property-based tests for L2Book deduplication and TopOfBookProjector recovery invariants.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_core::{
    BookTicker, DeliveryStamp, IntegrityAlert, L2Delta, L2Snapshot, MarketDataKind, MarketEvent,
    PriceLevel, RunId, Symbol, UnixNanos, VenueId,
};
use mdc_sinks::{BookError, L2Book, QuoteProjection, TopOfBookProjector, UnavailableReason};
use proptest::prelude::*;
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn property_snapshot_dedup_order_invariance(
        p_bid in 100u32..500,
        v_bid1 in 1u32..10_000,
        v_bid2 in 1u32..10_000,
        p_ask in 501u32..1000,
        v_ask1 in 1u32..10_000,
        v_ask2 in 1u32..10_000,
    ) {
        let price_bid = Decimal::from(p_bid);
        let vol_bid1 = Decimal::from(v_bid1);
        let vol_bid2 = Decimal::from(v_bid2);

        let price_ask = Decimal::from(p_ask);
        let vol_ask1 = Decimal::from(v_ask1);
        let vol_ask2 = Decimal::from(v_ask2);

        // Order A: (v1, v2)
        let mut book_a = L2Book::new(None);
        let snap_a = L2Snapshot {
            last_update_id: Some(1),
            bids: vec![
                PriceLevel::new(price_bid, vol_bid1),
                PriceLevel::new(price_bid, vol_bid2),
            ],
            asks: vec![
                PriceLevel::new(price_ask, vol_ask1),
                PriceLevel::new(price_ask, vol_ask2),
            ],
        };
        prop_assert!(book_a.apply_snapshot(&snap_a).is_ok());

        // Order B: (v2, v1)
        let mut book_b = L2Book::new(None);
        let snap_b = L2Snapshot {
            last_update_id: Some(1),
            bids: vec![
                PriceLevel::new(price_bid, vol_bid2),
                PriceLevel::new(price_bid, vol_bid1),
            ],
            asks: vec![
                PriceLevel::new(price_ask, vol_ask2),
                PriceLevel::new(price_ask, vol_ask1),
            ],
        };
        prop_assert!(book_b.apply_snapshot(&snap_b).is_ok());

        // Order invariance: both orders produce the exact same aggregated state
        prop_assert_eq!(book_a.bids(), book_b.bids());
        prop_assert_eq!(book_a.asks(), book_b.asks());

        // Quantities are exactly aggregated with checked arithmetic
        prop_assert_eq!(book_a.best_bid().unwrap().volume, vol_bid1 + vol_bid2);
        prop_assert_eq!(book_a.best_ask().unwrap().volume, vol_ask1 + vol_ask2);

        // No duplicate prices retained
        prop_assert_eq!(book_a.bids().len(), 1);
        prop_assert_eq!(book_a.asks().len(), 1);
    }

    #[test]
    fn property_snapshot_volume_overflow_rejected(
        p in 1u32..10_000,
        extra_vol in 1u32..100,
    ) {
        let price = Decimal::from(p);
        let mut book = L2Book::new(None);
        let snap = L2Snapshot {
            last_update_id: Some(1),
            bids: vec![
                PriceLevel::new(price, Decimal::MAX),
                PriceLevel::new(price, Decimal::from(extra_vol)),
            ],
            asks: vec![PriceLevel::new(price + Decimal::ONE, Decimal::ONE)],
        };
        let err = book.apply_snapshot(&snap).unwrap_err();
        prop_assert_eq!(err, BookError::VolumeOverflow(price));
    }

    #[test]
    fn property_arbitrary_replay_failure_never_publishes(
        cross_bid_offset in 0u32..50,
        rec_ts in 1_000u64..10_000,
    ) {
        let sym = Symbol::from("BTCUSDT");
        let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

        // Start resync
        proj.handle_event(&make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
            None,
            rec_ts,
        ));
        prop_assert!(!proj.is_in_sync());

        // Stage valid snapshot: bid = 100, ask = 105
        let snap = L2Snapshot {
            last_update_id: Some(10),
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
        };
        let snap_out = proj.handle_event(&make_event(
            MarketDataKind::L2Snapshot(snap),
            Some(rec_ts + 1),
            rec_ts + 2,
        ));
        prop_assert!(snap_out.is_none());
        prop_assert!(!proj.is_in_sync());

        // Crossed replay delta: best bid becomes 105 + cross_bid_offset >= best ask (105)
        let crossed_price = Decimal::from(105 + cross_bid_offset);
        let bad_delta = L2Delta {
            sequence: None,
            bids: vec![PriceLevel::new(crossed_price, Decimal::from(1))],
            asks: vec![],
        };
        let delta_out = proj.handle_event(&make_event(
            MarketDataKind::L2Delta(bad_delta),
            Some(rec_ts + 3),
            rec_ts + 4,
        ));
        prop_assert!(delta_out.is_none());
        prop_assert!(!proj.is_in_sync());

        // ResyncCompleted MUST NOT publish quote derived from failed replay
        let comp_out = proj.handle_event(&make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: Some(11),
            }),
            None,
            rec_ts + 5,
        ));

        let is_unavail = matches!(
            comp_out,
            Some(QuoteProjection::Unavailable {
                reason: UnavailableReason::NotSynchronized,
                ..
            })
        );
        prop_assert!(is_unavail);
        prop_assert!(!proj.is_in_sync());
    }

    #[test]
    fn property_recovery_from_valid_snapshot(
        bid_p in 10u32..500,
        spread in 1u32..100,
        rec_ts in 1_000u64..10_000,
    ) {
        let sym = Symbol::from("BTCUSDT");
        let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

        // Put projector into out-of-sync state via gap alert
        proj.handle_event(&make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                expected: 1,
                received: 10,
            }),
            None,
            rec_ts,
        ));
        prop_assert!(!proj.is_in_sync());

        // Valid snapshot arrives without ResyncCompleted: recovers locally!
        let bid = Decimal::from(bid_p);
        let ask = Decimal::from(bid_p + spread);
        let snap = L2Snapshot {
            last_update_id: Some(50),
            bids: vec![PriceLevel::new(bid, Decimal::from(2))],
            asks: vec![PriceLevel::new(ask, Decimal::from(3))],
        };
        let out = proj.handle_event(&make_event(
            MarketDataKind::L2Snapshot(snap),
            Some(rec_ts + 10),
            rec_ts + 10,
        ));

        let is_available = matches!(
            out,
            Some(QuoteProjection::Available {
                bid: b,
                ask: a,
                ..
            }) if b == bid && a == ask
        );
        prop_assert!(is_available);
        prop_assert!(proj.is_in_sync());
    }

    #[test]
    fn property_ticker_after_alert_recovery(
        bid_p in 1u32..10_000,
        spread in 1u32..500,
        rec_ts in 1_000u64..50_000,
    ) {
        let sym = Symbol::from("BTCUSDT");
        let mut proj = TopOfBookProjector::new_ticker(sym, 1);
        prop_assert!(proj.is_in_sync());

        // Invalidate with GapDetected alert
        let alert_ev = make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                expected: 5,
                received: 8,
            }),
            None,
            rec_ts,
        );
        let unavail = proj.handle_event(&alert_ev);
        let is_unavail = matches!(unavail, Some(QuoteProjection::Unavailable { .. }));
        prop_assert!(is_unavail);
        prop_assert!(!proj.is_in_sync());

        // Valid BookTicker arrives -> immediately recovers
        let bid = Decimal::from(bid_p);
        let ask = Decimal::from(bid_p + spread);
        let ticker = BookTicker::new(
            PriceLevel::new(bid, Decimal::from(1)),
            PriceLevel::new(ask, Decimal::from(2)),
            None,
        );
        let ticker_ev = make_event(
            MarketDataKind::BookTicker(ticker),
            Some(rec_ts + 10),
            rec_ts + 10,
        );
        let recovered = proj.handle_event(&ticker_ev);

        let is_recovered = matches!(
            recovered,
            Some(QuoteProjection::Available {
                bid: b,
                ask: a,
                ..
            }) if b == bid && a == ask
        );
        prop_assert!(is_recovered);
        prop_assert!(proj.is_in_sync());
    }
}
