//! Calendar and receipt-clock behavior independent of provider polling.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;

fn observation(timestamp: UnixNanos) -> RateView {
    RateView {
        provider: "rates",
        pair: Symbol::from("EURUSD"),
        midpoint: Decimal::ONE,
        observation_ts: timestamp,
        observation_date: timestamp,
        received_ts: UnixNanos::from_secs(10),
        diagnostic: None,
    }
}

#[test]
fn observation_date_handles_epoch_leap_centuries_and_timestamp_limit() {
    for (timestamp, expected) in [
        (UnixNanos(0), "1970-01-01"),
        (UnixNanos::from_secs(951_782_400), "2000-02-29"),
        (UnixNanos::from_secs(4_107_542_400), "2100-03-01"),
        (UnixNanos(u64::MAX), "2554-07-21"),
    ] {
        assert_eq!(observation(timestamp).observation_date_utc(), expected);
    }
}

#[test]
fn receipt_age_uses_the_supplied_clock_and_saturates_before_receipt() {
    let rate = observation(UnixNanos(0));
    assert_eq!(
        rate.receipt_age_at(UnixNanos::from_secs(13)),
        Duration::from_secs(3)
    );
    assert_eq!(
        rate.receipt_age_at(UnixNanos::from_secs(19)),
        Duration::from_secs(9)
    );
    assert_eq!(rate.receipt_age_at(UnixNanos::from_secs(9)), Duration::ZERO);
}
