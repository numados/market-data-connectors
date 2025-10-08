#![cfg(feature = "tokio")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use futures_util::StreamExt;
use mdc_core::{
    BookTicker, DeliveryStamp, MarketDataKind, MarketEvent, PriceLevel, RunId, Symbol, UnixNanos,
    VenueId, sink::MarketDataSink,
};
use mdc_sinks::{DeliveryFaultReason, loss_intolerant_channel};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

fn make_test_event(ordinal: u64) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: Some(UnixNanos(1000 + ordinal)),
        received_ts: UnixNanos(1005 + ordinal),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 1,
        delivery: DeliveryStamp::new(RunId::from_bytes([42; 16]), 1, ordinal),
        source_sequence: None,
        kind: MarketDataKind::BookTicker(BookTicker {
            best_bid: PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            best_ask: PriceLevel::new(Decimal::from(101), Decimal::from(1)),
            update_id: Some(ordinal),
        }),
    }
}

#[tokio::test]
async fn test_capacity_cap_and_terminal_fault_slot() {
    let cancel = CancellationToken::new();
    let capacity = 3;
    let affected_conn = 101;
    let (tx, mut rx) = loss_intolerant_channel(capacity, affected_conn, cancel.clone());

    assert_eq!(tx.capacity(), 3);
    assert_eq!(tx.affected_connection(), 101);
    assert_eq!(rx.affected_connection(), 101);
    assert_eq!(tx.len(), 0);

    // Enqueue 3 events within ordinary budget
    for i in 1..=3 {
        assert!(tx.try_send(make_test_event(i)).is_ok());
    }
    assert_eq!(tx.len(), 3);
    assert!(!cancel.is_cancelled());

    // 4th event exceeds capacity: must fail and store terminal fault in reserved slot
    let ev4 = make_test_event(4);
    let send_err = tx.try_send(ev4).unwrap_err();
    assert_eq!(send_err.affected_connection, 101);
    assert_eq!(send_err.stamp.ordinal, 4);
    assert_eq!(
        send_err.reason,
        DeliveryFaultReason::CapacityExceeded { capacity: 3 }
    );

    // Producer must be cancelled immediately
    assert!(cancel.is_cancelled());

    // Subsequent sends coalesce to the same terminal fault and do NOT overwrite queue
    let ev5 = make_test_event(5);
    let send_err2 = tx.try_send(ev5).unwrap_err();
    assert_eq!(send_err2.stamp.ordinal, 4); // Retains original fault stamp
    assert_eq!(
        send_err2.reason,
        DeliveryFaultReason::CapacityExceeded { capacity: 3 }
    );
    assert_eq!(tx.len(), 3);

    // Also verify MarketDataSink implementation
    let ev6 = make_test_event(6);
    let sink_err = tx.on_event(ev6).unwrap_err();
    assert_eq!(sink_err.stamp.ordinal, 4);

    // Consumer drains the 3-event prefix
    for i in 1..=3 {
        let item = rx.recv().await.expect("queued item").expect("ok event");
        assert_eq!(item.delivery.ordinal, i);
    }

    // Next item must be the terminal DeliveryFault!
    let fault = rx
        .recv()
        .await
        .expect("terminal fault")
        .expect_err("must be error");
    assert_eq!(fault.affected_connection, 101);
    assert_eq!(fault.stamp.ordinal, 4);
    assert_eq!(
        fault.reason,
        DeliveryFaultReason::CapacityExceeded { capacity: 3 }
    );

    // After terminal fault, stream terminates (EOF)
    assert!(rx.recv().await.is_none());
}

#[tokio::test]
async fn test_receiver_dropped_triggers_producer_cancellation() {
    let cancel = CancellationToken::new();
    let (tx, rx) = loss_intolerant_channel(5, 7, cancel.clone());

    assert!(tx.try_send(make_test_event(1)).is_ok());
    assert!(!cancel.is_cancelled());

    // Drop receiver
    drop(rx);

    // Cancellation token must be triggered
    assert!(cancel.is_cancelled());

    // Subsequent send fails with ReceiverClosed
    let err = tx.try_send(make_test_event(2)).unwrap_err();
    assert_eq!(err.affected_connection, 7);
    assert_eq!(err.reason, DeliveryFaultReason::ReceiverClosed);
}

#[tokio::test]
async fn test_explicit_fail_and_try_recv() {
    let cancel = CancellationToken::new();
    let (tx, mut rx) = loss_intolerant_channel(5, 12, cancel.clone());

    tx.try_send(make_test_event(1)).unwrap();
    tx.try_send(make_test_event(2)).unwrap();

    let fail_stamp = DeliveryStamp::new(RunId::from_bytes([99; 16]), 1, 999);
    let reason = DeliveryFaultReason::ConnectionTerminated("peer reset".to_string());
    let fault = tx.fail(fail_stamp, reason.clone());
    assert_eq!(fault.reason, reason);
    assert!(cancel.is_cancelled());

    // try_recv synchronously
    let e1 = rx.try_recv().unwrap().unwrap().unwrap();
    assert_eq!(e1.delivery.ordinal, 1);

    let e2 = rx.try_recv().unwrap().unwrap().unwrap();
    assert_eq!(e2.delivery.ordinal, 2);

    let f = rx.try_recv().unwrap().unwrap().unwrap_err();
    assert_eq!(
        f.reason,
        DeliveryFaultReason::ConnectionTerminated("peer reset".to_string())
    );

    // Terminated
    assert!(rx.try_recv().unwrap().is_none());
}

#[tokio::test]
async fn test_stream_clean_eof_when_sender_dropped() {
    let cancel = CancellationToken::new();
    let (tx, mut rx) = loss_intolerant_channel(5, 1, cancel.clone());

    tx.try_send(make_test_event(1)).unwrap();
    drop(tx);

    let mut received = Vec::new();
    while let Some(res) = rx.next().await {
        received.push(res.expect("clean event"));
    }

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].delivery.ordinal, 1);
}
