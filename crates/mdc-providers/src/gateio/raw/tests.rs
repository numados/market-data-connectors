use super::*;

const MODERN_SNAPSHOT: &str =
    include_str!("../../../tests/fixtures/gateio/order_book_snapshot_time_ms.json");

#[test]
fn test_extract_symbol() {
    assert_eq!(extract_symbol("ob.BTC_USDT.400"), "BTC_USDT");
    assert_eq!(extract_symbol("ob.ETH_USDT.50"), "ETH_USDT");
    assert_eq!(extract_symbol("BTC_USDT"), "BTC_USDT");
}

#[test]
fn test_parse_create_time_ms() {
    let ts = parse_create_time_ms("1700000000123.456").unwrap();
    assert_eq!(ts.as_nanos(), 1_700_000_000_123_456_000);

    let ts2 = parse_create_time_ms("1700000000123").unwrap();
    assert_eq!(ts2.as_nanos(), 1_700_000_000_123_000_000);

    let ts3 = parse_create_time_ms("1700000000123.5").unwrap();
    assert_eq!(ts3.as_nanos(), 1_700_000_000_123_500_000);

    assert!(parse_create_time_ms("invalid").is_err());
    assert!(parse_create_time_ms("123.").is_err());
}

#[test]
fn test_parse_snapshot_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/order_book_snapshot.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.obu");
    assert_eq!(env.event, Some("update"));

    let obu: RawObu<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
    assert!(obu.full);
    assert_eq!(obu.s, "ob.BTC_USDT.400");
    assert_eq!(obu.u, 123456);
    assert_eq!(obu.b.len(), 2);
    assert_eq!(obu.a.len(), 2);
}

#[test]
fn test_parse_snapshot_seed_zero_alloc() {
    let data = include_str!("../../../tests/fixtures/gateio/order_book_snapshot.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    let mut scratch = GateioConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
    let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

    assert!(header.full);
    assert_eq!(header.s, "ob.BTC_USDT.400");
    assert_eq!(header.last_id, 123456);
    assert_eq!(scratch.bids.len(), 2);
    assert_eq!(scratch.asks.len(), 2);
    assert_eq!(scratch.bids[0].price, Decimal::from(50000));
    assert_eq!(scratch.asks[0].price, Decimal::from(50001));
}

#[test]
fn test_parse_depth_update_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/depth_update.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.obu");
    assert_eq!(env.event, Some("update"));

    let obu: RawObu<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
    assert!(!obu.full);
    assert_eq!(obu.first_id, 123457);
    assert_eq!(obu.u, 123460);
    assert_eq!(obu.b.len(), 2);
    assert_eq!(obu.a.len(), 1);
}

#[test]
fn test_parse_depth_update_seed_zero_alloc() {
    let data = include_str!("../../../tests/fixtures/gateio/depth_update.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    let mut scratch = GateioConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
    let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

    assert!(!header.full);
    assert_eq!(header.first_id, 123457);
    assert_eq!(header.last_id, 123460);
    assert_eq!(scratch.bids.len(), 2);
    assert_eq!(scratch.asks.len(), 1);
}

#[test]
fn test_parse_depth_update_bid_only() {
    let data = include_str!("../../../tests/fixtures/gateio/depth_update_bid_only.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    let mut scratch = GateioConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
    let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

    assert_eq!(scratch.bids.len(), 1);
    assert!(scratch.asks.is_empty());
    assert_eq!(header.first_id, 123461);
    assert_eq!(header.last_id, 123462);
}

#[test]
fn test_parse_depth_update_ask_only() {
    let data = include_str!("../../../tests/fixtures/gateio/depth_update_ask_only.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    let mut scratch = GateioConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
    let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

    assert!(scratch.bids.is_empty());
    assert_eq!(scratch.asks.len(), 1);
    assert_eq!(header.first_id, 123463);
    assert_eq!(header.last_id, 123464);
}

#[test]
fn test_parse_heartbeat_or_ping_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/heartbeat_or_ping.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.ping");
    assert_eq!(env.time, 1700000030);
    assert!(env.event.is_none());
    assert!(env.result.is_none());

    let pong = SpotPong::new(env.time);
    let serialized = serde_json::to_string(&pong).unwrap();
    assert_eq!(serialized, r#"{"time":1700000030,"channel":"spot.pong"}"#);
}

#[test]
fn test_parse_trade_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/trade.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.trades");
    assert_eq!(env.event, Some("update"));

    let trade: RawTrade<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
    assert_eq!(trade.id.to_small_str().as_str(), "987654321");
    assert_eq!(trade.currency_pair, "BTC_USDT");
    assert_eq!(trade.side, "sell");
    assert_eq!(trade.price, Decimal::from(50000));
    assert_eq!(trade.amount, Decimal::from_str_exact("0.01").unwrap());
}

#[test]
fn test_parse_subscribe_ack_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/subscribe_ack.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.obu");
    assert_eq!(env.event, Some("subscribe"));

    let ack: RawSubAckResult<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
    assert_eq!(ack.status, "success");
}

#[test]
fn test_parse_subscribe_ack_error_fixture() {
    let data = include_str!("../../../tests/fixtures/gateio/subscribe_ack_error.json");
    let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
    assert_eq!(env.channel, "spot.obu");
    assert_eq!(env.event, Some("subscribe"));

    let ack: RawSubAckResult<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
    assert_eq!(ack.status, "fail");

    let err: RawSubAckError<'_> = serde_json::from_str(env.error.unwrap().get()).unwrap();
    assert_eq!(err.code, 2);
    assert_eq!(err.message, "invalid symbol");
}

#[test]
fn time_ms_only_snapshot_normalizes_both_envelope_paths() {
    let frame = MODERN_SNAPSHOT;
    let envelope: RawEnvelope<'_> = serde_json::from_str(frame).unwrap();
    assert_eq!(envelope.time, 1_748_572_394);
    assert_eq!(envelope.time_ms, Some(1_748_572_394_325));
    assert_eq!(envelope.channel, "spot.obu");
    let mut scratch = GateioConnectionScratch::with_capacity(4);
    let mut de = serde_json::Deserializer::from_str(frame);
    let header = GateioDepthFrameSeed::new(&mut scratch)
        .deserialize(&mut de)
        .unwrap();
    assert_eq!(header.time, 1_748_572_394);
    assert_eq!(header.time_ms, envelope.time_ms);
    assert_eq!(header.obu.t, 1_748_572_394_325);
    assert!(header.obu.full);
    assert_eq!(scratch.bids[0].price, Decimal::from(50_000));
    assert_eq!(scratch.asks[0].price, Decimal::from(50_001));
}

#[test]
fn heartbeat_accepts_milliseconds_without_fabricating_epoch_zero() {
    let envelope: RawEnvelope<'_> =
        serde_json::from_str(r#"{"channel":"spot.ping","time_ms":1700000030123}"#).unwrap();
    assert_eq!(
        serde_json::to_string(&SpotPong::new(envelope.time)).unwrap(),
        r#"{"time":1700000030,"channel":"spot.pong"}"#
    );
    for (frame, expected) in [
        (r#"{"channel":"spot.ping","time":7,"time_ms":9999999}"#, 7),
        (r#"{"channel":"spot.ping","time":0,"time_ms":9999999}"#, 0),
    ] {
        let envelope: RawEnvelope<'_> = serde_json::from_str(frame).unwrap();
        assert_eq!(envelope.time, expected);
    }
}

#[test]
fn missing_or_malformed_envelope_timestamps_remain_invalid() {
    for suffix in [
        "",
        r#", "time":null, "time_ms":1700000030123"#,
        r#", "time":-1, "time_ms":1700000030123"#,
        r#", "time":"bad", "time_ms":1700000030123"#,
        r#", "time_ms":-1"#,
        ", \"time_ms\":\"1700000030123\"",
        r#", "time_ms":18446744073709551616"#,
    ] {
        let frame = format!(r#"{{"channel":"spot.ping"{suffix}}}"#);
        assert!(
            serde_json::from_str::<serde_json::Value>(&frame).is_ok(),
            "timestamp fixtures must be well-formed JSON: {frame}"
        );
        assert!(
            serde_json::from_str::<RawEnvelope<'_>>(&frame).is_err(),
            "{frame}"
        );
    }
}

#[test]
fn depth_seed_keeps_legacy_absence_and_explicit_seconds_precedence() {
    for (timestamp, expected) in [("", 0), (r#""time":7,"time_ms":9999999,"#, 7)] {
        let frame = format!(
            r#"{{{timestamp}"result":{{"t":1,"full":true,"s":"ob.BTC_USDT.50","u":1,"b":[],"a":[]}}}}"#
        );
        let mut scratch = GateioConnectionScratch::with_capacity(1);
        let mut de = serde_json::Deserializer::from_str(&frame);
        let header = GateioDepthFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();
        assert_eq!(header.time, expected);
    }
}

#[test]
fn modern_envelope_borrows_payload_and_depth_seed_allocates_nothing() {
    let envelope: RawEnvelope<'_> = serde_json::from_str(MODERN_SNAPSHOT).unwrap();
    let payload = envelope.result.unwrap().get();
    let source_start = MODERN_SNAPSHOT.as_ptr() as usize;
    let payload_start = payload.as_ptr() as usize;
    assert!(payload_start >= source_start);
    assert!(payload_start + payload.len() <= source_start + MODERN_SNAPSHOT.len());
    let mut scratch = GateioConnectionScratch::with_capacity(4);
    let mut warmup = serde_json::Deserializer::from_str(MODERN_SNAPSHOT);
    GateioDepthFrameSeed::new(&mut scratch)
        .deserialize(&mut warmup)
        .unwrap();
    let allocs = allocation_counter::measure(|| {
        let mut de = serde_json::Deserializer::from_str(MODERN_SNAPSHOT);
        let header = GateioDepthFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();
        assert_eq!(envelope.time, header.time);
    });
    assert_eq!(allocs.count_total, 0);
}
