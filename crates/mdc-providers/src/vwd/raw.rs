//! Raw wire models and deserialization for VWD (Infront) FX SNAPQUOTE responses.

use serde::Deserialize;
use serde_json::value::RawValue;

/// A single row in the VWD `SNAPQUOTE` response array.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawRow<'a> {
    /// Snap quote payload if present.
    #[serde(borrow)]
    pub snap_quote: Option<RawSnapQuote<'a>>,
}

/// The inner quote record containing instrument key and market prices.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSnapQuote<'a> {
    /// Instrument key, e.g. `"EURUSD.XRATE.SPOT~X"`.
    pub instrument_key: Option<&'a str>,
    /// Best bid quote.
    #[serde(borrow)]
    pub bid: Option<RawQuote<'a>>,
    /// Best ask quote.
    #[serde(borrow)]
    pub ask: Option<RawQuote<'a>>,
    /// Last trade or indicative rate quote.
    #[serde(borrow)]
    pub last: Option<RawQuote<'a>>,
}

/// A price quote record with timestamp.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawQuote<'a> {
    /// Borrowed raw JSON token representing price.
    #[serde(borrow)]
    pub price: Option<&'a RawValue>,
    /// Quote timestamp, e.g. `"2026-09-29T10:00:00.000+02:00"`.
    pub date_time: Option<&'a str>,
}

/// An owned quote record stored in reusable scratch buffer without heap allocations.
#[derive(Debug, Clone, Default)]
pub struct ParsedVwdQuote {
    /// Instrument key, e.g. `"EURUSD.XRATE.SPOT~X"`.
    pub instrument_key: smallstr::SmallString<[u8; 32]>,
    /// Best bid price.
    pub bid_price: Option<rust_decimal::Decimal>,
    /// Best bid timestamp string.
    pub bid_time: Option<smallstr::SmallString<[u8; 36]>>,
    /// Best ask price.
    pub ask_price: Option<rust_decimal::Decimal>,
    /// Best ask timestamp string.
    pub ask_time: Option<smallstr::SmallString<[u8; 36]>>,
}

/// Pre-allocated scratch buffer for VWD SNAPQUOTE frames.
#[derive(Debug, Default)]
pub struct VwdScratch {
    /// Reused vector of parsed quotes.
    pub quotes: Vec<ParsedVwdQuote>,
}

impl VwdScratch {
    /// Creates a new scratch buffer with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            quotes: Vec::with_capacity(capacity),
        }
    }

    /// Clears quotes while retaining buffer capacity.
    pub fn clear(&mut self) {
        self.quotes.clear();
    }
}

/// Seed for deserializing an array of `RawRow`s into a `VwdScratch` without heap allocations.
pub struct VwdScratchSeed<'a> {
    scratch: &'a mut VwdScratch,
}

impl<'a> VwdScratchSeed<'a> {
    /// Creates a new seed wrapping a pre-allocated scratch buffer.
    pub fn new(scratch: &'a mut VwdScratch) -> Self {
        Self { scratch }
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for VwdScratchSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(VwdScratchVisitor {
            scratch: self.scratch,
        })
    }
}

struct VwdScratchVisitor<'a> {
    scratch: &'a mut VwdScratch,
}

impl<'de> serde::de::Visitor<'de> for VwdScratchVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON array of VWD SNAPQUOTE rows")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        use std::str::FromStr;
        self.scratch.clear();
        while let Some(row) = seq.next_element::<RawRow<'de>>()? {
            if let Some(q) = row.snap_quote {
                let mut parsed = ParsedVwdQuote::default();
                if let Some(key) = q.instrument_key {
                    parsed.instrument_key = smallstr::SmallString::from(key);
                }
                if let Some(bid) = q.bid {
                    if let Some(raw_p) = bid.price {
                        parsed.bid_price = rust_decimal::Decimal::from_str(raw_p.get()).ok();
                    }
                    if let Some(dt) = bid.date_time {
                        parsed.bid_time = Some(smallstr::SmallString::from(dt));
                    }
                }
                if let Some(ask) = q.ask {
                    if let Some(raw_p) = ask.price {
                        parsed.ask_price = rust_decimal::Decimal::from_str(raw_p.get()).ok();
                    }
                    if let Some(dt) = ask.date_time {
                        parsed.ask_time = Some(smallstr::SmallString::from(dt));
                    }
                }
                self.scratch.quotes.push(parsed);
            }
        }
        Ok(())
    }
}

/// Deserializes a raw JSON slice into a list of borrowed `RawRow`s.
pub fn parse_snap_quotes<'a>(payload: &'a str) -> Result<Vec<RawRow<'a>>, serde_json::Error> {
    serde_json::from_str(payload)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::load_fixture;

    #[test]
    fn test_parse_order_book_snapshot() {
        let json = load_fixture("crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json")
            .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 2);

        let row0 = rows[0].snap_quote.as_ref().unwrap();
        assert_eq!(row0.instrument_key, Some("EURUSD.XRATE.SPOT~X"));
        assert_eq!(row0.bid.as_ref().unwrap().price.unwrap().get(), "1.0842");
        assert_eq!(
            row0.bid.as_ref().unwrap().date_time,
            Some("2026-09-29T10:00:00.000+02:00")
        );
        assert_eq!(row0.ask.as_ref().unwrap().price.unwrap().get(), "1.0844");
        assert_eq!(row0.last.as_ref().unwrap().price.unwrap().get(), "1.0843");

        let row1 = rows[1].snap_quote.as_ref().unwrap();
        assert_eq!(row1.instrument_key, Some("GBPUSD.XRATE.SPOT~X"));
        assert_eq!(row1.bid.as_ref().unwrap().price.unwrap().get(), "1.2950");
        assert_eq!(row1.ask.as_ref().unwrap().price.unwrap().get(), "1.2955");
    }

    #[test]
    fn test_parse_inverse_only() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_inverse_only.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].snap_quote.as_ref().unwrap();
        assert_eq!(q.instrument_key, Some("USDEUR.XRATE.SPOT~X"));
        assert_eq!(q.bid.as_ref().unwrap().price.unwrap().get(), "0.92217");
        assert_eq!(q.ask.as_ref().unwrap().price.unwrap().get(), "0.92234");
    }

    #[test]
    fn test_parse_missing_ask() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_missing_ask.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].snap_quote.as_ref().unwrap();
        assert_eq!(q.instrument_key, Some("EURUSD.XRATE.SPOT~X"));
        assert!(q.bid.is_some());
        assert!(q.ask.is_none());
    }

    #[test]
    fn test_parse_zero_price() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_zero_price.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].snap_quote.as_ref().unwrap();
        assert_eq!(q.bid.as_ref().unwrap().price.unwrap().get(), "0.0");
    }

    #[test]
    fn test_parse_empty_array() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/empty_array.json").unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_parse_missing_side_time() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/missing_side_time.json").unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].snap_quote.as_ref().unwrap();
        assert!(q.bid.as_ref().unwrap().date_time.is_some());
        assert!(q.ask.as_ref().unwrap().date_time.is_none());
    }

    #[test]
    fn test_parse_unrelated_fresh_last_time() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/unrelated_fresh_last_time.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        assert_eq!(rows.len(), 1);
        let q = rows[0].snap_quote.as_ref().unwrap();
        assert_eq!(
            q.bid.as_ref().unwrap().date_time,
            Some("2026-09-29T09:59:00.000+02:00")
        );
        assert_eq!(
            q.last.as_ref().unwrap().date_time,
            Some("2026-09-29T10:05:00.000+02:00")
        );
    }
}
