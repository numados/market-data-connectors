//! Raw wire models and zero-copy deserialization for Frankfurter ECB reference rate responses.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;
use smallstr::SmallString;

/// Maximum number of rate records allowed in a single response payload to prevent unbounded memory growth.
pub const MAX_RATES_PER_BATCH: usize = 1000;

/// A single raw rate record borrowed directly from the JSON response frame.
#[derive(Debug, Clone, Deserialize)]
pub struct RawRateRow<'a> {
    /// Observation date formatted as `YYYY-MM-DD`.
    pub date: &'a str,
    /// Base currency code (e.g. `"EUR"`).
    pub base: &'a str,
    /// Quote currency code (e.g. `"USD"`, `"GBP"`).
    pub quote: &'a str,
    /// Exact JSON numeric token for the midpoint exchange rate.
    #[serde(borrow)]
    pub rate: &'a RawValue,
}

/// A parsed reference rate entry stored in reusable scratch memory without heap allocations.
#[derive(Debug, Clone, Default)]
pub struct ParsedFrankfurterRate {
    /// Date string, e.g. `"2026-03-25"`.
    pub date: SmallString<[u8; 16]>,
    /// Base currency, e.g. `"EUR"`.
    pub base: SmallString<[u8; 8]>,
    /// Quote currency, e.g. `"USD"`.
    pub quote: SmallString<[u8; 8]>,
    /// Exact parsed Decimal rate if strictly positive and exact, or `None` if invalid.
    pub rate: Option<Decimal>,
}

/// Pre-allocated scratch buffer for deserializing Frankfurter ECB rate frames.
#[derive(Debug, Default)]
pub struct FrankfurterScratch {
    /// Reusable collection of parsed rates.
    pub rates: Vec<ParsedFrankfurterRate>,
}

impl FrankfurterScratch {
    /// Creates a new scratch buffer with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rates: Vec::with_capacity(capacity),
        }
    }

    /// Clears rates while preserving vector capacity.
    pub fn clear(&mut self) {
        self.rates.clear();
    }
}

/// Deserialization seed that populates a [`FrankfurterScratch`] buffer directly.
pub struct FrankfurterScratchSeed<'a> {
    scratch: &'a mut FrankfurterScratch,
}

impl<'a> FrankfurterScratchSeed<'a> {
    /// Creates a new seed wrapping a pre-allocated scratch buffer.
    pub fn new(scratch: &'a mut FrankfurterScratch) -> Self {
        Self { scratch }
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for FrankfurterScratchSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(FrankfurterScratchVisitor {
            scratch: self.scratch,
        })
    }
}

struct FrankfurterScratchVisitor<'a> {
    scratch: &'a mut FrankfurterScratch,
}

impl<'de> serde::de::Visitor<'de> for FrankfurterScratchVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON array of Frankfurter rate records")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        self.scratch.clear();
        while let Some(row) = seq.next_element::<RawRateRow<'de>>()? {
            if self.scratch.rates.len() >= MAX_RATES_PER_BATCH {
                return Err(serde::de::Error::custom(
                    "response exceeded maximum rate record batch limit",
                ));
            }

            let mut parsed = ParsedFrankfurterRate {
                date: SmallString::from(row.date),
                base: SmallString::from(row.base),
                quote: SmallString::from(row.quote),
                rate: None,
            };

            let rate_token = row.rate.get().trim();
            if let Ok(dec) = Decimal::from_str(rate_token) {
                if dec > Decimal::ZERO {
                    parsed.rate = Some(dec);
                }
            }

            self.scratch.rates.push(parsed);
        }
        Ok(())
    }
}

/// Deserializes a raw JSON payload into borrowed [`RawRateRow`] items.
pub fn parse_rates_slice<'a>(payload: &'a str) -> Result<Vec<RawRateRow<'a>>, serde_json::Error> {
    serde_json::from_str(payload)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::load_fixture;
    use serde::de::DeserializeSeed;

    #[test]
    fn test_parse_reference_rates_fixture() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
                .unwrap();
        let rows = parse_rates_slice(&json).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].base, "EUR");
        assert_eq!(rows[0].quote, "GBP");
        assert_eq!(rows[0].rate.get(), "0.8623");
        assert_eq!(rows[1].quote, "USD");
        assert_eq!(rows[1].rate.get(), "1.089");
    }

    #[test]
    fn test_parse_identity_row_fixture() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/identity_row.json")
                .unwrap();
        let rows = parse_rates_slice(&json).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].base, "EUR");
        assert_eq!(rows[0].quote, "EUR");
        assert_eq!(rows[0].rate.get(), "1.0");
    }

    #[test]
    fn test_scratch_seed_deserialization() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut deserializer = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut deserializer)
            .unwrap();

        assert_eq!(scratch.rates.len(), 2);
        assert_eq!(scratch.rates[0].quote.as_str(), "GBP");
        assert_eq!(
            scratch.rates[0].rate,
            Some(Decimal::from_str("0.8623").unwrap())
        );
        assert_eq!(scratch.rates[1].quote.as_str(), "USD");
        assert_eq!(
            scratch.rates[1].rate,
            Some(Decimal::from_str("1.089").unwrap())
        );
    }

    #[test]
    fn test_invalid_rate_detected_in_scratch() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/invalid_rate.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut deserializer = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut deserializer)
            .unwrap();

        assert_eq!(scratch.rates.len(), 2);
        assert_eq!(scratch.rates[0].quote.as_str(), "GBP");
        assert!(scratch.rates[0].rate.is_some());
        assert_eq!(scratch.rates[1].quote.as_str(), "USD");
        assert!(scratch.rates[1].rate.is_none());
    }

    #[test]
    fn test_scratch_reused_without_heap_allocation() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);

        // Warm up
        let mut d1 = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d1)
            .unwrap();

        let allocs = allocation_counter::measure(|| {
            let mut d2 = serde_json::Deserializer::from_str(&json);
            FrankfurterScratchSeed::new(&mut scratch)
                .deserialize(&mut d2)
                .unwrap();
        });

        assert_eq!(allocs.count_total, 0);
    }
}
