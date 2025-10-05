//! Venue order book checksum calculation strategies and pure algorithms.

use rust_decimal::Decimal;

/// Reference to a price level with both parsed decimals and raw string tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawLevelRef<'a> {
    /// Parsed price decimal.
    pub price: Decimal,
    /// Parsed volume decimal.
    pub volume: Decimal,
    /// Exact JSON number string of price as sent by the venue.
    pub price_raw: &'a str,
    /// Exact JSON number string of quantity as sent by the venue.
    pub volume_raw: &'a str,
}

impl<'a> RawLevelRef<'a> {
    /// Creates a new `RawLevelRef`.
    pub const fn new(
        price: Decimal,
        volume: Decimal,
        price_raw: &'a str,
        volume_raw: &'a str,
    ) -> Self {
        Self {
            price,
            volume,
            price_raw,
            volume_raw,
        }
    }
}

/// Strategy for calculating an order book checksum over the top price levels.
pub trait ChecksumStrategy: Send + Sync + 'static {
    /// Computes the checksum given top bids (best-first) and asks (best-first).
    fn compute(&self, bids: &[RawLevelRef<'_>], asks: &[RawLevelRef<'_>]) -> u32;

    /// Returns the maximum depth of price levels inspected per side.
    fn depth(&self) -> usize;
}

/// Disables checksum computation (always returns 0).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoChecksum;

impl ChecksumStrategy for NoChecksum {
    fn compute(&self, _bids: &[RawLevelRef<'_>], _asks: &[RawLevelRef<'_>]) -> u32 {
        0
    }

    fn depth(&self) -> usize {
        0
    }
}

/// Formats a raw number token for Kraken CRC32 checksum calculation:
/// removes the decimal point and leading zeros. If the result is empty, outputs "0".
pub fn remove_decimal_and_leading_zeros(s: &str, out: &mut String) {
    let start_len = out.len();
    let mut seen_non_zero = false;
    for b in s.bytes() {
        if b == b'.' {
            continue;
        }
        if !seen_non_zero {
            if b == b'0' {
                continue;
            }
            seen_non_zero = true;
        }
        out.push(b as char);
    }
    if out.len() == start_len {
        out.push('0');
    }
}

/// Helper returning the formatted token string.
pub fn clean_token(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    remove_decimal_and_leading_zeros(s, &mut out);
    out
}

/// Kraken WebSocket v2 CRC32 checksum calculation strategy.
///
/// Computes CRC32 over the top 10 asks (sorted low to high) followed by
/// top 10 bids (sorted high to low). For each level, the price and quantity
/// tokens have decimal points and leading zeros removed, then price and qty
/// are concatenated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KrakenChecksum {
    depth: usize,
}

impl Default for KrakenChecksum {
    fn default() -> Self {
        Self { depth: 10 }
    }
}

impl KrakenChecksum {
    /// Creates a new `KrakenChecksum` with the specified level depth (default 10).
    pub const fn new(depth: usize) -> Self {
        Self { depth }
    }
}

impl ChecksumStrategy for KrakenChecksum {
    fn compute(&self, bids: &[RawLevelRef<'_>], asks: &[RawLevelRef<'_>]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        let mut buf = String::with_capacity(64);

        // Top asks (low to high = best ask first)
        for ask in asks.iter().take(self.depth) {
            buf.clear();
            remove_decimal_and_leading_zeros(ask.price_raw, &mut buf);
            hasher.update(buf.as_bytes());

            buf.clear();
            remove_decimal_and_leading_zeros(ask.volume_raw, &mut buf);
            hasher.update(buf.as_bytes());
        }

        // Top bids (high to low = best bid first)
        for bid in bids.iter().take(self.depth) {
            buf.clear();
            remove_decimal_and_leading_zeros(bid.price_raw, &mut buf);
            hasher.update(buf.as_bytes());

            buf.clear();
            remove_decimal_and_leading_zeros(bid.volume_raw, &mut buf);
            hasher.update(buf.as_bytes());
        }

        hasher.finalize()
    }

    fn depth(&self) -> usize {
        self.depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap_or_else(|_| panic!("invalid test decimal: {s}"))
    }

    #[test]
    fn test_remove_decimal_and_leading_zeros_vectors() {
        assert_eq!(clean_token("0.0"), "0");
        assert_eq!(clean_token("0.00001000"), "1000");
        assert_eq!(clean_token("45283.5"), "452835");
        assert_eq!(clean_token("100"), "100");
        assert_eq!(clean_token("0"), "0");
        assert_eq!(clean_token("0.00000000"), "0");
        assert_eq!(clean_token("0.5"), "5");
    }

    #[test]
    fn test_kraken_official_documentation_vector() {
        // Official Kraken documentation example vector from:
        // https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
        let asks = [
            RawLevelRef::new(dec("45285.2"), dec("0.00100000"), "45285.2", "0.00100000"),
            RawLevelRef::new(dec("45286.4"), dec("1.54571953"), "45286.4", "1.54571953"),
            RawLevelRef::new(dec("45286.6"), dec("1.54571109"), "45286.6", "1.54571109"),
            RawLevelRef::new(dec("45289.6"), dec("1.54560911"), "45289.6", "1.54560911"),
            RawLevelRef::new(dec("45290.2"), dec("0.15890660"), "45290.2", "0.15890660"),
            RawLevelRef::new(dec("45291.8"), dec("1.54553491"), "45291.8", "1.54553491"),
            RawLevelRef::new(dec("45294.7"), dec("0.04454749"), "45294.7", "0.04454749"),
            RawLevelRef::new(dec("45296.1"), dec("0.35380000"), "45296.1", "0.35380000"),
            RawLevelRef::new(dec("45297.5"), dec("0.09945542"), "45297.5", "0.09945542"),
            RawLevelRef::new(dec("45299.5"), dec("0.18772827"), "45299.5", "0.18772827"),
        ];

        let bids = [
            RawLevelRef::new(dec("45283.5"), dec("0.10000000"), "45283.5", "0.10000000"),
            RawLevelRef::new(dec("45283.4"), dec("1.54582015"), "45283.4", "1.54582015"),
            RawLevelRef::new(dec("45282.1"), dec("0.10000000"), "45282.1", "0.10000000"),
            RawLevelRef::new(dec("45281.0"), dec("0.10000000"), "45281.0", "0.10000000"),
            RawLevelRef::new(dec("45280.3"), dec("1.54592586"), "45280.3", "1.54592586"),
            RawLevelRef::new(dec("45279.0"), dec("0.07990000"), "45279.0", "0.07990000"),
            RawLevelRef::new(dec("45277.6"), dec("0.03310103"), "45277.6", "0.03310103"),
            RawLevelRef::new(dec("45277.5"), dec("0.30000000"), "45277.5", "0.30000000"),
            RawLevelRef::new(dec("45277.3"), dec("1.54602737"), "45277.3", "1.54602737"),
            RawLevelRef::new(dec("45276.6"), dec("0.15445238"), "45276.6", "0.15445238"),
        ];

        let strategy = KrakenChecksum::default();
        let checksum = strategy.compute(&bids, &asks);
        assert_eq!(checksum, 3310070434);
    }
}
