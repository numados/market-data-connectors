//! Trading symbol and tag identifiers.

use smallstr::SmallString;

/// Trading symbol as the venue spells it (e.g., "BTCUSDT", "BTC/USD", "BTC-USD").
/// Stores up to 16 bytes inline on the stack; longer symbols spill to the heap.
pub type Symbol = SmallString<[u8; 16]>;

/// Consumer-assigned numeric identifier for a symbol, used to avoid string lookups in hot paths.
pub type SymbolTag = u64;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn inline_symbol_capacity() {
        let btc = Symbol::from("BTCUSDT");
        assert!(!btc.spilled());
        assert_eq!(btc.as_str(), "BTCUSDT");

        let slash = Symbol::from("BTC/USD");
        assert!(!slash.spilled());
        assert_eq!(slash.as_str(), "BTC/USD");

        let sats = Symbol::from("1000SATSUSDT");
        assert!(!sats.spilled());
        assert_eq!(sats.as_str(), "1000SATSUSDT");

        let exact_16 = Symbol::from("1234567890123456");
        assert_eq!(exact_16.len(), 16);
        assert!(!exact_16.spilled());

        let spilled_17 = Symbol::from("12345678901234567");
        assert_eq!(spilled_17.len(), 17);
        assert!(spilled_17.spilled());
    }

    #[test]
    fn symbol_size_documented() {
        // Expected size on 64-bit platforms: pointer + capacity + len or inline buffer.
        let size = std::mem::size_of::<Symbol>();
        assert!(size <= 32);
    }
}
