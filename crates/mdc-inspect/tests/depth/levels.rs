//! Synthetic, uncrossed prices shared by native depth regressions.
use std::fmt::Write;
use std::ops::Range;

pub(super) fn prices(range: Range<usize>) -> impl Iterator<Item = (usize, usize)> {
    range.map(|index| (1_000_000 - index, 2_000_000 + index))
}

pub(super) fn arrays(range: Range<usize>, quantity: &str, quoted: bool) -> (String, String) {
    let quote = if quoted { "\"" } else { "" };
    let mut bids = String::new();
    let mut asks = String::new();
    for (bid, ask) in prices(range) {
        if !bids.is_empty() {
            bids.push(',');
            asks.push(',');
        }
        write!(bids, "[{quote}{bid}{quote},{quote}{quantity}{quote}]").unwrap();
        write!(asks, "[{quote}{ask}{quote},{quote}{quantity}{quote}]").unwrap();
    }
    (bids, asks)
}
