//! Heartbeat-based staleness and sequence gap monitor for Coinbase Exchange.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mdc_core::Symbol;

/// Verdict returned upon processing an incoming heartbeat message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatVerdict {
    /// Heartbeat arrived within expected interval with no sequence gap.
    Healthy,
    /// First heartbeat observed for this symbol or after resubscribe reset.
    First,
    /// Heartbeat arrived after exceeding the silence timeout.
    Timeout {
        /// Elapsed duration since the previous heartbeat.
        silent_for: Duration,
    },
    /// Sequence jump exceeded the configured threshold.
    SequenceGap {
        /// Observed sequence jump.
        gap: u64,
    },
}

/// Timer-driven heartbeat monitor tracking per-product silence and sequence continuity.
#[derive(Debug, Clone)]
pub struct HeartbeatMonitor {
    timeout: Duration,
    gap_threshold: Option<u64>,
    last: HashMap<Symbol, (Instant, u64)>,
}

impl HeartbeatMonitor {
    /// Creates a new `HeartbeatMonitor` with the given silence timeout and optional sequence gap threshold.
    pub fn new(timeout: Duration, gap_threshold: Option<u64>) -> Self {
        Self {
            timeout,
            gap_threshold,
            last: HashMap::new(),
        }
    }

    /// Handles arrival of a heartbeat message for a symbol.
    ///
    /// Evaluates silence duration and sequence jumps, records the new heartbeat state, and returns
    /// a [HeartbeatVerdict].
    pub fn on_heartbeat(
        &mut self,
        symbol: &Symbol,
        sequence: u64,
        now: Instant,
    ) -> HeartbeatVerdict {
        if let Some(&(prev_instant, prev_seq)) = self.last.get(symbol) {
            let silent_for = now.saturating_duration_since(prev_instant);
            if silent_for >= self.timeout {
                self.last.insert(symbol.clone(), (now, sequence));
                return HeartbeatVerdict::Timeout { silent_for };
            }

            if let Some(threshold) = self.gap_threshold {
                if sequence > prev_seq {
                    let gap = sequence - prev_seq;
                    if gap > threshold {
                        self.last.insert(symbol.clone(), (now, sequence));
                        return HeartbeatVerdict::SequenceGap { gap };
                    }
                }
            }

            self.last.insert(symbol.clone(), (now, sequence));
            HeartbeatVerdict::Healthy
        } else {
            self.last.insert(symbol.clone(), (now, sequence));
            HeartbeatVerdict::First
        }
    }

    /// Polls periodic timers and returns all symbols whose last heartbeat is older than the timeout.
    pub fn poll(&mut self, now: Instant) -> impl Iterator<Item = (Symbol, Duration)> {
        let mut timed_out = Vec::new();
        for (symbol, (received_at, _)) in &self.last {
            let silent_for = now.saturating_duration_since(*received_at);
            if silent_for >= self.timeout {
                timed_out.push((symbol.clone(), silent_for));
            }
        }
        timed_out.into_iter()
    }

    /// Forgets the symbol after resubscribe or snapshot so the next heartbeat is classified as `First`.
    pub fn reset(&mut self, symbol: &Symbol) {
        self.last.remove(symbol);
    }

    /// Clears all tracked symbols.
    pub fn clear(&mut self) {
        self.last.clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_monitor_lifecycle_and_verdicts() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(5000);
        let gap_threshold = Some(3000);
        let mut monitor = HeartbeatMonitor::new(timeout, gap_threshold);

        let btc = Symbol::from("BTC-USD");

        // First heartbeat -> First
        assert_eq!(monitor.on_heartbeat(&btc, 100, t0), HeartbeatVerdict::First);

        // 4.9s later -> Healthy (seq 101, gap 1)
        let t1 = t0 + Duration::from_millis(4900);
        assert_eq!(
            monitor.on_heartbeat(&btc, 101, t1),
            HeartbeatVerdict::Healthy
        );

        // 5.1s later (relative to t1) -> Timeout
        let t2 = t1 + Duration::from_millis(5100);
        let verdict = monitor.on_heartbeat(&btc, 102, t2);
        assert!(matches!(
            verdict,
            HeartbeatVerdict::Timeout { silent_for } if silent_for == Duration::from_millis(5100)
        ));

        // Jump 3,000 from 102 to 3102 (within 1s) -> Healthy (threshold is 3,000, jump must exceed)
        let t3 = t2 + Duration::from_millis(1000);
        assert_eq!(
            monitor.on_heartbeat(&btc, 3102, t3),
            HeartbeatVerdict::Healthy
        );

        // Jump 3,001 from 3102 to 6103 (within 1s) -> SequenceGap { gap: 3001 }
        let t4 = t3 + Duration::from_millis(1000);
        assert_eq!(
            monitor.on_heartbeat(&btc, 6103, t4),
            HeartbeatVerdict::SequenceGap { gap: 3001 }
        );

        // Reset makes next heartbeat First
        monitor.reset(&btc);
        let t5 = t4 + Duration::from_millis(500);
        assert_eq!(
            monitor.on_heartbeat(&btc, 6104, t5),
            HeartbeatVerdict::First
        );
    }

    #[test]
    fn test_heartbeat_monitor_poll_timed_out_symbols() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(5000);
        let mut monitor = HeartbeatMonitor::new(timeout, Some(3000));

        let btc = Symbol::from("BTC-USD");
        let eth = Symbol::from("ETH-USD");

        monitor.on_heartbeat(&btc, 100, t0);
        monitor.on_heartbeat(&eth, 200, t0 + Duration::from_millis(1000));

        // At t0 + 4s: none timed out
        let poll_4s: Vec<_> = monitor.poll(t0 + Duration::from_millis(4000)).collect();
        assert!(poll_4s.is_empty());

        // At t0 + 5.1s: BTC timed out (5.1s > 5s), ETH not yet (4.1s < 5s)
        let poll_5_1s: Vec<_> = monitor.poll(t0 + Duration::from_millis(5100)).collect();
        assert_eq!(poll_5_1s.len(), 1);
        assert_eq!(poll_5_1s[0].0, btc);
        assert_eq!(poll_5_1s[0].1, Duration::from_millis(5100));

        // At t0 + 6.1s: both timed out
        let poll_6_1s: Vec<_> = monitor.poll(t0 + Duration::from_millis(6100)).collect();
        assert_eq!(poll_6_1s.len(), 2);
    }
}
