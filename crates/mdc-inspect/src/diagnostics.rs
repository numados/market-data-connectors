//! Bounded redacting in-memory diagnostics (PRD 31 §3.8).
//!
//! A custom `tracing` subscriber feeds a 256-record ring. Only the level,
//! target, event name, and an allow-list of field names (venue, symbol,
//! connection identifier, provider) are retained; arbitrary messages and
//! field values — URLs, configuration `Debug` output, error chains,
//! credentials — are dropped by default (requirement 128).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Metadata, Subscriber};

use crate::config::DIAGNOSTICS_RING;

/// Field names safe to retain.
const ALLOWED_FIELDS: &[&str] = &["venue", "symbol", "connection", "connection_id", "provider"];

/// One retained diagnostics record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticRecord {
    /// Event level.
    pub level: Level,
    /// Event target (module path of the emitting site).
    pub target: String,
    /// Event name.
    pub name: String,
    /// Allow-listed, non-secret field values.
    pub fields: Vec<(String, String)>,
}

/// Bounded diagnostics ring with an eviction counter.
#[derive(Debug)]
pub struct DiagnosticsRing {
    records: VecDeque<DiagnosticRecord>,
    evicted: usize,
    capacity: usize,
}

impl DiagnosticsRing {
    /// Creates an empty ring with the configured capacity.
    pub fn new() -> Self {
        Self::with_capacity(DIAGNOSTICS_RING)
    }

    /// Creates an empty ring with an explicit capacity (tests).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: VecDeque::with_capacity(capacity),
            evicted: 0,
            capacity,
        }
    }

    /// Pushes a record, evicting the oldest when full.
    pub fn push(&mut self, record: DiagnosticRecord) {
        if self.capacity == 0 {
            self.evicted += 1;
            return;
        }
        if self.records.len() >= self.capacity {
            self.records.pop_front();
            self.evicted += 1;
        }
        self.records.push_back(record);
    }

    /// Immutable snapshot view (oldest first) with the eviction count.
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        DiagnosticsSnapshot {
            records: self.records.iter().cloned().collect(),
            evicted: self.evicted,
        }
    }
}

impl Default for DiagnosticsRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of the diagnostics ring for rendering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    /// Retained records, oldest first.
    pub records: Vec<DiagnosticRecord>,
    /// Records evicted from the bounded ring.
    pub evicted: usize,
}

/// Redacting `tracing` subscriber writing into a shared ring.
pub struct DiagnosticsSubscriber {
    ring: Arc<Mutex<DiagnosticsRing>>,
}

impl DiagnosticsSubscriber {
    /// Creates a subscriber over a fresh ring and returns both.
    pub fn new() -> (Arc<Mutex<DiagnosticsRing>>, Self) {
        let ring = Arc::new(Mutex::new(DiagnosticsRing::new()));
        let subscriber = Self {
            ring: Arc::clone(&ring),
        };
        (ring, subscriber)
    }

    /// Creates a subscriber over an existing ring.
    pub fn from_ring(ring: Arc<Mutex<DiagnosticsRing>>) -> Self {
        Self { ring }
    }
}

impl Default for DiagnosticsSubscriber {
    fn default() -> Self {
        let (_, subscriber) = Self::new();
        subscriber
    }
}

impl Subscriber for DiagnosticsSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = AllowListVisitor::default();
        event.record(&mut visitor);
        let record = DiagnosticRecord {
            level: *event.metadata().level(),
            target: event.metadata().target().to_string(),
            name: event.metadata().name().to_string(),
            fields: visitor.fields,
        };
        // Poisoned synchronization must recover without panicking
        // (requirement 131).
        let mut ring = match self.ring.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        ring.push(record);
    }
}

/// Field visitor retaining only allow-listed field values.
#[derive(Default)]
struct AllowListVisitor {
    fields: Vec<(String, String)>,
}

impl Visit for AllowListVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if is_allowed(field.name()) {
            self.fields
                .push((field.name().to_string(), format!("{value:?}")));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if is_allowed(field.name()) {
            self.fields
                .push((field.name().to_string(), value.to_string()));
        }
    }
}

fn is_allowed(name: &str) -> bool {
    ALLOWED_FIELDS.contains(&name)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tracing::info;

    #[test]
    fn ring_is_bounded_and_reports_evictions() {
        let mut ring = DiagnosticsRing::with_capacity(4);
        for index in 0..10 {
            ring.push(DiagnosticRecord {
                level: Level::INFO,
                target: "t".to_string(),
                name: format!("n{index}"),
                fields: Vec::new(),
            });
        }
        let snapshot = ring.snapshot();
        assert_eq!(snapshot.records.len(), 4);
        assert_eq!(snapshot.evicted, 6);
        assert_eq!(snapshot.records.last().unwrap().name, "n9");
    }

    #[test]
    fn subscriber_retains_allow_listed_fields_and_drops_messages() {
        let (ring, subscriber) = DiagnosticsSubscriber::new();
        tracing::subscriber::with_default(subscriber, || {
            info!(
                message = "secret-url https://user:pass@example.com/creds",
                venue = "binance",
                symbol = "BTCUSDT",
                connection_id = 7,
                password = "hunter2",
                "event text with secrets"
            );
        });
        let snapshot = ring.lock().unwrap().snapshot();
        assert_eq!(snapshot.records.len(), 1);
        let record = &snapshot.records[0];
        let names: Vec<&str> = record
            .fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert!(names.contains(&"venue"));
        assert!(names.contains(&"symbol"));
        assert!(names.contains(&"connection_id"));
        assert!(!names.contains(&"message"));
        assert!(!names.contains(&"password"));
        for (_, value) in &record.fields {
            assert!(!value.contains("secret"));
            assert!(!value.contains("hunter2"));
        }
    }

    #[test]
    fn poisoned_ring_recovers_without_panicking() {
        let ring = Arc::new(Mutex::new(DiagnosticsRing::new()));
        let holder = Arc::clone(&ring);
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = holder.lock().unwrap();
            panic!("poison the diagnostics mutex");
        }));
        assert!(poisoned.is_err());
        // A poisoned mutex must not panic the subscriber (requirement 131).
        let subscriber = DiagnosticsSubscriber::from_ring(Arc::clone(&ring));
        tracing::subscriber::with_default(subscriber, || {
            info!(venue = "binance", "event after poison");
        });
        let snapshot = ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot();
        assert_eq!(snapshot.records.len(), 1);
    }
}
