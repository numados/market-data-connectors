//! Bounded redacting in-memory diagnostics (PRD 31 §3.8).
//!
//! A custom `tracing` subscriber feeds INFO-and-above events to a 256-record
//! ring. It retains metadata, bounded non-secret context and fixed summaries.
//! Message text is classified transiently, never retained; URLs, configuration
//! output, error chains, credentials and other unapproved values are dropped.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Metadata, Subscriber};

use crate::config::DIAGNOSTICS_RING;

mod summary;

/// Reserved output field; native fields with this name are not accepted.
pub(crate) const EVENT_FIELD: &str = "event";

const CONTEXT_LIMIT: usize = 64;

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

impl DiagnosticRecord {
    pub(crate) fn summary(&self) -> &'static str {
        self.fields
            .iter()
            .find(|(name, _)| name == EVENT_FIELD)
            .map_or(summary::DEFAULT_SUMMARY, |(_, value)| {
                summary::trusted_label(value)
            })
    }

    pub(crate) fn context_fields(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields.iter().filter_map(|(name, value)| {
            let name = context_field_name(name)?;
            safe_context_value(value).then_some((name, value.as_str()))
        })
    }
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
    max_level: Level,
}

impl DiagnosticsSubscriber {
    /// Creates a subscriber over a fresh ring and returns both.
    pub fn new() -> (Arc<Mutex<DiagnosticsRing>>, Self) {
        let ring = Arc::new(Mutex::new(DiagnosticsRing::new()));
        let subscriber = Self::from_ring(Arc::clone(&ring));
        (ring, subscriber)
    }

    /// Creates a subscriber over an existing ring.
    pub fn from_ring(ring: Arc<Mutex<DiagnosticsRing>>) -> Self {
        Self {
            ring,
            max_level: Level::INFO,
        }
    }

    /// Selects capture verbosity. Redaction remains active at every level.
    pub fn with_max_level(mut self, max_level: Level) -> Self {
        self.max_level = max_level;
        self
    }
}

impl Default for DiagnosticsSubscriber {
    fn default() -> Self {
        let (_, subscriber) = Self::new();
        subscriber
    }
}

impl Subscriber for DiagnosticsSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= self.max_level
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        if !self.enabled(event.metadata()) {
            return;
        }
        let mut visitor = AllowListVisitor::default();
        event.record(&mut visitor);
        visitor.fields.push((
            EVENT_FIELD.to_string(),
            visitor
                .summary
                .unwrap_or(summary::DEFAULT_SUMMARY)
                .to_string(),
        ));
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
    summary: Option<&'static str>,
}

impl Visit for AllowListVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            if self.summary.is_none() {
                self.summary = Some(summary::from_debug(value));
            }
            return;
        }
        if let Some(name) = context_field_name(field.name()) {
            if let Some(value) = summary::bounded_debug(value, CONTEXT_LIMIT)
                .filter(|value| safe_context_value(value))
            {
                self.fields.push((name.to_string(), value));
            }
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            if self.summary.is_none() {
                self.summary = Some(summary::from_text(value));
            }
            return;
        }
        if let Some(name) = context_field_name(field.name()) {
            if safe_context_value(value) {
                self.fields.push((name.to_string(), value.to_string()));
            }
        }
    }
}

fn context_field_name(name: &str) -> Option<&str> {
    match name {
        "sym" => Some("symbol"),
        "venue" | "symbol" | "connection" | "connection_id" | "provider" => Some(name),
        _ => None,
    }
}

pub(crate) fn safe_context_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= CONTEXT_LIMIT
        && !value.contains("://")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'/' | b'.' | b':')
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
