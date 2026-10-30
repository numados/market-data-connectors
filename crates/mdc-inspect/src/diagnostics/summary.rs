//! Fixed event summaries selected without retaining untrusted log text.

use std::fmt::{self, Write};

pub(super) const DEFAULT_SUMMARY: &str = "Event details redacted";
const MESSAGE_LIMIT: usize = 256;

enum Pattern {
    Prefix(&'static str),
    Contains(&'static str),
    Exact(&'static str),
}

impl Pattern {
    fn matches(&self, text: &str) -> bool {
        match self {
            Self::Prefix(prefix) => text.starts_with(prefix),
            Self::Contains(fragment) => text.contains(fragment),
            Self::Exact(expected) => text == *expected,
        }
    }
}

use Pattern::{Contains, Exact, Prefix};

// These labels, never the matched text, are the only retained message data.
const RULES: &[(&str, &[Pattern])] = &[
    (
        "Connection failed",
        &[
            Contains("failed to connect"),
            Exact("invalid bitfinex websocket url"),
        ],
    ),
    (
        "Subscription rejected",
        &[
            Contains("subscription rejected"),
            Contains("subscription error"),
            Prefix("subscribed to unknown"),
        ],
    ),
    (
        "Subscription failed",
        &[
            Contains("failed to send book subscribe"),
            Contains("failed to send subscribe"),
            Contains("failed to send unsubscribe"),
        ],
    ),
    (
        "Client initialization failed",
        &[Exact("failed to initialize rest client")],
    ),
    (
        "Snapshot request failed",
        &[
            Contains("snapshot fetch failed"),
            Contains("failed to fetch bitstamp rest snapshot"),
            Contains("snapshot request returned non-ok"),
        ],
    ),
    (
        "Invalid data received",
        &[
            Contains("failed to parse"),
            Contains("malformed json"),
            Contains("checksum mismatch"),
            Contains("sequence gap"),
            Contains("decompression failed"),
            Contains("missing checksum"),
            Contains("missing sequence_num"),
        ],
    ),
    (
        "Rate limited",
        &[Contains("too_many_requests"), Contains("rate limit")],
    ),
    (
        "Connection closed",
        &[
            Contains("websocket closed"),
            Contains("stream ended"),
            Contains("received close frame"),
            Contains("connection closed"),
        ],
    ),
    (
        "Reconnecting",
        &[
            Prefix("reconnecting"),
            Prefix("backing off before"),
            Contains("forcing reconnect"),
            Contains("triggered reconnect"),
            Contains("; reconnecting"),
            Contains("operator forced reconnect"),
            Contains("operator reconnect requested"),
            Contains("operator requested bitfinex reconnect"),
        ],
    ),
    ("Connecting", &[Prefix("connecting to ")]),
    ("Connected", &[Prefix("connected to ")]),
    ("Unsubscribed", &[Prefix("unsubscribed from channel")]),
    ("Subscribed", &[Exact("subscribed to channel")]),
    (
        "Server information received",
        &[Exact("bitfinex server info")],
    ),
    (
        "Configuration response received",
        &[Exact("bitfinex conf response")],
    ),
    (
        "Stopped",
        &[
            Contains("worker cancelled"),
            Contains("task exited"),
            Contains("terminating connection task"),
        ],
    ),
    ("Paused", &[Prefix("pausing "), Prefix("operator paused")]),
    ("Resuming", &[Prefix("resuming ")]),
    (
        "Provider error",
        &[
            Contains("error frame"),
            Contains("error notification"),
            Contains("error from"),
            Contains("server error"),
        ],
    ),
];

pub(super) fn trusted_label(label: &str) -> &'static str {
    RULES
        .iter()
        .find(|(trusted, _)| *trusted == label)
        .map_or(DEFAULT_SUMMARY, |(trusted, _)| *trusted)
}

pub(super) fn from_text(text: &str) -> &'static str {
    if text.len() > MESSAGE_LIMIT {
        return DEFAULT_SUMMARY;
    }
    let text = text.to_ascii_lowercase();
    RULES
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|pattern| pattern.matches(&text)))
        .map_or(DEFAULT_SUMMARY, |(label, _)| *label)
}

pub(super) fn from_debug(value: &dyn fmt::Debug) -> &'static str {
    bounded_debug(value, MESSAGE_LIMIT).map_or(DEFAULT_SUMMARY, |text| from_text(&text))
}

pub(super) fn bounded_debug(value: &dyn fmt::Debug, limit: usize) -> Option<String> {
    let mut probe = MessageProbe {
        text: String::new(),
        limit,
    };
    write!(&mut probe, "{value:?}").ok()?;
    Some(probe.text)
}

struct MessageProbe {
    text: String,
    limit: usize,
}

impl Write for MessageProbe {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.text.len().saturating_add(text.len()) > self.limit {
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}
