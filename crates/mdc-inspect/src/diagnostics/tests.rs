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

#[test]
fn native_messages_become_only_approved_summaries() {
    let cases = [
        ("binance", "Connecting to Binance WebSocket", "Connecting"),
        ("kraken", "Connecting to Kraken WebSocket v2", "Connecting"),
        (
            "coinbase",
            "Connecting to Coinbase Exchange WebSocket",
            "Connecting",
        ),
        (
            "bitfinex",
            "Connected to Bitfinex WebSocket v2",
            "Connected",
        ),
        ("gateio", "Connecting to Gate.io WebSocket", "Connecting"),
        ("bitstamp", "Connecting to Bitstamp WebSocket", "Connecting"),
        ("huobi", "Connecting to Huobi WebSocket", "Connecting"),
        (
            "coinbase-advanced",
            "Connecting to Coinbase Advanced Trade WebSocket",
            "Connecting",
        ),
        ("lmax", "Connecting to LMAX Digital WebSocket", "Connecting"),
        (
            "frankfurter",
            "failed to initialize REST client",
            "Client initialization failed",
        ),
        (
            "vwd",
            "failed to initialize REST client",
            "Client initialization failed",
        ),
        (
            "bitfinex",
            "Bitfinex server info",
            "Server information received",
        ),
        (
            "bitfinex",
            "Bitfinex conf response",
            "Configuration response received",
        ),
        ("bitfinex", "Subscribed to channel", "Subscribed"),
        (
            "bitfinex",
            "Subscribed to unknown symbol; unsubscribing",
            "Subscription rejected",
        ),
        (
            "coinbase",
            "Received error frame from Coinbase",
            "Provider error",
        ),
        (
            "bitfinex",
            "Failed to connect to Bitfinex WebSocket",
            "Connection failed",
        ),
        ("binance", "Reconnecting with backoff", "Reconnecting"),
        (
            "binance",
            "REST depth snapshot fetch failed",
            "Snapshot request failed",
        ),
        (
            "huobi",
            "Gzip decompression failed; skipping frame",
            "Invalid data received",
        ),
        (
            "lmax",
            "LMAX TOO_MANY_REQUESTS rate limit hit; backing off control sends",
            "Rate limited",
        ),
        (
            "bitfinex",
            "Bitfinex connection worker cancelled",
            "Stopped",
        ),
        ("bitfinex", "Pausing Bitfinex connector", "Paused"),
        ("bitfinex", "Resuming Bitfinex connector", "Resuming"),
        ("bitfinex", "Unsubscribed from channel", "Unsubscribed"),
        (
            "bitfinex",
            "Bitfinex WebSocket closed by remote server",
            "Connection closed",
        ),
        (
            "test",
            "synthetic-password https://user:pass@example.invalid/raw",
            "Event details redacted",
        ),
    ];
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        for (provider, message, _) in cases {
            tracing::info!(provider, message = message);
        }
        let oversized = format!("Connected to {}", "synthetic-password".repeat(1000));
        tracing::info!(message = oversized.as_str());
    });
    let snapshot = ring.lock().unwrap().snapshot();
    assert_eq!(snapshot.records.len(), cases.len() + 1);
    for (record, (_, _, expected)) in snapshot.records.iter().zip(cases) {
        let summary = record
            .fields
            .iter()
            .find(|(name, _)| name == "event")
            .map(|(_, value)| value.as_str());
        assert_eq!(summary, Some(expected));
        assert_eq!(record.fields.last().unwrap().0, "event");
    }
    assert_eq!(
        snapshot.records.last().unwrap().fields.last().unwrap().1,
        "Event details redacted"
    );
    let text = format!("{snapshot:?}");
    assert!(!text.contains("synthetic-password"));
    assert!(!text.contains("user:pass"));
}

#[test]
fn native_context_is_bounded_without_control_or_url_values() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        let sym = mdc_core::Symbol::from("tBTCUSD");
        tracing::info!(target: "mdc_providers::bitfinex::connection",
            connection_id = 7, %sym, symbol = "hidden\u{202e}control",
            provider = "https://user:synthetic-password@example.invalid",
            "Connected to Bitfinex WebSocket v2");
        let oversized = "synthetic-context-marker".repeat(1000);
        tracing::info!(connection_id = %oversized, "Bitfinex server info");
    });
    let snapshot = ring.lock().unwrap().snapshot();
    let fields = &snapshot.records[0].fields;
    assert!(fields.contains(&("symbol".into(), "tBTCUSD".into())));
    assert!(fields.contains(&("connection_id".into(), "7".into())));
    let text = format!("{snapshot:?}");
    for forbidden in ["hidden", "synthetic-password", "synthetic-context-marker"] {
        assert!(!text.contains(forbidden), "{text}");
    }
}

#[test]
fn duplicate_server_message_cannot_forge_or_leak_event_details() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        let message = "Connected to synthetic-password";
        tracing::warn!(target: "mdc_providers::coinbase::connection", connection_id = 1,
            %message, event = "forged-secret-marker", error = "error-secret-marker",
            "Received error frame from Coinbase");
    });
    let snapshot = ring.lock().unwrap().snapshot();
    let record = &snapshot.records[0];
    let summaries: Vec<_> = record
        .fields
        .iter()
        .filter(|(name, _)| name == "event")
        .collect();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].1, "Provider error");
    assert_eq!(record.fields.last().unwrap().0, "event");
    let text = format!("{snapshot:?}");
    for forbidden in [
        "synthetic-password",
        "forged-secret-marker",
        "error-secret-marker",
    ] {
        assert!(!text.contains(forbidden), "{text}");
    }
}

#[test]
fn oversized_formatted_message_is_not_partially_classified() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        let payload = "synthetic-payload-marker".repeat(1000);
        tracing::info!(
            url = "https://user:pass@example.invalid",
            "Connected to {payload}"
        );
    });
    let snapshot = ring.lock().unwrap().snapshot();
    assert_eq!(
        snapshot.records[0].fields.last().unwrap().1,
        "Event details redacted"
    );
    assert!(!format!("{snapshot:?}").contains("synthetic-payload-marker"));
}

#[test]
fn trace_flood_does_not_evict_operator_information() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("Connected to Bitfinex WebSocket v2");
        for _ in 0..1000 {
            tracing::trace!("Checksum verified valid");
            tracing::debug!("Protocol detail");
        }
    });
    let snapshot = ring.lock().unwrap().snapshot();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.evicted, 0);
}

#[test]
fn explicit_trace_capture_does_not_disable_redaction() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    let subscriber = subscriber.with_max_level(Level::TRACE);
    tracing::subscriber::with_default(subscriber, || {
        tracing::trace!(
            password = "synthetic-password-marker",
            raw = "synthetic-payload",
            "Checksum verified valid"
        );
        tracing::debug!(
            error = "synthetic-error-marker",
            "Connected to Bitfinex WebSocket v2"
        );
    });
    let snapshot = ring.lock().unwrap().snapshot();
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(snapshot.records[0].level, Level::TRACE);
    assert_eq!(snapshot.records[1].level, Level::DEBUG);
    let text = format!("{snapshot:?}");
    for forbidden in [
        "synthetic-password-marker",
        "synthetic-payload",
        "synthetic-error-marker",
    ] {
        assert!(!text.contains(forbidden), "{text}");
    }
}

#[test]
fn default_level_excludes_debug_and_trace() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        tracing::trace!("Checksum verified valid");
        tracing::debug!("Protocol detail");
        tracing::info!("Connected to Bitfinex WebSocket v2");
        tracing::warn!("Bitfinex error notification");
        tracing::error!("Failed to connect to Bitfinex WebSocket");
    });
    let snapshot = ring.lock().unwrap().snapshot();
    let levels: Vec<_> = snapshot.records.iter().map(|record| record.level).collect();
    assert_eq!(levels, vec![Level::INFO, Level::WARN, Level::ERROR]);
    assert_eq!(snapshot.evicted, 0);
}
