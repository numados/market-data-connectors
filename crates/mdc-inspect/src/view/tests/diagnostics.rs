//! Public rendering of safe diagnostics, including level styles and context.

use super::*;
#[cfg(feature = "bitfinex")]
use crate::diagnostics::DiagnosticsSubscriber;

#[cfg(feature = "bitfinex")]
#[test]
fn native_bitfinex_information_is_readable_before_source_location() {
    let (ring, subscriber) = DiagnosticsSubscriber::new();
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "mdc_providers::bitfinex::connection",
            connection_id = 7, url = "https://user:synthetic-password@example.invalid",
            "Connected to Bitfinex WebSocket v2");
        let sym = Symbol::from("tBTCUSD");
        tracing::info!(target: "mdc_providers::bitfinex::connection",
            connection_id = 7, chan_id = 1, channel = "book", %sym, "Subscribed to channel");
    });
    let diagnostics = ring.lock().unwrap().snapshot();
    let text = render(
        180,
        40,
        &snapshot(DisplayStatus::Empty, Vec::new()),
        &UiState::new(),
        &diagnostics,
    );
    assert!(
        text.contains("INFO Bitfinex conn=7 symbol=tBTCUSD"),
        "{text}"
    );
    assert!(text.contains("Connected"), "{text}");
    assert!(text.contains("diagnostics.rs:"), "{text}");
    assert!(
        !text.contains("mdc_providers::bitfinex::connection"),
        "{text}"
    );
    assert!(!text.contains("synthetic-password"), "{text}");
    assert!(!text.contains("event="), "{text}");
    let row = text
        .lines()
        .find(|line| line.contains("symbol=tBTCUSD"))
        .unwrap();
    assert!(row.find("conn=7").unwrap() < row.find(" · Subscribed").unwrap());
    assert!(row.find(" · Subscribed").unwrap() < row.find("diagnostics.rs:").unwrap());
    let narrow = render(
        70,
        40,
        &snapshot(DisplayStatus::Empty, Vec::new()),
        &UiState::new(),
        &diagnostics,
    );
    assert!(narrow.contains("Subscribed"), "{narrow}");
}

#[test]
fn forged_record_values_cannot_bypass_redacted_presentation() {
    let diagnostics = DiagnosticsSnapshot {
        records: vec![DiagnosticRecord {
            level: tracing::Level::INFO,
            target: "https://user:synthetic-password@example.invalid".into(),
            name: "event https://user:synthetic-password@example.invalid".into(),
            fields: vec![
                ("event".into(), "synthetic-raw-message".into()),
                ("symbol".into(), "https://u:synthetic-password@x".into()),
                ("provider".into(), "hidden\u{202e}control".into()),
                ("password".into(), "synthetic-password".into()),
            ],
        }],
        evicted: 0,
    };
    let text = render(
        240,
        40,
        &snapshot(DisplayStatus::Empty, Vec::new()),
        &UiState::new(),
        &diagnostics,
    );
    assert!(text.contains("Event details redacted"), "{text}");
    for forbidden in [
        "synthetic-password",
        "synthetic-raw-message",
        "hidden",
        "https://",
    ] {
        assert!(!text.contains(forbidden), "{text}");
    }
}

#[test]
fn compiled_provider_labels_are_reused_in_diagnostic_rows() {
    let diagnostics = DiagnosticsSnapshot {
        records: crate::provider::ProviderKind::ALL
            .iter()
            .map(|kind| DiagnosticRecord {
                level: tracing::Level::INFO,
                target: format!(
                    "mdc_providers::{}::connection",
                    kind.as_str().replace('-', "_")
                ),
                name: "source.rs:1".into(),
                fields: Vec::new(),
            })
            .collect(),
        evicted: 0,
    };
    let text = render(
        240,
        100,
        &snapshot(DisplayStatus::Empty, Vec::new()),
        &UiState::new(),
        &diagnostics,
    );
    for kind in crate::provider::ProviderKind::ALL {
        assert!(text.contains(&format!("INFO {} ·", kind.label())), "{text}");
    }
}

#[test]
fn severity_badges_use_distinct_colours() {
    let cases = [
        (tracing::Level::INFO, Color::Cyan),
        (tracing::Level::WARN, Color::Yellow),
        (tracing::Level::ERROR, Color::Red),
        (tracing::Level::DEBUG, Color::DarkGray),
        (tracing::Level::TRACE, Color::DarkGray),
    ];
    let diagnostics = DiagnosticsSnapshot {
        records: cases
            .iter()
            .map(|(level, _)| DiagnosticRecord {
                level: *level,
                target: "test".into(),
                name: "source.rs:7".into(),
                fields: vec![("event".into(), "Connected".into())],
            })
            .collect(),
        evicted: 0,
    };
    let snap = snapshot(DisplayStatus::Empty, Vec::new());
    let mut terminal = Terminal::new(TestBackend::new(180, 80)).unwrap();
    terminal
        .draw(|frame| draw_main(frame, &snap, &UiState::new(), &diagnostics))
        .unwrap();
    let cells = &terminal.backend().buffer().content;
    for (level, colour) in cases {
        let label = level.to_string();
        let badge = cells
            .windows(label.len())
            .find(|cells| cells.iter().map(|cell| cell.symbol()).collect::<String>() == label)
            .expect("level badge rendered");
        assert!(
            badge.iter().all(|cell| cell.fg == colour),
            "{level}: {badge:?}"
        );
        if matches!(level, tracing::Level::DEBUG | tracing::Level::TRACE) {
            assert!(
                badge
                    .iter()
                    .all(|cell| cell.modifier.contains(Modifier::DIM))
            );
        }
    }
    let source = cells
        .windows("source.rs:7".len())
        .find(|cells| cells.iter().map(|cell| cell.symbol()).collect::<String>() == "source.rs:7")
        .unwrap();
    assert!(
        source
            .iter()
            .all(|cell| cell.modifier.contains(Modifier::DIM))
    );
}
