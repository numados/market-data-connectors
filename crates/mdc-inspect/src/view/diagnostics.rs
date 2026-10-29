//! Compact diagnostics with bounded visible-row formatting.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem};

use crate::diagnostics::{DiagnosticRecord, DiagnosticsSnapshot, safe_context_value};
use crate::display::UiState;
use crate::provider::ProviderKind;

use super::focus_label;

pub(super) fn draw_diagnostics(
    frame: &mut Frame<'_>,
    area: Rect,
    diagnostics: &DiagnosticsSnapshot,
    ui: &UiState,
) {
    let items: Vec<ListItem<'_>> = if diagnostics.records.is_empty() && diagnostics.evicted == 0 {
        vec![ListItem::new("no diagnostics")]
    } else {
        std::iter::once(None)
            .filter(|_| diagnostics.evicted > 0)
            .chain(diagnostics.records.iter().map(Some))
            .skip(ui.diagnostics_scroll)
            .take(usize::from(area.height.saturating_sub(2)))
            .map(|record| match record {
                Some(record) => {
                    ListItem::new(row(record, usize::from(area.width.saturating_sub(2))))
                }
                None => ListItem::new(format!(
                    "... {} older diagnostics evicted",
                    diagnostics.evicted
                )),
            })
            .collect()
    };
    let list = List::new(items).block(
        Block::new()
            .borders(Borders::ALL)
            .title(format!("Diagnostics (focus {})", focus_label(ui.focus))),
    );
    frame.render_widget(list, area);
}

fn row(record: &DiagnosticRecord, width: usize) -> Line<'_> {
    let level = record.level.to_string();
    let provider = provider_name(&record.target);
    let summary = record.summary();
    // Context is optional when space is tight; never let it hide the event.
    let mut context_budget =
        width.saturating_sub(level.len() + 1 + provider.len() + 3 + summary.len());
    let mut spans = vec![
        Span::styled(level, level_style(record.level)),
        Span::raw(format!(" {provider}")),
    ];
    for (name, value) in record.context_fields() {
        let name = match name {
            "connection" | "connection_id" => "conn",
            name => name,
        };
        let field_width = name.len() + value.len() + 2;
        if field_width <= context_budget {
            spans.push(Span::raw(format!(" {name}={value}")));
            context_budget -= field_width;
        }
    }
    spans.push(Span::raw(format!(" · {summary} · ")));
    spans.push(Span::styled(
        source_name(&record.name),
        Style::new().fg(Color::DarkGray).add_modifier(Modifier::DIM),
    ));
    Line::from(spans)
}

fn level_style(level: tracing::Level) -> Style {
    match level {
        tracing::Level::ERROR => Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
        tracing::Level::WARN => Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        tracing::Level::INFO => Style::new().fg(Color::Cyan),
        tracing::Level::DEBUG | tracing::Level::TRACE => {
            Style::new().fg(Color::DarkGray).add_modifier(Modifier::DIM)
        }
    }
}

fn provider_name(target: &str) -> &str {
    let module = target
        .strip_prefix("mdc_providers::")
        .and_then(|target| target.split("::").next())
        .unwrap_or_else(|| target.rsplit("::").next().unwrap_or("diagnostics"));
    let canonical = module.replace('_', "-");
    if let Some(provider) = ProviderKind::ALL
        .iter()
        .find(|provider| provider.as_str() == canonical)
    {
        return provider.label();
    }
    if safe_context_value(module) {
        module
    } else {
        "diagnostics"
    }
}

fn source_name(name: &str) -> &str {
    let name = name.strip_prefix("event ").unwrap_or(name);
    let name = std::path::Path::new(name)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("source");
    if safe_context_value(name) {
        name
    } else {
        "source"
    }
}
