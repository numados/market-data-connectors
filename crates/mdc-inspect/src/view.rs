//! Pure Ratatui layout and widgets over display/UI state (PRD 31 §3.6).
//!
//! The view reads state; it never controls provider tasks. Every status is
//! rendered as a text label in addition to any color, retained historical
//! values carry an explicit `HISTORICAL` label with dimmed styling, and
//! narrow terminals degrade to stacked panes without panicking.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs};

use crate::diagnostics::DiagnosticsSnapshot;
use crate::display::{
    DisplaySnapshot, DisplayStatus, DisplayedQuote, Focus, SymbolDisplay, UiState,
};
use crate::setup::SetupState;

/// Minimum width below which panes stack vertically.
const NARROW_WIDTH: u16 = 70;

/// Draws the guided-configuration screen.
pub fn draw_setup(frame: &mut Frame<'_>, setup: &SetupState) {
    let area = frame.area();
    let rows = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(3),
    ])
    .split(area);

    let provider = setup
        .selected_provider()
        .map(|kind| kind.label())
        .unwrap_or("<no provider features compiled>");
    let picker = Paragraph::new(format!(
        "Provider (Up/Down): {provider}\nCompiled providers: {}",
        setup.provider_count()
    ))
    .block(Block::new().borders(Borders::ALL).title("Select provider"));
    frame.render_widget(picker, rows[0]);

    let input_line = format!(
        "Symbol: {}▏  (Enter adds, Delete removes last)",
        setup.input()
    );
    let input = Paragraph::new(input_line).block(
        Block::new()
            .borders(Borders::ALL)
            .title("Symbols — text entry mode"),
    );
    frame.render_widget(input, rows[1]);

    let items: Vec<Line<'_>> = setup
        .symbols()
        .iter()
        .map(|symbol| Line::from(format!("  {symbol}")))
        .collect();
    let list = List::new(items).block(
        Block::new()
            .borders(Borders::ALL)
            .title(format!("Configured symbols ({})", setup.symbols().len())),
    );
    frame.render_widget(list, rows[2]);

    let footer = match (setup.error(), setup.selected_provider()) {
        (Some(error), _) => Line::from(Span::styled(
            format!("error: {error}"),
            Style::new().fg(Color::Red),
        )),
        (None, Some(kind)) => {
            let bounds = kind.session_bounds();
            let hint = match kind.as_str() {
                "frankfurter" => {
                    "first symbol is base currency, then 1–100 quote currencies (EUR, USD)"
                }
                "vwd" => "six uppercase letters per direct FX pair (for example EURUSD)",
                _ => "provider-native identifiers",
            };
            Line::from(format!(
                "Enter with empty input starts the session; {} accepts up to {} symbols; {hint}",
                kind.label(),
                bounds.max_symbols
            ))
        }
        (None, None) => Line::from("no provider compiled; rebuild with --features <provider>"),
    };
    frame.render_widget(Paragraph::new(footer), rows[3]);
}

/// Draws the main inspection view.
pub fn draw_main(
    frame: &mut Frame<'_>,
    snapshot: &DisplaySnapshot,
    ui: &UiState,
    diagnostics: &DiagnosticsSnapshot,
) {
    let area = frame.area();
    let rows = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length(1),
    ])
    .split(area);

    draw_header(frame, rows[0], snapshot);
    draw_symbol_tabs(frame, rows[1], snapshot, ui);

    let selected = selected_symbol(snapshot, ui);
    let is_rate = snapshot.symbols.iter().any(|symbol| symbol.rate.is_some());
    if area.width < NARROW_WIDTH {
        let stacked = Layout::vertical([
            Constraint::Length(6),
            Constraint::Percentage(50),
            Constraint::Percentage(50),
        ])
        .split(rows[2]);
        if is_rate {
            draw_rate(frame, stacked[0], selected);
        } else {
            draw_quote(frame, stacked[0], selected);
        }
        draw_alerts(frame, stacked[1], snapshot, ui);
        draw_diagnostics(frame, stacked[2], diagnostics, ui);
    } else {
        let columns = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(rows[2]);
        if is_rate {
            draw_rate(frame, columns[0], selected);
        } else {
            draw_book(frame, columns[0], selected, ui);
        }
        let right = Layout::vertical([
            Constraint::Length(6),
            Constraint::Percentage(45),
            Constraint::Min(4),
        ])
        .split(columns[1]);
        if !is_rate {
            draw_quote(frame, right[0], selected);
        }
        draw_alerts(frame, right[1], snapshot, ui);
        draw_diagnostics(frame, right[2], diagnostics, ui);
    }

    draw_hints(frame, rows[3], snapshot);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, snapshot: &DisplaySnapshot) {
    let mut line = vec![
        Span::styled("mdc-inspect", Style::new().add_modifier(Modifier::BOLD)),
        Span::raw(format!("  provider: {}", snapshot.provider)),
        Span::raw(format!("  session: {}", snapshot.session.label())),
    ];
    if capability_label(snapshot.provider) == "unknown" {
        line.push(Span::raw("  capabilities: unknown"));
    }
    if let Some(failure) = &snapshot.failure {
        line.push(Span::styled(
            format!("  failure: {failure}"),
            Style::new().fg(Color::Red),
        ));
    }
    frame.render_widget(
        Paragraph::new(Line::from(line)).block(Block::new().borders(Borders::ALL)),
        area,
    );
}

fn draw_symbol_tabs(frame: &mut Frame<'_>, area: Rect, snapshot: &DisplaySnapshot, ui: &UiState) {
    if snapshot.symbols.is_empty() {
        return;
    }
    let titles: Vec<&str> = snapshot
        .symbols
        .iter()
        .map(|symbol| symbol.symbol.as_str())
        .collect();
    let selected = ui.selected_symbol.min(snapshot.symbols.len() - 1);
    let tabs = Tabs::new(titles)
        .select(selected)
        .highlight_style(Style::new().add_modifier(Modifier::REVERSED));
    frame.render_widget(tabs, area);
}

fn selected_symbol<'a>(snapshot: &'a DisplaySnapshot, ui: &UiState) -> Option<&'a SymbolDisplay> {
    if snapshot.symbols.is_empty() {
        return None;
    }
    let index = ui.selected_symbol.min(snapshot.symbols.len() - 1);
    snapshot.symbols.get(index)
}

fn draw_quote(frame: &mut Frame<'_>, area: Rect, symbol: Option<&SymbolDisplay>) {
    let lines = match symbol {
        None => vec![Line::from("no symbol selected")],
        Some(symbol) => {
            let mut lines = vec![Line::from(vec![
                Span::raw("status: "),
                Span::styled(symbol.status.label(), quote_style(symbol.status)),
            ])];
            if let Some(reason) = &symbol.unavailability {
                lines.push(Line::from(Span::styled(
                    format!("reason: {reason}"),
                    Style::new().fg(Color::Red),
                )));
            }
            match &symbol.quote {
                Some(quote) => lines.push(quote_line(quote)),
                None => lines.push(Line::from("no quote observed yet")),
            }
            lines
        }
    };
    frame.render_widget(
        Paragraph::new(lines).block(Block::new().borders(Borders::ALL).title("Top of book")),
        area,
    );
}

fn quote_line(quote: &DisplayedQuote) -> Line<'_> {
    let q = &quote.quote;
    let spread = match q.spread {
        Some(value) => value.to_string(),
        None => "n/a".to_string(),
    };
    if quote.historical {
        Line::from(vec![
            Span::styled("[HISTORICAL] ", Style::new().fg(Color::Yellow)),
            Span::styled(
                format!(
                    "bid {} x {}  ask {} x {}  spread {}",
                    q.bid, q.bid_size, q.ask, q.ask_size, spread
                ),
                Style::new().fg(Color::DarkGray).add_modifier(Modifier::DIM),
            ),
        ])
    } else {
        Line::from(format!(
            "bid {} x {}  ask {} x {}  spread {}",
            q.bid, q.bid_size, q.ask, q.ask_size, spread
        ))
    }
}

const fn quote_style(status: DisplayStatus) -> Style {
    match status {
        DisplayStatus::InSync => Style::new().fg(Color::Green),
        DisplayStatus::Unavailable | DisplayStatus::Failed => Style::new().fg(Color::Red),
        DisplayStatus::Paused => Style::new().fg(Color::Yellow),
        _ => Style::new(),
    }
}

fn draw_rate(frame: &mut Frame<'_>, area: Rect, symbol: Option<&SymbolDisplay>) {
    let lines = match symbol.and_then(|symbol| symbol.rate.as_ref()) {
        Some(displayed) => {
            let rate = &displayed.rate;
            let prefix = if displayed.historical {
                "[HISTORICAL] "
            } else {
                ""
            };
            vec![
                Line::from(format!(
                    "status: {}",
                    symbol.map(|s| s.status.label()).unwrap_or("empty")
                )),
                Line::from(format!("{prefix}midpoint {}", rate.midpoint)),
                Line::from(format!("observation date UTC {}", rate.observation_date.0)),
                Line::from(format!("receipt age {:?}", rate.receipt_age)),
            ]
        }
        None => vec![Line::from("no reference rate observed yet")],
    };
    frame.render_widget(
        Paragraph::new(lines).block(Block::new().borders(Borders::ALL).title("Reference rate")),
        area,
    );
}

fn draw_book(frame: &mut Frame<'_>, area: Rect, symbol: Option<&SymbolDisplay>, ui: &UiState) {
    let items: Vec<ListItem<'_>> = match symbol {
        None => vec![ListItem::new("no symbol selected")],
        Some(symbol) => {
            let mut items = Vec::new();
            for level in symbol.bids.iter() {
                items.push(ListItem::new(format!(
                    "bid  {:>14} x {}",
                    level.price, level.size
                )));
            }
            for level in symbol.asks.iter() {
                items.push(ListItem::new(format!(
                    "ask  {:>14} x {}",
                    level.price, level.size
                )));
            }
            if items.is_empty() {
                items.push(ListItem::new("book empty"));
            }
            items
        }
    };
    let offset = ui.book_scroll;
    let title = if symbol.map(|s| s.symbol.as_str()).unwrap_or("").len() == 6 {
        "Level 2 — synthetic one-level indicative (not executable liquidity)".to_string()
    } else {
        format!(
            "Level 2 (scroll {offset}, focus {})   ",
            focus_label(ui.focus)
        )
    };
    let list = List::new(items).block(Block::new().borders(Borders::ALL).title(title));
    frame.render_widget(list, area);
}

fn draw_alerts(frame: &mut Frame<'_>, area: Rect, snapshot: &DisplaySnapshot, ui: &UiState) {
    let mut lines = Vec::new();
    if snapshot.alerts_evicted > 0 {
        lines.push(ListItem::new(format!(
            "... {} older alerts evicted",
            snapshot.alerts_evicted
        )));
    }
    for record in &snapshot.alerts {
        let symbol = record.symbol.as_ref().map(|s| s.as_str()).unwrap_or("-");
        lines.push(ListItem::new(format!(
            "{symbol}: {} ({})",
            record.summary, record.received_ts.0
        )));
    }
    if lines.is_empty() {
        lines.push(ListItem::new("no integrity alerts"));
    }
    let list = List::new(lines).block(Block::new().borders(Borders::ALL).title(format!(
        "Integrity alerts (focus {})",
        focus_label(ui.focus)
    )));
    frame.render_widget(list, area);
}

fn draw_diagnostics(
    frame: &mut Frame<'_>,
    area: Rect,
    diagnostics: &DiagnosticsSnapshot,
    ui: &UiState,
) {
    let mut lines = Vec::new();
    if diagnostics.evicted > 0 {
        lines.push(ListItem::new(format!(
            "... {} older diagnostics evicted",
            diagnostics.evicted
        )));
    }
    for record in &diagnostics.records {
        let fields = record
            .fields
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(" ");
        lines.push(ListItem::new(format!(
            "{} {} {} {fields}",
            record.level, record.target, record.name
        )));
    }
    if lines.is_empty() {
        lines.push(ListItem::new("no diagnostics"));
    }
    let list = List::new(lines).block(
        Block::new()
            .borders(Borders::ALL)
            .title(format!("Diagnostics (focus {})", focus_label(ui.focus))),
    );
    frame.render_widget(list, area);
}

const fn focus_label(focus: Focus) -> &'static str {
    match focus {
        Focus::Symbols => "tabs",
        Focus::Book => "book",
        Focus::Alerts => "alerts",
        Focus::Diagnostics => "diagnostics",
    }
}

fn capability_label(provider: &str) -> &'static str {
    match provider {
        "VWD/Infront" | "Coinbase Advanced" => "unknown",
        _ => "declared",
    }
}

fn draw_hints(frame: &mut Frame<'_>, area: Rect, snapshot: &DisplaySnapshot) {
    let hints = match snapshot.session {
        DisplayStatus::Empty => "Enter: start | Tab: focus | ?: help | q: quit | Ctrl-C: quit",
        DisplayStatus::Stopped => "c: configure | Enter: start | q: quit | Ctrl-C: quit",
        _ => "s: stop | Tab: focus | Arrows: scroll | ?: help | q: quit | Ctrl-C: quit",
    };
    frame.render_widget(Paragraph::new(hints), area);
}

/// Draws the help overlay listing exactly the supported controls.
pub fn draw_help(frame: &mut Frame<'_>) {
    let area = frame.area();
    let width = area.width.min(60);
    let height = area.height.min(16);
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    };
    let text = vec![
        Line::from("mdc-inspect controls"),
        Line::from(""),
        Line::from("Enter        start session / add symbol"),
        Line::from("s            stop active session"),
        Line::from("c            return to configuration"),
        Line::from("Tab/BackTab  move pane focus"),
        Line::from("Left/Right   change symbol tab"),
        Line::from("Up/Down/PgUp/PgDn  scroll or select"),
        Line::from("?            toggle this help"),
        Line::from("q            quit"),
        Line::from("Ctrl-C       quit from any state"),
    ];
    frame.render_widget(
        Paragraph::new(text).block(Block::new().borders(Borders::ALL).title("Help")),
        popup,
    );
}

#[cfg(test)]
mod tests;
