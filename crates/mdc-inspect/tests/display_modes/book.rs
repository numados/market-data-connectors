use super::*;
use mdc_core::{IntegrityAlert, L2Delta, L2Snapshot};
use mdc_inspect::config::SessionConfig;
use mdc_inspect::display::{DisplayMode, LevelView};
use mdc_inspect::provider::ProviderKind;
use mdc_inspect::reducer::Reducer;

fn book_config() -> SessionConfig {
    let provider = ProviderKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.presentation().mode != DisplayMode::ReferenceRate)
        .expect("a book provider is compiled");
    let symbol = if provider.presentation().mode == DisplayMode::SyntheticBook {
        "EURUSD"
    } else {
        "BTCUSDT"
    };
    SessionConfig::new(provider, &[symbol.to_string()]).expect("valid book config")
}

#[test]
fn l2_reducer_reaches_exact_book_quote_and_display_after_delta() {
    let config = book_config();
    let provider = config.provider;
    let symbol = config.symbols[0].clone();
    let mut reducer = Reducer::new(&config, provider.session_bounds());

    reducer.apply_event(&event(
        symbol.as_str(),
        MarketDataKind::L2Snapshot(L2Snapshot::new(
            vec![PriceLevel::new(decimal("100.123400"), decimal("2.500"))],
            vec![PriceLevel::new(decimal("100.456700"), decimal("3.750"))],
            Some(7),
        )),
        Some(1_000),
        1_005,
        1,
    ));
    let loading = reducer.snapshot();
    assert_eq!(loading.session, DisplayStatus::Loading);
    assert_eq!(loading.symbols[0].status, DisplayStatus::Loading);
    assert!(loading.symbols[0].quote.is_none());

    reducer.apply_event(&event(
        symbol.as_str(),
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(7),
        }),
        None,
        1_010,
        2,
    ));
    assert_eq!(reducer.snapshot().session, DisplayStatus::InSync);

    reducer.apply_event(&event(
        symbol.as_str(),
        MarketDataKind::L2Delta(L2Delta::new(
            vec![
                PriceLevel::new(decimal("100.123400"), Decimal::ZERO),
                PriceLevel::new(decimal("100.234500"), decimal("2.750")),
            ],
            vec![
                PriceLevel::new(decimal("100.456700"), Decimal::ZERO),
                PriceLevel::new(decimal("100.567800"), decimal("3.250")),
            ],
            None,
        )),
        Some(1_020),
        1_025,
        3,
    ));

    let synchronized = reducer.snapshot();
    let displayed = &synchronized.symbols[0];
    assert_eq!(synchronized.presentation.mode, provider.presentation().mode);
    assert_eq!(synchronized.session, DisplayStatus::InSync);
    assert_eq!(displayed.symbol, symbol);
    assert_eq!(displayed.status, DisplayStatus::InSync);
    assert_eq!(
        displayed.bids,
        vec![LevelView {
            price: decimal("100.234500"),
            size: decimal("2.750"),
        }]
    );
    assert_eq!(
        displayed.asks,
        vec![LevelView {
            price: decimal("100.567800"),
            size: decimal("3.250"),
        }]
    );
    let quote = displayed.quote.as_ref().expect("synchronized quote");
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, decimal("100.234500"));
    assert_eq!(quote.quote.ask, decimal("100.567800"));
    assert_eq!(quote.quote.bid_size, decimal("2.750"));
    assert_eq!(quote.quote.ask_size, decimal("3.250"));
    assert_eq!(quote.quote.spread, Some(decimal("0.333300")));

    let unsolicited = BookTicker::new(
        PriceLevel::new(decimal("1.000"), decimal("9.000")),
        PriceLevel::new(decimal("2.000"), decimal("9.000")),
        Some(99),
    );
    reducer.apply_event(&event(
        symbol.as_str(),
        MarketDataKind::BookTicker(unsolicited),
        Some(1_030),
        1_035,
        4,
    ));
    let after_ticker = reducer.snapshot();
    assert_eq!(after_ticker.symbols[0], synchronized.symbols[0]);

    let text = render(&after_ticker, UnixNanos(1_100));
    assert!(text.contains("Level 2"), "{text}");
    if synchronized.presentation.mode == DisplayMode::SyntheticBook {
        assert!(text.contains("synthetic indicative"), "{text}");
    }
    assert!(
        text.contains("bid 100.234500 x 2.750  ask 100.567800 x 3.250  spread 0.333300"),
        "{text}"
    );
    assert!(text.contains("100.234500"), "{text}");
    assert!(text.contains("100.567800"), "{text}");
}
