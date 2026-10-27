use super::*;

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[cfg(any(feature = "binance", feature = "bitfinex", feature = "lmax"))]
fn numbered_symbols(count: usize, prefix: &str) -> Vec<String> {
    (0..count).map(|index| format!("{prefix}{index}")).collect()
}

#[cfg(feature = "frankfurter")]
fn quote_codes(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| {
            format!(
                "A{}{}",
                char::from(b'A' + (index / 26) as u8),
                char::from(b'A' + (index % 26) as u8)
            )
        })
        .collect()
}

#[cfg(feature = "binance")]
#[test]
fn accepts_distinct_symbols_and_preserves_spelling() {
    let config = SessionConfig::new(ProviderKind::Binance, &strings(&["BTCUSDT", " ETHUSDT "]))
        .expect("valid configuration");
    assert_eq!(
        config.symbols,
        vec![Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")]
    );
}

#[cfg(feature = "binance")]
#[test]
fn validation_errors_identify_context_and_escape_control_values() {
    let cases = [
        (
            "empty list",
            Vec::new(),
            ["Binance", "symbols", "<empty>", "provider-native symbol"],
        ),
        (
            "blank entry",
            strings(&["  "]),
            ["Binance", "symbols", "  ", "empty after trimming"],
        ),
        (
            "control entry",
            strings(&["BTC\tUSDT"]),
            [
                "Binance",
                "symbols",
                "BTC\\tUSDT",
                "terminal control characters",
            ],
        ),
    ];
    for (name, raw, fragments) in cases {
        let error = SessionConfig::new(ProviderKind::Binance, &raw)
            .expect_err(name)
            .to_string();
        for fragment in fragments {
            assert!(error.contains(fragment), "{name}: {error}");
        }
        assert!(!error.chars().any(char::is_control), "{name}: {error:?}");
    }
}

#[cfg(feature = "binance")]
#[test]
fn duplicate_error_is_provider_aware_and_not_reference_rate_worded() {
    let duplicated = SessionConfig::new(ProviderKind::Binance, &strings(&["BTCUSDT", " BTCUSDT "]));
    let error = duplicated
        .expect_err("duplicate symbols must be rejected")
        .to_string();
    assert_eq!(
        error,
        "invalid configuration: Binance Spot duplicate symbol 'BTCUSDT' after trimming"
    );
    assert!(!error.contains("Frankfurter"));
    assert!(!error.contains("quotes"));
    assert!(!error.contains("base"));
}

#[cfg(feature = "binance")]
#[test]
fn enforces_provider_symbol_cap_with_bound_context() {
    let many = numbered_symbols(26, "SYM");
    let error = SessionConfig::new(ProviderKind::Binance, &many)
        .expect_err("26 symbols must exceed the Binance cap")
        .to_string();
    assert!(error.contains("Binance"));
    assert!(error.contains("field 'symbols'"));
    assert!(error.contains("at most 25"));
    assert!(error.contains("got 26"));
}

#[cfg(feature = "vwd")]
#[test]
fn vwd_native_syntax_boundaries_are_table_driven() {
    let cases = [
        ("lowercase", strings(&["eurusd"]), false),
        ("five bytes", strings(&["EURUS"]), false),
        ("six uppercase letters", strings(&["EURUSD"]), true),
    ];
    for (name, raw, expected_ok) in cases {
        let result = SessionConfig::new(ProviderKind::Vwd, &raw);
        assert_eq!(result.is_ok(), expected_ok, "{name}");
        if !expected_ok {
            let error = result.expect_err(name).to_string();
            assert!(error.contains("VWD"), "{name}: {error}");
            assert!(error.contains("field 'symbols'"), "{name}: {error}");
            assert!(
                error.contains("expected 6 uppercase ASCII letters"),
                "{name}: {error}"
            );
            assert!(error.contains(raw[0].trim()), "{name}: {error}");
        }
    }
}

#[cfg(feature = "frankfurter")]
#[test]
fn frankfurter_configuration_boundaries_are_table_driven() {
    let mut hundred_quotes = vec!["EUR".to_string()];
    hundred_quotes.extend(quote_codes(100));
    let mut one_hundred_one_quotes = vec!["EUR".to_string()];
    one_hundred_one_quotes.extend(quote_codes(101));
    let cases = [
        ("invalid length", strings(&["EU", "USD"]), false),
        ("invalid case", strings(&["EUR", "usd"]), false),
        ("base equals quote", strings(&["EUR", "EUR"]), false),
        ("100 quotes", hundred_quotes, true),
        ("101 quotes", one_hundred_one_quotes, false),
    ];
    for (name, raw, expected_ok) in cases {
        let result = SessionConfig::new(ProviderKind::Frankfurter, &raw);
        assert_eq!(result.is_ok(), expected_ok, "{name}: {result:?}");
        if name == "100 quotes" {
            assert_eq!(result.expect("100 quote currencies").symbols.len(), 100);
        } else if !expected_ok {
            let error = result.expect_err(name).to_string();
            assert!(error.contains("Frankfurter"), "{name}: {error}");
            match name {
                "invalid length" => {
                    assert!(error.contains("field 'base'"), "{name}: {error}");
                    assert!(error.contains("EU"), "{name}: {error}");
                    assert!(
                        error.contains("3 uppercase ASCII letters"),
                        "{name}: {error}"
                    );
                }
                "invalid case" => {
                    assert!(error.contains("field 'quote'"), "{name}: {error}");
                    assert!(error.contains("usd"), "{name}: {error}");
                }
                "base equals quote" => {
                    assert!(error.contains("field 'quote'"), "{name}: {error}");
                    assert!(error.contains("EUR"), "{name}: {error}");
                    assert!(error.contains("must differ from base"), "{name}: {error}");
                }
                "101 quotes" => {
                    assert!(error.contains("field 'quotes'"), "{name}: {error}");
                    assert!(error.contains("at most 100"), "{name}: {error}");
                    assert!(error.contains("got 101"), "{name}: {error}");
                }
                _ => unreachable!("unexpected Frankfurter case: {name}"),
            }
        }
    }
}

#[cfg(feature = "frankfurter")]
#[test]
fn entries_reconstruct_base_before_quotes() {
    let config = SessionConfig::new(
        ProviderKind::Frankfurter,
        &strings(&[" EUR ", " USD ", "GBP"]),
    )
    .expect("valid reference-rate configuration");
    let entries: Vec<&str> = config.entries().map(|entry| entry.as_str()).collect();
    assert_eq!(entries, ["EUR", "USD", "GBP"]);
}

#[cfg(feature = "bitfinex")]
#[test]
fn bitfinex_symbol_boundaries_are_table_driven() {
    let cases = [
        ("12 symbols", numbered_symbols(12, "tBTCUSD"), true),
        ("13 symbols", numbered_symbols(13, "tBTCUSD"), false),
    ];
    for (name, raw, expected_ok) in cases {
        let result = SessionConfig::new(ProviderKind::Bitfinex, &raw);
        assert_eq!(result.is_ok(), expected_ok, "{name}");
        if !expected_ok {
            let error = result.expect_err(name).to_string();
            assert!(error.contains("Bitfinex"), "{name}: {error}");
            assert!(error.contains("field 'symbols'"), "{name}: {error}");
            assert!(error.contains("at most 12"), "{name}: {error}");
            assert!(error.contains("got 13"), "{name}: {error}");
        }
    }
}

#[cfg(feature = "lmax")]
#[test]
fn lmax_symbol_boundaries_are_table_driven() {
    let cases = [
        ("26 symbols", numbered_symbols(26, "btc-usd-"), true),
        ("27 symbols", numbered_symbols(27, "btc-usd-"), false),
    ];
    for (name, raw, expected_ok) in cases {
        let result = SessionConfig::new(ProviderKind::Lmax, &raw);
        assert_eq!(result.is_ok(), expected_ok, "{name}");
        if !expected_ok {
            let error = result.expect_err(name).to_string();
            assert!(error.contains("LMAX"), "{name}: {error}");
            assert!(error.contains("field 'symbols'"), "{name}: {error}");
            assert!(error.contains("at most 26"), "{name}: {error}");
            assert!(error.contains("got 27"), "{name}: {error}");
        }
    }
}

#[test]
fn test_identity_entries_are_trimmed_without_a_venue_feature() {
    let config = SessionConfig::new(ProviderKind::Test, &strings(&[" first ", "second"]))
        .expect("test identity uses structural validation");
    let entries: Vec<&str> = config.entries().map(|entry| entry.as_str()).collect();
    assert_eq!(entries, ["first", "second"]);
}
