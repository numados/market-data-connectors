#![cfg(feature = "lmax")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 3 Property tests for LMAX fast key-scan frame dispatcher.

use mdc_providers::lmax::raw::{LmaxFrameKind, peek_frame_kind};
use proptest::prelude::*;
use serde_json::Value;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn property_peek_frame_kind_matches_json_reference(
        target_kind in prop::sample::select(vec![
            LmaxFrameKind::Control,
            LmaxFrameKind::OrderBook,
            LmaxFrameKind::Trade,
            LmaxFrameKind::Unknown,
        ]),
        dummy_key in "[a-z]{3,10}",
        dummy_val in "[a-zA-Z0-9_]{1,15}",
        nested_key in prop::sample::select(vec!["type", "bids", "last_price", "other"]),
    ) {
        let mut map = serde_json::Map::new();

        // Add nested objects that contain target keys (should NOT trigger top-level dispatch)
        let mut nested = serde_json::Map::new();
        nested.insert(nested_key.to_string(), Value::String("nested_value".to_string()));
        map.insert("nested_struct".to_string(), Value::Object(nested));

        map.insert(dummy_key, Value::String(dummy_val));

        match target_kind {
            LmaxFrameKind::Control => {
                map.insert("type".to_string(), Value::String("SUBSCRIBE".to_string()));
            }
            LmaxFrameKind::OrderBook => {
                map.insert("bids".to_string(), Value::Array(vec![]));
                map.insert("asks".to_string(), Value::Array(vec![]));
            }
            LmaxFrameKind::Trade => {
                map.insert("last_price".to_string(), Value::String("50000.00".to_string()));
                map.insert("last_quantity".to_string(), Value::String("1.0".to_string()));
            }
            LmaxFrameKind::Unknown => {
                // Ensure no conflicting target keys at top level
                map.remove("type");
                map.remove("bids");
                map.remove("last_price");
            }
        }

        let obj = Value::Object(map);
        let serialized = serde_json::to_vec(&obj).unwrap();

        let verdict = peek_frame_kind(&serialized);
        prop_assert_eq!(verdict, target_kind);
    }

    #[test]
    fn property_peek_frame_kind_ignores_keys_inside_string_values(
        fake_key in prop::sample::select(vec!["type", "bids", "last_price"]),
        prefix in "[a-z]{0,5}",
    ) {
        // Construct JSON where the target key appears only inside a string value
        let val_str = format!("{prefix} \"{fake_key}\": 123");
        let obj = serde_json::json!({
            "instrument_id": val_str,
            "random_field": "some_value"
        });
        let bytes = serde_json::to_vec(&obj).unwrap();

        let verdict = peek_frame_kind(&bytes);
        prop_assert_eq!(verdict, LmaxFrameKind::Unknown);
    }
}
