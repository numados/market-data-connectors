//! Shared validation, deduplication, and sorting utilities for provider catalog decoders.

use mdc_core::catalog::{CatalogError, CatalogInstrument, ProviderKind};

/// Processes raw catalog items: enforces max item limits, detects conflicting duplicates,
/// collapses equal duplicates, and sorts deterministically by `native_id`.
#[allow(dead_code)]
pub fn process_catalog_items(
    provider: ProviderKind,
    mut items: Vec<CatalogInstrument>,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    if items.len() as u64 > max_items as u64 {
        return Err(CatalogError::ItemLimitExceeded {
            provider,
            max: max_items,
            actual: items.len() as u32,
        });
    }

    // Sort by native_id
    items.sort_by(|a, b| a.native_id.cmp(&b.native_id));

    let mut deduplicated = Vec::with_capacity(items.len());
    let mut iter = items.into_iter();

    if let Some(first) = iter.next() {
        let mut current = first;
        for next in iter {
            if next.native_id == current.native_id {
                if next == current {
                    // Equal duplicate: collapse deterministically
                    continue;
                } else {
                    // Conflicting duplicate
                    return Err(CatalogError::ConflictingDuplicate {
                        provider,
                        native_id: Box::new(next.native_id),
                    });
                }
            } else {
                deduplicated.push(current);
                current = next;
            }
        }
        deduplicated.push(current);
    }

    Ok(deduplicated)
}
