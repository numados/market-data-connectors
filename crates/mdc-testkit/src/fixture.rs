//! Test fixture loading helpers.

use std::path::{Path, PathBuf};

/// Reads a test fixture file as a string.
///
/// Looks relative to the manifest directory or workspace root.
pub fn load_fixture(rel_path: impl AsRef<Path>) -> Result<String, std::io::Error> {
    let rel = rel_path.as_ref();
    if rel.exists() {
        return std::fs::read_to_string(rel);
    }

    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(manifest_dir).join(rel);
        if p.exists() {
            return std::fs::read_to_string(p);
        }
    }

    // Try walking up to workspace root
    let mut dir = std::env::current_dir()?;
    for _ in 0..5 {
        let candidate = dir.join(rel);
        if candidate.exists() {
            return std::fs::read_to_string(candidate);
        }
        if !dir.pop() {
            break;
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Fixture not found: {}", rel.display()),
    ))
}

/// Reads a test fixture file as raw bytes.
///
/// Looks relative to the manifest directory or workspace root.
pub fn load_fixture_bytes(rel_path: impl AsRef<Path>) -> Result<Vec<u8>, std::io::Error> {
    let rel = rel_path.as_ref();
    if rel.exists() {
        return std::fs::read(rel);
    }

    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let p = PathBuf::from(manifest_dir).join(rel);
        if p.exists() {
            return std::fs::read(p);
        }
    }

    // Try walking up to workspace root
    let mut dir = std::env::current_dir()?;
    for _ in 0..5 {
        let candidate = dir.join(rel);
        if candidate.exists() {
            return std::fs::read(candidate);
        }
        if !dir.pop() {
            break;
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Fixture not found: {}", rel.display()),
    ))
}
