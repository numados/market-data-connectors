# Contributing to Market Data Connectors

Thank you for your interest in contributing to `market-data-connectors`.

This repository maintains strict engineering standards to ensure low latency, zero allocations on hot paths, deterministic book integrity, and robust structured concurrency.

---

## Architecture & Code Standards

All contributions must adhere to our engineering contract:
- [Engineering Standards](docs/engineering-standards.md): Architecture invariants, code conventions, shift-left testing strategy, and crate layering.
- [Commit Conventions](docs/commit-conventions.md): Conventional Commit formatting, scopes, and commit hygiene.

---

## Development Workflow

### Prerequisites
- Rust 1.85.0+ (Edition 2024)
- `cargo-hack` (for per-feature verification): `cargo install cargo-hack`
- `cargo-deny` (for dependency & license checking): `cargo install cargo-deny`
- `gitleaks` (for secret scanning)

### Testing & Verification
All changes must pass the full verification gate before being merged:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo hack check -p mdc-providers --each-feature --no-dev-deps
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo bench --workspace --no-run
cargo deny check
gitleaks detect --no-git --source .
```

### Adding New Venues
When contributing a new exchange connector, follow the protocol outlined in [Engineering Standards §5](docs/engineering-standards.md#5-protocol-for-adding-an-exchange-provider):
1. Capture public fixture payloads under `crates/mdc-providers/tests/fixtures/<provider>/`.
2. Implement zero-copy serde/borrowed wire parsers.
3. Wire the venue's integrity checks (sequence gaps, checksums, watchdogs).
4. Build deterministic mock tests using `mdc-testkit`.
5. Pass conformance checks and criterion benchmarks with zero allocations on the streaming path.
