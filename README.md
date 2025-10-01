# Market Data Connectors

A Rust workspace for reusable market-data connectors and their shared contracts.

## Rust version policy

The minimum supported Rust version (MSRV) is **1.85.0**. Crates use Rust edition
2024; consuming applications may use an earlier edition with a compatible
compiler. The workspace uses Cargo resolver 3.

MSRV is declared in `Cargo.toml` and inherited by each workspace member. CI tests
the workspace on both MSRV and stable Rust. The supported minimum covers the
library and its tests; dependency updates must preserve that compatibility.

MSRV does not increase automatically when Rust releases a new stable version.
Raise it only when required by a language feature, a fix, or a maintained
dependency. Announce increases in release notes and make them in a minor release,
not a patch release. The new minimum must have been available for at least six
months, except for an explicitly documented urgent security fix.

## Local checks

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo +1.85.0 test --workspace --all-features --locked
```

Install the MSRV toolchain before running its test command. Additional repository
checks are listed in `docs/engineering-standards.md`.
