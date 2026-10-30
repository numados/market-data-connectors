# Engineering Standards

This document defines the engineering standards, architecture invariants, code conventions, testing methodologies, and verification gates for the `market-data-connectors` workspace.

---

## 1. Mission & Architecture Invariants

The workspace provides standalone market-data connectors for cryptocurrency and FX venues. Providers do not depend on proprietary shared-memory or C-ABI layouts; downstream projects integrate through public contracts.

### Non-Negotiable Invariants

1. **Decoupled Transport and Sinks**: Core networking and parsing crates must not mandate downstream transport or storage. Providers emit through the per-level `StreamingSink`; `MarketDataSink` (owned events), channels, and async streams are adapters on top. Connection loss, pause, and integrity transitions are signalled **in-band** as `IntegrityAlert` events, ordered with the data they invalidate, rather than only through hooks or logs.
2. **Book Integrity**: Every order-book connector must maintain book integrity. Sequence gaps ($U/u$), CRC32 checksum mismatches (Kraken, Bitfinex), heartbeat loss, and silence timeouts trigger deterministic recovery instead of passing a known-invalid book state downstream. The order book is invalid from the alert until the provider's recovery point; no order-book data for that symbol is emitted in between. Polling reference-rate and quote connectors validate their own response contracts instead.
3. **Bounded Memory and Task Lifetime**: All connection tasks use structured concurrency (`tokio_util::sync::CancellationToken`). In-flight delta buffers during snapshot reconciliation are bounded, and task or buffer lifetime must not grow with uptime.
4. **Hot-Path Allocation Budget**: Use zero-copy parsing (borrowing `serde_json` with `RawValue` where a venue checksum needs exact tokens), fixed-size symbols (`SmallString<[u8; 16]>`), and reused scratch buffers. Capture the ingestion timestamp immediately upon socket-frame receipt, before decompression and parsing. The `StreamingSink` path is tested for zero allocations per frame after warm-up; owned-event adapters may allocate and must document it.
5. **Explicit policy boundaries**: Resync retry/quarantine, selected checksum behavior, backoff, HTTP transport, and handshake/auth concerns are represented by provider-specific configuration or policy seams where the provider exposes them. Downstream quote age, book projection, storage, and business rules belong to the consumer. Sequence validation is part of the provider's venue-specific integrity path; it is not a universal consumer-replaceable switch.
6. **Clock Injection**: Every state machine in `mdc-core` takes `now: Instant` as a parameter. Only runtime drivers read clocks. This keeps integrity logic deterministic and testable without wall-clock time.
7. **Secrets and Sensitive Configuration**: Do not store credentials, account paths, tenant names, or internal hosts in source, fixtures, docs, or history. Credentials are configuration, wrapped in a zeroizing secret type, absent from `Debug` output, and checked with secret scanning.

---

## 2. Code Conventions

### 2.1 Crates and Layering

| Crate | May depend on | Runtime deps | Contents |
|---|---|---|---|
| `mdc-core` | — | none (no tokio, no I/O) | models, events, integrity state machines, all public traits; `#![forbid(unsafe_code)]` |
| `mdc-providers` | `mdc-core` | tokio, tokio-tungstenite, reqwest, flate2, crc32fast | WebSocket/REST drivers, backoff, watchdog wiring, venue implementations behind feature flags (`binance`, `kraken`, …, `all`; TLS: `native-tls` default or `rustls`) |
| `mdc-sinks` | `mdc-core` (+ tokio optional) | optional | channel and stream adapters, top-of-book reference projection, bounded channels; `#![forbid(unsafe_code)]` |
| `mdc-testkit` | `mdc-core`, `mdc-providers`, `mdc-sinks` | tokio, wiremock | mock WebSocket/REST servers with scripted faults, fixture loader, proptest strategies, conformance runners; `#![forbid(unsafe_code)]` |
| `mdc-inspect` | `mdc-core`, `mdc-providers`, `mdc-sinks` | tokio, ratatui, crossterm | terminal application for provider inspection and consumer-path session handling; `#![forbid(unsafe_code)]` |

Dependencies point strictly downward. `mdc-core` never learns about a venue's wire format.

> **Runtime boundary**: A standalone `mdc-runtime` crate and generic public `Supervisor` are not part of this workspace. Transport runtime, provider-local supervision, socket loops, backoff, and REST snapshot execution live within `mdc-providers` alongside each venue implementation. Downstream consumers embed connectors directly or compose them with their own application supervisor.

### 2.2 Modules and Visibility

- File layout follows standard Rust conventions: `foo.rs` + `foo/` for submodules (2018 style). No `mod.rs`.
- `lib.rs` is a façade: `mod` declarations, `pub use` re-exports, crate-level `//!` docs. Logic lives in concept modules.
- Modules are named by domain concept (`integrity`, `resync`, `embargo`, `checksum`), never generic catch-alls like `utils`, `helpers`, `common`, or `misc`.
- Default visibility is `pub(crate)`; widen to `pub` only at the intended API boundary. Every `pub` item is semver surface.
- Raw wire-format structs are `pub(crate)` inside the provider module; only canonical models cross crate boundaries.

### 2.3 Errors and Panics

- Core and provider APIs use `thiserror`-based error types. `mdc-core` exposes `MdcError` and provider modules also expose provider-specific configuration, transport, and parsing errors where their public API needs them. `anyhow` is not used by the library crates; boxed errors are used only where a public contract requires an opaque source.
- Preserve source errors when the public error type supports it. At boundaries whose public error contract is a string-based configuration variant, the provider may convert the detail to a string; do not claim that all provider errors share one taxonomy.
- `unwrap`, `expect`, direct indexing, and `unreachable!` are forbidden on any path reachable from venue data, files, network, or configuration. They are allowed in tests, benches, and examples. `expect` in library code is acceptable only for a locally obvious invariant and must state what failed. Enforced by `clippy::unwrap_used` and `clippy::expect_used` at `deny` workspace-wide.
- Venue data never panics a connector: a malformed frame is logged at `warn` with `venue`, `connection_id`, and a bounded excerpt, counted, and skipped.
- Arithmetic on venue-provided numbers uses checked or saturating operations.

### 2.4 Unsafe Code

- `mdc-core`, `mdc-sinks`, `mdc-testkit`: `#![forbid(unsafe_code)]`.
- All workspace crates currently forbid `unsafe` code, including `mdc-providers`. `unsafe_op_in_unsafe_fn = "deny"` is also set workspace-wide.

### 2.5 Hot Path and Async

- Capture `received_ts` as close to frame receipt as the provider path permits and reuse it for events derived from that frame. Providers with different dispatch paths may capture it later; consumers should treat it as the provider's ingestion/dispatch timestamp, not as a universal parser-entry timestamp.
- Borrow from the frame buffer (`&'a str`, `&'a RawValue`); no `String` per message except a symbol longer than 16 bytes (logged once per symbol at `debug`).
- Reuse buffers: one `BytesMut` frame buffer, one gzip output buffer, two `Vec<PriceLevel>` scratch vectors per connection. Capacity is retained across frames.
- Sequence and checksum decisions are made on the header before levels are materialized; only a symbol waiting for a snapshot buffers owned deltas, and that buffer is strictly capped.
- No lock held across `.await`. No blocking I/O or `std::thread::sleep` on an async runtime thread. No unbounded channels fed by venue data. Provider tasks are supervised by `JoinSet` where the connector uses a multi-connection supervisor; provider-specific polling or connection tasks may instead be spawned directly, but are tied to the connection's `CancellationToken`.
- `Connector::run` must work on a `current_thread` runtime: no `block_in_place`, no correctness dependence on `spawn_blocking`.
- Timers use `tokio::time`, allowing deterministic virtual-time testing.

### 2.6 Observability and Secrets

- Structured logging with `tracing` only; no `println!` or `eprintln!` in library crates. Standard fields: `venue`, `connection_id`, `symbol`. Routine reconnects and venue-scheduled disconnects are `info`, not `error`.
- `Debug` for any configuration type containing credentials redacts them; unit tests assert secret strings are absent from `format!("{:?}", cfg)`.
- Fixtures contain captured public payloads only; strip any session IDs or account references before committing.

### 2.7 Dependencies and Features

- Versions are declared once in `[workspace.dependencies]`; members use `{ workspace = true }`. `Cargo.lock` is committed.
- Optional dependencies use `dep:` syntax; every feature is documented in the crate's `//!` docs. Providers compile with `--no-default-features` and with each venue feature alone.
- Workspace uses edition 2024 and resolver 3. MSRV is 1.85, declared in `[workspace.package].rust-version` and inherited by every member.

### 2.8 Documentation and Naming

- Every `pub` item has rustdoc; `missing_docs` is `warn` workspace-wide and `deny` in `mdc-core`. Doc examples compile. `cargo doc --no-deps` passes with `RUSTDOCFLAGS="-D warnings"`.
- Every provider module opens with a `//!` header: endpoint, limits, integrity mechanism, defaults, a short usage example, and required configuration.
- Comments explain intent, invariants, venue quirks, and safety contracts.
- Names follow Rust API Guidelines: `Symbol`, `VenueId`, `UnixNanos`, and `L2Delta`. Builder return types are provider-specific: some return a connector/handle tuple, while polling connectors and lower-level connectors expose different run shapes. Exhaustive state enums (`MarketDataKind`, `IntegrityAlert`, `Side`) are not `#[non_exhaustive]`; open sets (`VenueId`, error kinds) are.

### 2.9 Commit Conventions

All commits follow Conventional Commits (`type(scope): Description`). Consult [`docs/commit-conventions.md`](commit-conventions.md) for allowed types, valid scopes, body rules, and repository examples.

---

## 3. Shift-Left Testing Standards

All verification is pushed to the earliest possible stage: types, schema contracts, property-based invariants, and local in-process mocks.

### Core Testing Rules

1. **Type-Driven Defect Prevention**: Use newtypes and typed enums for protocol values and lifecycle decisions. Provider connection lifecycle is implemented by provider-specific runtime loops rather than one shared `Disconnected`/`Connecting`/`Connected` typestate API. Per-symbol synchronization states such as `BufferingDiffs`, `InSync`, and provider-specific quarantine states are runtime enums verified by deterministic and property-based tests.
2. **Contract & Golden Fixture First**: Capture real venue payloads as static JSON files in `tests/fixtures/<provider>/` before implementing network handlers. Unit tests verify schema compatibility and edge cases (negative values, scientific notation, empty arrays, one-sided updates).
3. **Property-Based Testing (`proptest`)**: Sequence validation ($U/u$), checksum algorithms, time gating, and backoff curves are verified with randomized inputs simulating gaps, out-of-order delivery, duplicates, and mutations. State machines get a `proptest-state-machine` reference model with invariants checked after every transition.
4. **Deterministic Local Testing**: **Never connect to a live venue during automated tests.** All networking terminates at `127.0.0.1:0` through `mdc-testkit`. Timers run under `tokio::time::pause()`; tests advance time explicitly instead of sleeping.
5. **Fault Coverage**: Tests cover gap injection, bad checksum, silence timeout, close frame, TCP drop, HTTP 429/418 with and without `Retry-After`, stale snapshots, pause/resume, and cancellation.

### Test Placement

| Test kind | Location | Rule |
|---|---|---|
| Unit | `#[cfg(test)] mod tests` at the bottom of the file under test | May test private items. When tests exceed ~200 lines, move to a sibling `tests.rs` or `tests/` directory with `#[cfg(test)] mod tests;`. |
| Integration | `tests/<scenario>.rs` | Public API only. Named by scenario (`mock_binance_ws.rs`, `property_resync_state_machine.rs`). Shared helpers in `tests/common/mod.rs`. Feature-gated tests start with `#![cfg(feature = "<venue>")]`. |
| Doc tests | `///` examples | Must compile and pass via `cargo test`. |
| Benches | `benches/<name>.rs` | `criterion`; hot-path benches also assert allocation counts. |
| Fuzz (optional) | `fuzz/` | Own workspace, `cargo-fuzz`, parsers only. |

---

## 4. Testing Layers

| Layer | Scope | Tooling | Location |
|---|---|---|---|
| **Level 1: Compile & Lints** | Syntax, safety, lints, docs | `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo doc` with `-D warnings` | Pre-commit / CI |
| **Level 2: Unit & Fixtures** | Deserialization, zero-copy parsing, normalization, symbol mapping | `serde_json`, golden JSON files | `tests/fixtures/`, `mod tests` in `src/` |
| **Level 3: Property & FSM** | Sequence gaps, checksums, time gates, backoff curves, state machines | `proptest`, `proptest-state-machine` | `tests/property_*.rs` |
| **Level 4: Protocol Mocking** | Reconnects, ping/pong, resubscribe, REST snapshot buffering, embargo, pause, cancel | `mdc-testkit` (`MockWsServer` on `TcpListener(0)`, `MockRest` over `wiremock`, `conformance::run`) | `tests/mock_*.rs` |
| **Level 5: Benchmarks** | Parse latency, checksum cost, allocation counts | `criterion`, `allocation-counter` | `benches/` |

---

## 5. Protocol for Adding an Exchange Provider

Follow this sequence when adding provider support in `crates/mdc-providers`:

1. **Fixtures**: Add authentic sample payloads to `crates/mdc-providers/tests/fixtures/<provider>/` (`depth_update.json`, `order_book_snapshot.json`, `trade.json`, `book_ticker.json`, `heartbeat_or_ping.json`). Compressed venues keep a `<name>.json.gz` twin of every fixture.
2. **Schema & Deserialization**: Write `pub(crate)` raw message structs (borrowed fields, scratch-buffer level arrays) and unit tests verifying every fixture parses and normalizes field by field into canonical models.
3. **Integrity Strategy**: Implement the venue's sequence, checksum, heartbeat, or time-gate rule on top of the `mdc-core` primitives, with unit tests, `proptest` invariants, and state-machine models.
4. **Mock Integration**: Implement subscription generation and socket lifecycle. Write deterministic `mdc-testkit` tests for handshake, subscribe payloads, ping/pong, fault scenarios, `ConnectorHandle` commands, and cancellation. Providers never roll their own mock servers.
5. **Embedder Conformance**: Verify the provider passes `mdc_testkit::conformance::run` and `MrsConsumerHarness` tests with zero unhandled alert sequences.
6. **Benchmarks**: `benches/<provider>_parse.rs` (largest fixture, parse + normalize, allocation count 0 on the streaming path) plus a checksum bench where applicable.
7. **Docs**: `//!` header per §2.8, feature flag documented, example compiles.
8. **Verification Gate**: §6.

---

## 6. Verification Gate

Run before submitting changes or merging to `main`:

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
