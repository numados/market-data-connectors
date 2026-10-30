# Commit Conventions

This document defines the repository-wide Conventional Commit standards for `market-data-connectors`.

---

## 1. Commit Structure

```text
type(scope): Description

- Essential durable behavior, contract, or invariant outcome
- Additional distinct outcome, when present
```

- **Subject line**: Imperative present tense ("add", "fix", "implement", never "added", "fixes", "implementing"). Concise, max 72 characters. No trailing period.
- **Body**: One to four concise `- ` bullet points describing durable outcomes. Never repeat the subject line.

---

## 2. Commit Types

| Type | Purpose | When to use |
|---|---|---|
| `feat` | New feature or capability | New providers, core domain contracts, resync controllers, sink implementations |
| `fix` | Bug fix | Correcting sequence gap logic, race conditions, memory leaks, serde desync |
| `test` | Automated tests | Unit tests, proptest state-machine models, mock websocket/rest tests |
| `perf` | Performance improvements | Zero-allocation hot paths, scratch buffer reuse, criterion benches |
| `refactor` | Code restructuring | Internal reorganization without changing public contracts or runtime behavior |
| `docs` | Documentation | Updating specifications, architecture docs, rustdoc comments |
| `chore` | Maintenance & tooling | Updating dependencies, Cargo lints, CI workflows, license files |
| `build` | Build system | Cargo workspace configurations, build scripts, feature gate wiring |
| `style` | Formatting | Fixing formatting or layout that does not affect code logic |
| `ops` | Operational automation | Release scripts, local test harnesses, container definitions |

---

## 3. Allowed Scopes

The scope must identify the **changed crate, domain topic, or workspace component**, never the repository name.

### Crate Scopes
- `core`: Changes to `crates/mdc-core` (models, sinks, error taxonomy, traits).
- `providers`: Cross-provider infrastructure in `crates/mdc-providers`.
- `sinks`: Changes to `crates/mdc-sinks` (channels, reference book, optional staleness monitors).
- `testkit`: Changes to `crates/mdc-testkit` (mock servers, fixture loaders, conformance runners).

### Domain & Provider Concept Scopes
- `integrity`: Sequence continuity (`SequenceTracker`), silence watchdog, embargo state machine.
- `resync`: Self-healing resync FSM, snapshot reconciliation, delta buffering.
- `model`: Canonical market data models (`PriceLevel`, `L2Delta`, `MarketEvent`, `Symbol`).
- `sink`: `StreamingSink`, `MarketDataSink`, and downstream dispatch abstractions.
- Venue-specific scopes: `binance`, `kraken`, `coinbase`, `coinbase-advanced`, `bitfinex`, `gateio`, `bitstamp`, `huobi`, `lmax`, `vwd`, `frankfurter`.
- Workspace component scopes: `inspect`, `catalog`, `rest-quote`, and `conformance`.

### Meta & Workspace Scopes
- `workspace`: Workspace root, root `Cargo.toml`, dependency definitions.
- `docs`: Developer standards, specifications, repository documentation.
- `ci`: GitHub Actions workflows, check gates, security scanners.

---

## 4. Body Decision Gate & Bullet Rules

### When a Body is Required
- The change delivers two or more distinct functional, contract, or operational outcomes.
- The subject necessarily compresses multiple components (e.g. sequence tracking + watchdog + embargo).
- An amend combines an existing commit with staged follow-up changes.
- A critical invariant, boundary condition, or design rationale must be preserved in git history.

### Bullet Formatting
- Use 1 to 4 bullet points starting with `- `. Capitalize the first letter.
- Describe **durable outcomes, invariants, and behaviors** (what changed and why).
- **Do not** list modified filenames; git diff already shows changed files.
- **Do not** repeat or paraphrase the subject line.

---

## 5. Artifact Hygiene

- **No Prompt/Session References**: Never mention prompts, conversations, tickets, or plan IDs.
- **No Process Narration**: Avoid phrases like "as requested", "following review feedback", or "per instruction".
- **No Validation Theater**: Avoid exhaustive test count declarations or listing every compiler lint checked. Focus on what changed.

---

## 6. Amend Workflows

When preparing a commit message for `git commit --amend`:
1. Inspect the complete delta from the base commit's parent to the new index (`git diff HEAD~1`).
2. Integrate follow-up changes into the existing message context instead of describing only the latest patch.
3. Keep the commit message cohesive as if the entire feature had been committed cleanly in a single step.

---

## 7. Concrete Repository Examples

### Feature Additions (`feat`)
```text
feat(resync): implement self-healing resync controller and reconciliation state machine

- Add ResyncController managing per-symbol synchronization lifecycle
- Support fast-path range checks without heap allocation during InSync state
- Implement bounded delta buffer with purge on overflow
- Reconcile REST snapshots with buffered deltas, rejecting stale snapshots
- Export resync module types from mdc-core
```

### Testing (`test`)
```text
test(core): add property-based and state-machine tests for integrity and resync FSM

- Add property tests for SequenceTracker continuity, gap detection, and overlap
- Add property tests for EmbargoTracker monotonicity, permit invariants, and safety caps
- Add state-machine model verification for SilenceWatchdog with virtual time
- Add state-machine invariant tests for ResyncController reconciliation
```

### Performance & Benchmarks (`perf`)
```text
perf(core): add criterion benchmarks for integrity tracker and resync controller

- Add Criterion benchmarks for SequenceTracker::check
- Add Criterion benchmarks for ResyncController::check_delta on the InSync hot path
- Add Criterion benchmark for ResyncController::on_snapshot with 5,000 deltas
```

### Documentation (`docs`)
```text
docs: relocate engineering standards and commit conventions

- Move normative standards to docs/engineering-standards.md
- Document Conventional Commit rules in docs/commit-conventions.md
- Update CONTRIBUTING.md with development and verification guidelines
```

### Good vs Bad Patterns

| Dimension | Bad (Do Not Use) | Good (Repository Aligned) |
|---|---|---|
| **Scope** | `feat(market-data-connectors): add binance` | `feat(binance): implement websocket connector` |
| **Subject** | `feat(core): adding domain types and stuff` | `feat(core): implement domain models, canonical events, and integrity alerts` |
| **Body** | `- Updated src/resync/controller.rs lines 20-40` | `- Prevent buffer overflow by purging deltas on capacity breach` |
| **Narrative** | `fix(core): fix review remarks from team` | `fix(core): correct saturating timestamp subtraction` |
