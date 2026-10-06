# Apiary Project — Copilot Instructions

## What Is This Project?

Apiary is a distributed data processing engine inspired by honeybee colony
intelligence. It is built first for edge and IoT sites (three to ten Raspberry
Pis) and also runs in the cloud. Apache DataFusion executes queries and Delta
Lake stores data; the colony decides who runs what, where data sits, and when to
slow down.

The project is **mid-migration** from V1 to a redesign. The design is the source
of truth; the code catches up to it one gated phase at a time.

## Source of Truth

- `docs/design/apiary-biomimetic-design.md` — the design. Its section 12 gives
  the build order and the gate for each step. Do not skip ahead of a gate.
- `docs/architecture/` — V1, kept as history. Where it disagrees with the
  design, the design wins. See `docs/architecture/README.md`.

## What the code is today

The code is still mostly V1: a custom JSON ledger in `apiary-comb`, in-memory
tables in `apiary-plan`, and heartbeats through object storage in
`apiary-runtime`. Each redesign phase replaces one of these. Before changing a
subsystem, check which phase owns it in the design's build order, and do not
extend V1 behaviour that a later phase deletes.

Crate layout, and the design crate each is becoming:

- `apiary-core` — ids, errors, `NodeConfig`, `Clock`, `SeededRng`, `Env`
- `apiary-comb` (was `apiary-storage`) — storage backends, ledger, cells (phase 1: `delta-rs`)
- `apiary-plan` (was `apiary-query`) — DataFusion integration
- `apiary-runtime` — node lifecycle, bee pool, heartbeats (split across the crates below)
- `apiary-floor`, `apiary-colony`, `apiary-forage`, `apiary-entrance`,
  `apiary-net`, `apiary-observe` — empty skeletons, filled in by later phases
- `apiary-py` (was `apiary-python`) — PyO3 bindings; `apiary-cli` builds the `apiary` binary

## Technical Stack

- **Language:** Rust, edition 2024
- **Python bridge:** PyO3 + maturin
- **SQL engine:** Apache DataFusion
- **Table format:** Delta Lake via `delta-rs` (phase 1; V1 still uses its own ledger)
- **Data formats:** Parquet in the comb, Arrow in memory and between Nodes
- **Object storage:** `object_store`
- **Networking:** iroh QUIC (phase 2)
- **Async runtime:** Tokio

## Code Conventions

- Use `thiserror` for error types, not anyhow
- Use `tracing` for logging, not println or log
- All public APIs documented with rustdoc
- Unit tests go in `#[cfg(test)]` modules in the same file; integration tests in `tests/`
- Python SDK mirrors Rust API naming (snake_case)
- **Time and randomness go through `apiary_core::Env`** (`Clock`, `SeededRng`).
  Never call `Utc::now`, `Instant::now`, `tokio::time::sleep` or `Uuid::new_v4`
  directly in code that will run under the simulator, so any run replays exactly
  from its seed.
- Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`
  and `cargo test --workspace` before pushing

## Design Invariants

1. **Correctness never rests on the biology.** Delta's commit protocol,
   idempotent stage outputs and fencing tokens carry correctness; the colony
   mechanisms govern performance only.
2. **Create-if-absent is a hard requirement** wherever a Delta log, a Patch
   completion record or a quorum record lives. Stores without conditional
   writes (Garage, for example) are not supported for those.
3. **No Node holds a role the others lack.** Any Node's entrance accepts work.
4. **Local information only.** A Bee reads only its own Node's copy of the dance
   floor, never a colony-wide aggregate.
5. **Every mechanism names its biological source, its control law and the
   simulation test that shows it behaves like the colony.**
6. **Joins are planned to fit a Bee.** DataFusion's hash join cannot spill.

## When You Are Unsure

Read the design. If the answer is not there, flag it as an open question in a
code comment prefixed with `// DESIGN:` and continue with the simplest
reasonable implementation.
