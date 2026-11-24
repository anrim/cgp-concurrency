# Changelog

All notable changes to this project are documented here.

## v0.2.0

This release targets CGP v0.8.0 and requires Rust 1.89 or later.

### Added

- `JoinHandlers` polls a fixed list of CGP handlers in the caller's future. Branches can borrow
  local data and hold non-`Send` values across suspension. The caller receives every result,
  including application errors, in declaration order. The join works with the facade's default
  feature set and can feed its results into another handler. [Facade example](crates/main/cgp-concurrency/README.md#join-typed-handlers)

## v0.1.0 — 2026-11-23

This first release targets CGP v0.8.0 and requires Rust 1.89 or later.
The facade has no default runtime. Enable the `tokio` or `smol` feature, or both.

### Added

- A portable `CanSpawnLocalTask` contract in `cgp-concurrency-runtime`, exposed through the
  facade, with Tokio and Smol local spawners and shared ownership tests.
- Cross-thread spawning through `CanSpawnTask`, with `TokioSpawner` and `SmolSpawner`
  accepting owned `Send` futures and returning transferable task owners.
- Cooperative cancellation, abort, and drain for local and cross-thread tasks. Fallible spawn
  reports prior cancellation as `SpawnError::Cancelled`. Draining an accepted task distinguishes
  completion, cancellation, panic, and executor failure. Application errors retain their type
  inside `TaskOutcome::Completed`.
- Initial facade, Tokio provider, Smol provider, and conformance-test workspace.
- CI gates for formatting, linting, feature isolation, tests, and documentation.
