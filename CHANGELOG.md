# Changelog

All notable changes to this project are documented here.

## Unreleased

- Added `JoinHandlers` to the facade for joining borrowed, non-`Send` handlers in the caller's
  future and collecting every typed result in declaration order.

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
