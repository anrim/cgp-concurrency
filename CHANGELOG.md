# Changelog

All notable changes to this project are documented here.

## Unreleased

### Added

- A portable `CanSpawnLocalTask` contract in `cgp-concurrency-runtime`, exposed through the
  facade, with Tokio and Smol local spawners and shared ownership tests.
- Cross-thread spawning through `CanSpawnTask`, with `TokioSpawner` and `SmolSpawner`
  accepting owned `Send` futures and returning transferable task owners.
- Cooperative cancellation, abort, and drain for local and cross-thread tasks. Fallible spawn
  reports refusal at admission. Accepted tasks report completion, cancellation, panic, or runtime failure.
- Initial facade, Tokio provider, Smol provider, and conformance-test workspace.
- CI gates for formatting, linting, feature isolation, tests, and documentation.
