# Changelog

All notable changes to this project are documented here.

## Unreleased

### Added

- A portable `CanSpawnLocalTask` contract in `cgp-concurrency-runtime`, exposed through the
  facade, with Tokio and Smol local spawners and shared ownership tests.
- Cross-thread spawning through the portable `CanSpawnTask` component.
- Initial facade, Tokio provider, Smol provider, and conformance-test workspace.
- CI gates for formatting, linting, feature isolation, tests, and documentation.
