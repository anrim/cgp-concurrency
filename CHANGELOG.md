# Changelog

All notable changes to this project are documented here.

## Unreleased

- `impl_send_handler!` forwards a CGP handler to `SendHandler` in one declaration. Rust
  checks that the handler future can move between worker threads. The facade implements
  `SendHandler` for CGP's `ReturnInput`. Remove any overlapping forwarding declaration
  for that provider when upgrading.
- **Fork handlers.** `spawn` submits one owned task per handler before returning. The provider
  selects local or worker execution. Both `tasks.join().await` and `tasks.await` yield one
  typed outcome per handler in declaration order. If a child panics with unwinding enabled,
  the group waits for its siblings and reports `TaskOutcome::Panicked` in that child's slot.
  Dropping the group or a pending join requests abort for its pending children. [Fork handlers example](crates/main/cgp-concurrency/README.md#fork-handlers)
- Custom `HandlerTaskSpawner` providers name their owned join future with `type Join`.
  Generic callers can return it from `tasks.join()` or `tasks.cancel()` after dropping the parent.
- `spawn_cancellable` supplies a fresh cancellation token to the child context.
  `tasks.cancel().await` signals the group and collects every child's outcome after cleanup. [Cancel and drain a group](crates/main/cgp-concurrency/README.md#cancel-and-drain-a-group)
- `parent.try_spawn(&token, code, child, inputs).await` returns an owned handler group on success.
  A refusal stops submission and aborts and drains every accepted child before returning the error. [Fallible submission](crates/main/cgp-concurrency/README.md#fallible-submission)

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
