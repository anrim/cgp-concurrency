# CGP Concurrency

Runtime-neutral structured concurrency for
[Context-Generic Programming](https://contextgeneric.dev/), with Tokio and Smol providers.

```rust
let tasks = parent.spawn(code, child, inputs);
let product![first, second] = tasks.join().await;
```

[Installation](#installation) · [Usage](#usage) · [Workspace](#workspace) · [Documentation](#documentation) · [Contributing](#contributing) · [License](#license)

## Installation

This checkout targets Rust 1.89 or later and CGP v0.8.0. From an application next to the
`cgp-concurrency` checkout, add the facade with your preferred runtime feature.

```sh
cargo add --path ../cgp-concurrency/crates/main/cgp-concurrency --features smol
```

Use `tokio` for the Tokio adapter. You can enable both features, or omit `--features`
for caller-driven joins.

## Usage

Run the [fork and join example](crates/main/cgp-concurrency/examples/fork_handlers.rs)
from the workspace root.

```sh
cargo run --locked -p cgp-concurrency --features smol --example fork_handlers
```

| Task | API |
| --- | --- |
| Start owned handlers now and collect their outcomes later | `parent.spawn(…)`, then `tasks.join().await`. Outcomes follow handler declaration order. [Fork handlers](crates/main/cgp-concurrency/README.md#fork-handlers) |
| Handle a refusal during submission | `parent.try_spawn(&token, code, child, inputs).await?` returns an owned group after admission. On refusal it aborts and drains the accepted children. [Fallible submission](crates/main/cgp-concurrency/README.md#fallible-submission) |
| Give the handlers time to clean up | `parent.spawn_cancellable(…)`, then `tasks.cancel().await`. [Cancel and drain a group](crates/main/cgp-concurrency/README.md#cancel-and-drain-a-group) |
| Run handlers that borrow caller data | `JoinHandlers` polls each branch in the caller’s task. [Join typed handlers](crates/main/cgp-concurrency/README.md#join-typed-handlers) |
| Submit a single future | `spawn_local_task` accepts local work. `spawn_task` accepts `Send` work. [Spawn owned tasks](crates/main/cgp-concurrency/README.md#spawn-owned-tasks) |

## Workspace

| Crate | Provides |
| --- | --- |
| [`cgp-concurrency`](crates/main/cgp-concurrency/README.md#handler-traits) | Handler traits for spawning groups, cooperative cancellation, and caller-task joins |
| [`cgp-concurrency-runtime`](crates/rtes/cgp-concurrency-runtime/README.md#runtime-consumer-traits) | Re-exports `cgp-runtime`'s `HasRuntimeType` to name the runtime and `HasRuntime` to borrow it. Adds `CanSpawnLocalTask` and `CanSpawnTask` for owned futures. |
| [`cgp-concurrency-tokio`](crates/rtes/cgp-concurrency-tokio) | Local tasks on a `LocalSet`, worker tasks through a `Handle` |
| [`cgp-concurrency-smol`](crates/rtes/cgp-concurrency-smol) | Local tasks on a `LocalExecutor`, worker tasks on an `Executor` |
| [`cgp-concurrency-tests`](crates/tests/cgp-concurrency-tests) | Shared conformance tests for both providers |

## Documentation

- [Design](DESIGN.md) describes task ownership, execution, and ordering guarantees.
- [Release tasks](TASKS.md) tracks completed and planned features.
- [Changelog](CHANGELOG.md) lists changes by release.

## Contributing

Development and CI use Rust 1.100.0. Run the workspace tests from the repository root.

```sh
cargo nextest run --locked --workspace --all-features --no-fail-fast
```

See the [contributing guide](CONTRIBUTING.md) for setup and required checks, and the
[testing guide](TESTING.md) for conformance tests and benchmarks.

## License

Licensed under the [Apache License 2.0](LICENSE).
