# CGP Concurrency

Runtime-neutral structured concurrency for
[Context-Generic Programming](https://contextgeneric.dev/), with Tokio and Smol providers.

```rust
let tasks = parent.spawn(code, child, inputs);
let product![first, second] = tasks.join().await;
```

`spawn` submits one owned task per handler to the configured local or worker executor. The parent
can do other work before joining. Both `tasks.join().await` and `tasks.await` collect the
outcomes in declaration order. [Fork handlers example](crates/main/cgp-concurrency/README.md#fork-handlers)

## Scope

CGP's published `cgp-runtime` supplies runtime selection. This workspace defines the portable
task contract and implements it for Tokio and Smol. The facade's `JoinHandlers` polls borrowed
handlers in the caller's task and works with both runtime features disabled. [Caller-task join example](crates/main/cgp-concurrency/README.md#join-typed-handlers)

For individual futures, `CanSpawnLocalTask` supports an `Rc` held across an await.
`CanSpawnTask` submits owned `Send` work to a worker runtime. Both operations return
an owned handle for cancellation, abort, and drain.

The facade has no default runtime, does not expose runtime-specific types in portable APIs, and does
not provide detached tasks.

## Workspace

| Crate | Purpose | Status |
| --- | --- | --- |
| [`cgp-concurrency`](crates/main/cgp-concurrency) | Runtime-neutral facade | Join handlers in the caller's task or fork them into owned tasks |
| [`cgp-concurrency-runtime`](crates/rtes/cgp-concurrency-runtime) | Portable contract | Local and cross-thread spawn, cancellation, drain, and terminal outcomes |
| [`cgp-concurrency-tokio`](crates/rtes/cgp-concurrency-tokio) | Tokio provider | Local spawn on a `LocalSet` and worker tasks through a runtime `Handle` |
| [`cgp-concurrency-smol`](crates/rtes/cgp-concurrency-smol) | Smol provider | Local spawn on a `LocalExecutor` and worker tasks on an `Executor` |
| [`cgp-concurrency-tests`](crates/tests/cgp-concurrency-tests) | Shared conformance tests | Handler joins, local and worker forks, and task ownership |

This project targets CGP v0.8.0 and versions itself independently.

## Documentation

| Document | Use it for |
| --- | --- |
| [Design](DESIGN.md) | Ownership, dependency direction, and execution contracts |
| [Release tasks](TASKS.md) | Scope, status, and acceptance criteria by release |
| [Testing](TESTING.md) | Run the conformance suite and see how it verifies scheduling, ordering, and cleanup |
| [Contributing](CONTRIBUTING.md) | SDLC, local checks, review, and release process |
| [Changelog](CHANGELOG.md) | User-visible changes |

## Development

Development and CI use Rust 1.100.0. The library also builds on Rust 1.89. Run the local checks with

```sh
cargo +nightly-2026-10-17 fmt --all
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo nextest run --locked --workspace --all-features --no-fail-fast
```

The complete feature and documentation gates are in [CONTRIBUTING.md](CONTRIBUTING.md#verification).

## License

Licensed under the [Apache License 2.0](LICENSE).
