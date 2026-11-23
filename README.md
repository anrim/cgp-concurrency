# CGP Concurrency

Runtime-neutral structured concurrency for
[Context-Generic Programming](https://contextgeneric.dev/), with Tokio and Smol providers.

> [!IMPORTANT]
> Use `CanSpawnLocalTask` with Tokio or Smol to run a future that holds an `Rc` across an await
> and recover it as `TaskOutcome::Completed`. The portable contract lives in this workspace.
> Both adapters use the same published CGP dependency.

## Scope

CGP's published `cgp-runtime` supplies runtime selection. This workspace defines the portable
task contract and implements it for Tokio and Smol. The release plan also covers handler
combinators and concurrency policies that work with either provider.

The facade has no default runtime, does not expose runtime-specific types in portable APIs, and does
not provide detached tasks.

## Workspace

| Crate | Purpose | Status |
| --- | --- | --- |
| [`cgp-concurrency`](crates/main/cgp-concurrency) | Runtime-neutral facade | Portable task API and optional adapters |
| [`cgp-concurrency-runtime`](crates/rtes/cgp-concurrency-runtime) | Portable contract | Local spawn, owned handle and completed output |
| [`cgp-concurrency-tokio`](crates/rtes/cgp-concurrency-tokio) | Tokio provider | Local spawn on a `LocalSet` |
| [`cgp-concurrency-smol`](crates/rtes/cgp-concurrency-smol) | Smol provider | Local spawn on a `LocalExecutor` |
| [`cgp-concurrency-tests`](crates/tests/cgp-concurrency-tests) | Shared provider conformance tests | Local spawn acceptance tests |

This project targets CGP v0.8.0 and versions itself independently.

## Documentation

| Document | Use it for |
| --- | --- |
| [Design](DESIGN.md) | Ownership, dependency direction, and execution contracts |
| [Release tasks](TASKS.md) | Scope, status, and acceptance criteria by release |
| [Testing](TESTING.md) | Run the local spawn acceptance tests |
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
