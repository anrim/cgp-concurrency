# AGENTS.md

## Orient first

Read [the design](DESIGN.md) and the active release in [TASKS.md](TASKS.md).

This workspace targets CGP v0.8.0 and versions itself independently. Do not describe manifest
pre-release suffixes in prose.

## Ownership

- Put portable task-lifecycle components in `crates/rtes/cgp-concurrency-runtime`.
  Reuse runtime selection from the published CGP `cgp-runtime` dependency.
- Put provider-neutral handler combinators and policies in this repository.
- Put Tokio and Smol mechanics only in their provider crates.
- Keep the facade source `no_std`; provider crates may require `std`. Do not claim cross-target
  support until the CGP dependency chain and a bare-metal target check both pass.
- Do not introduce blanket `Send + Sync` bounds. Recover Send on the operations that cross threads.
- Do not expose a detached-task operation unless a later release explicitly designs one.

Document public concurrency components in this workspace and exercise them through the facade.

## Commands

```sh
cargo +nightly-2026-10-17 fmt --all
cargo +1.89.0 check --locked --workspace --all-features --all-targets
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo clippy --locked --no-default-features --all-targets -- -D warnings
cargo nextest run --locked --workspace --all-features --no-fail-fast
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

Also check the facade with no runtime, Tokio only, Smol only, and both runtime features. Lifecycle
tests must be deterministic: use controlled futures, barriers, and atomics rather than sleeps.
