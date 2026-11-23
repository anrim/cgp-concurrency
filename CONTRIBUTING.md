# Contributing

This document is the software-development lifecycle for `cgp-concurrency`. Keep changes small,
deterministic, and inside one ownership boundary.

## 1. Shape the change

Before implementation:

1. identify the user outcome and target release;
2. confirm the owner in [DESIGN.md](DESIGN.md);
3. state in-scope and out-of-scope behavior;
4. define observable acceptance criteria, including failure and shutdown behavior.

A change is ready when its owner, contract, and test approach are clear.
Portable task operations belong in `cgp-concurrency-runtime`. Handler policy belongs in the
facade. Runtime mechanics stay in the provider crates.

## 2. Design only what is active

Update [DESIGN.md](DESIGN.md) when a stable contract or boundary changes. Update
[TASKS.md](TASKS.md) when scope or sequencing changes. Add a decision record only when
future maintainers need rationale that neither document can express concisely.

Do not copy the same contract into several documents. Link to its owner instead.

## 3. Implement in reviewable slices

- Branch from `main` with a short `feat/`, `fix/`, `docs/`, or `chore/` name.
- Keep commits single-purpose and use an imperative summary.
- Add crates only when an increment introduces a real public boundary.
- Recover `Send` at operations that cross threads; do not add blanket `Send + Sync` bounds.
- Do not introduce detached execution.

## 4. Test the contract

Public behavior requires tests and documentation in the same change. Cover the relevant empty,
singleton, many-item, maximum-capacity, failure, panic, cancellation, and shutdown cases. See
[TESTING.md](TESTING.md) for the shared suite, test tools, and parallel-handler test plan.

### Verification

Rustup selects the development compiler from [rust-toolchain.toml](rust-toolchain.toml). CI
checks library compatibility separately on Rust 1.89. Install the additional verification tools with

```sh
rustup toolchain install 1.89.0 --profile minimal
rustup toolchain install nightly-2026-10-17 --profile minimal --component rustfmt
cargo install cargo-nextest --version 0.9.148 --locked
```

The fast local loop is:

```sh
cargo +nightly-2026-10-17 fmt --all
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo nextest run --locked --workspace --all-features --no-fail-fast
```

Before review, run the full gate from the workspace root:

```sh
cargo +nightly-2026-10-17 fmt --all -- --check
cargo +1.89.0 check --locked --workspace --all-features --all-targets
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo clippy --locked --no-default-features --all-targets -- -D warnings

cargo check --locked -p cgp-concurrency --no-default-features
cargo check --locked -p cgp-concurrency --no-default-features --features tokio
cargo check --locked -p cgp-concurrency --no-default-features --features smol
cargo check --locked -p cgp-concurrency --all-features

cargo nextest run --locked --workspace --all-features --no-fail-fast
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features tokio
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features smol

cargo test --locked --workspace --all-features --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

CI runs the same groups. A release may add a target-specific check, such as the supported bare-metal
target required before making a cross-target `no_std` claim.

## 5. Review and merge

A pull request explains the outcome, boundary, risks, and verification evidence. Review checks:

- correctness of ownership, cancellation, draining, ordering, and bounds;
- feature isolation and absence of runtime-specific types in portable APIs;
- deterministic coverage of both providers; and
- documentation and changelog impact.

Merge only with green required checks and resolved review comments. Prefer squash merge for a noisy
development branch; preserve intentional multi-commit series when each commit is independently
reviewable.

## 6. Release

This workspace has its own release sequence:

1. confirm the active release's exit criteria in [TASKS.md](TASKS.md) and run the full verification gate;
2. update crate versions, the lockfile, and the `CHANGELOG.md` compatibility notes;
3. verify package contents and generated documentation;
4. merge the release change to `main`;
5. publish `cgp-concurrency-runtime`, then the provider crates, then the facade once its dependencies are available;
6. create the signed `vX.Y.Z` tag and release notes; and
7. verify the published crates and documentation.

The change is done when tests, public documentation, and the changelog are complete.
