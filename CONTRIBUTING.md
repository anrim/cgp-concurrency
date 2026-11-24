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

- Start a feature from `main`. Name the branch `<kind>/<short-kebab-case-description>`, using
  `feat`, `fix`, `docs`, or `chore` for the kind. For example, `feat/typed-join` holds the
  typed join work. Keep the release number in [TASKS.md](TASKS.md) and the release tag.
- Work through the active release checklist in order on that feature branch. Give each task
  its own commit with an imperative summary. Include the implementation, documentation, and
  focused tests that establish its contract. When the checklist names a separate acceptance
  scenario, give that scenario its own commit.
- Run the focused checks before marking the item complete. In the same commit, check off the
  item in [TASKS.md](TASKS.md) and link to the implementation or test that verifies it.
- Add crates only when an increment introduces a real public boundary.
- Recover `Send` at operations that cross threads; do not add blanket `Send + Sync` bounds.
- Do not introduce detached execution.

## 4. Test the contract

Public behavior requires tests and documentation in the same change. Cover the relevant empty,
singleton, many-item, maximum-capacity, failure, panic, cancellation, and shutdown cases. See
[TESTING.md](TESTING.md) for the shared suite, test tools, and parallel-handler test plan.

Exercise public CGP operations through the facade with checked application wiring. Choose inputs
that expose the promised type and lifetime bounds, such as a caller-local borrow or an `Rc`
held across suspension. Control branch progress with gates. For a wakeup guarantee, open the
gate after observing `Pending` and let the executor perform the next poll. A manual repoll
can hide a lost wake.

### Verification

Rustup selects the development compiler from [rust-toolchain.toml](rust-toolchain.toml). CI
checks library compatibility separately on Rust 1.89. Install the additional verification tools with

```sh
rustup toolchain install 1.89.0 --profile minimal
rustup toolchain install nightly-2026-10-17 --profile minimal --component rustfmt
cargo install cargo-nextest --version 0.9.148 --locked
```

For changes to CGP components, providers, or wiring, verify conformance before review.
Check the provider against the published CGP v0.8.0 traits and the
[CGP knowledge base](https://github.com/contextgeneric/cgp-knowledge-base). Verify that the
context and code tag reach the intended provider, output and error types are preserved, and
the bounds permit the supported callers.

Type-check the workspace, including the facade tests' checked application wiring. With
`cargo-cgp` installed, run the following local check for CGP-specific diagnostics.

```sh
cargo cgp check --locked --workspace --all-features --all-targets
```

CI type-checks the wiring with Cargo and runs the facade conformance tests with nextest.

When reviewing dependencies, run `cargo update --dry-run` to see updates within the manifest
requirements. To check direct dependencies outside those ranges, use cargo-edit's
`cargo upgrade --dry-run --incompatible allow`. Review proposed updates against the workspace's
CGP target and minimum Rust version.

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
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --test typed_join
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features tokio
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features smol

cargo test --locked --workspace --all-features --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

The [CI workflow](.github/workflows/tests.yml) exercises the typed join with both runtime features
disabled and runs the conformance suite with Tokio, Smol, and both together. A release may add
a target-specific check, such as the bare-metal check required before making a cross-target
`no_std` claim.

## 5. Review and merge

Review each task against its acceptance criteria before moving on. Check ownership, cancellation,
draining, ordering, feature isolation, and the public documentation and changelog. Inspect the CGP
macro expansion when the generated bounds are unclear.

Ask which regression the acceptance test would catch. For example, serial polling should fail a
test that requires two branches to be pending together. When that is hard to tell from inspection,
try the fault in a temporary copy. Fix contract or coverage gaps in the task under review. Record
optional refactors separately, with measurements when the concern is performance or memory use.

Push reviewed commits to the same branch name on `origin`. Verify that the SHA returned by
`git ls-remote --heads origin refs/heads/feat/typed-join` matches `git rev-parse HEAD`,
substituting the branch being reviewed. If a rebase changed the commit IDs, use `git range-diff`
to compare the reviewed series with the series being pushed.

Open a pull request against `main` with the outcome, risks, and verification results. Hosted CI
runs on pull requests and pushes to `main`. When reporting completion, distinguish local results
from hosted checks on the reviewed commit. Merge after required checks pass and review comments
are resolved. Preserve the separate task commits when merging the feature branch.

## 6. Release

Completing a feature checklist makes the work ready for review. Tagging and publishing belong
to a separate release decision. This workspace has its own release sequence:

1. confirm the active release's exit criteria in [TASKS.md](TASKS.md) and run the full verification gate;
2. update crate versions, the lockfile, and the `CHANGELOG.md` compatibility notes;
3. verify package contents and generated documentation;
4. merge the release change to `main`;
5. publish `cgp-concurrency-runtime`, then the provider crates, then the facade once its dependencies are available;
6. create the signed `vX.Y.Z` tag and release notes; and
7. verify the published crates and documentation.

The change is done when tests, public documentation, and the changelog are complete.
