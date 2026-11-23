# Testing

`cargo-nextest` is the Cargo subcommand we use to run Rust unit and integration tests. It builds
the test binaries, discovers their tests, and runs each test in its own process. Several tests can
run at once. Inside each test, the executor drives the futures and the test controls when each
handler can proceed. [How nextest works](https://nexte.st/docs/design/how-it-works/)

## Run the conformance suite

From the workspace root, install the same runner version as CI once, then run the shared suite.

```sh
cargo install cargo-nextest --version 0.9.148 --locked
cargo nextest run --locked -p cgp-concurrency-tests --all-features
```

This filter reruns just the public local-spawn acceptance test on both providers.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features -E 'test(local_spawn::)'
```

Run the two public worker-spawn cases and check the type boundary in the facade's doctests.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features -j 1 -E 'test(multithread)'
cargo test --locked -p cgp-concurrency --doc
```

## What runs today

The same caller holds an `Rc` across an await on Tokio's `LocalSet` and Smol's `LocalExecutor`.
It calls the public spawn component through a test application's compile-time checked CGP wiring.
It observes a pending handle, releases the task, then checks that the returned `Rc` points to
the original allocation. A second case drops a pending owner and waits for the task's channel
receiver to be destroyed. [Local-spawn acceptance tests](crates/tests/cgp-concurrency-tests/tests/task_ownership/local_spawn.rs)

The multithreaded cases submit work through `CanSpawnTask` on a Tokio runtime handle and a Smol
executor with two workers. Each observes the task's first poll on a different thread from the
caller, recovers a `Cell<u32>` as the output, and verifies that dropping a second pending owner
destroys its task. The facade doctests compile a future holding an `Arc` across an await and
check that the same cross-thread call with an `Rc` fails to compile.

The nextest timeout catches a missing wake or an owner that leaves its task running.

## How we will test parallel handlers

These cases are planned for the corresponding handler releases. Each test controls the order
of events explicitly so it also works when nextest runs a single test at a time. [Release tasks](TASKS.md)

| Operation | How the test drives it | What must hold |
| --- | --- | --- |
| Typed join in the caller's task (v0.2) | Give two branches separate release gates. Wait for both to report their first poll while both gates are closed. Let the second finish before releasing the first. Use borrowed data and return an application error from one branch. | Both branches make progress while the other is pending. The caller receives both typed results in declaration order. |
| Join of spawned children (v0.3) | Run the same scenarios on Tokio and Smol with local and cross-thread tasks. In separate cases cancel the parent, panic a child, and refuse a later spawn after a child has started. Record child starts, outcomes, and destruction. | Cancellation and spawn refusal stop admission. The parent drains every accepted child before it returns. A child panic is observable in its branch's result. |
| Bounded map (v0.4) | Set the limit to two and hold both active items at gates. Check the third has not started, then release the second and wait for the third to start while the first is still pending. Count active handlers and record the peak. | The peak is exactly two. Results, including item errors, follow input order. Empty input starts no handlers and a zero limit is rejected before any handler starts. |

For cross-thread cases, use atomics for counts and channels or barriers for handoffs.

## CI checks

The full workspace run exercises both providers. Separate runs catch accidental dependencies
between their features. Cargo runs the rustdoc examples in a separate step.

```sh
cargo nextest run --locked --workspace --all-features --no-fail-fast
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features tokio
cargo nextest run --locked -p cgp-concurrency-tests --no-default-features --features smol
cargo test --locked --workspace --all-features --doc
```

CI also checks all four facade feature combinations, runs rustfmt and Clippy, builds on the
minimum Rust version, and builds documentation with warnings treated as errors.
[Full verification commands](CONTRIBUTING.md#verification) · [CI workflow](.github/workflows/tests.yml)
