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

Run the public worker-spawn cases on both providers.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features -j 1 -E 'test(multithread)'
```

## Join handlers and fork handlers

Both joins return one typed result per handler in declaration order. The tests distinguish
them by observing what happens when a branch's gate opens.

| What the test observes | Join handlers (`JoinHandlers`, v0.2.0) | Spawn then join (`CanSpawnHandlerTasks`, v0.3.0) |
| --- | --- | --- |
| Who advances the handler? | Opening the second gate wakes the caller. Its next poll of the join advances the second handler. | `spawn` submits both children immediately. The test opens the second gate and observes the child pass it before calling `tasks.join()`. |
| What can a branch carry? | The label borrows the caller's string across an await. | Each child owns its input and context clone. A local child returns an `Rc` on the caller's thread. A worker child returns a `Cell` from another thread. |
| How does an application error appear? | The second slot holds `Err(InvalidRecord(17))`. | The second slot holds `TaskOutcome::Completed(Err(Rejected(17)))`. The task completed and the handler returned an error. |
| What happens when a child panics? | The panic unwinds through the caller. | The first slot holds `TaskOutcome::Panicked`. Joining the group waits for the gated sibling and returns its application result in the second slot. |
| What happens on drop? | Dropping the join directly drops its pending futures and retained results. | Dropping the group or a pending join drops its owned handles and requests abort. The test observes the executor reclaim both children while their gate stays shut. |

Run the [caller-task join tests](crates/tests/cgp-concurrency-tests/tests/typed_join.rs) and
their future-size check. The [three-handler case](crates/tests/cgp-concurrency-tests/tests/typed_join/interleaved.rs)
releases the middle, last, and first handler on successive wakeups. The facade doctests exercise
composition with `PipeHandlers` and check that surplus inputs fail to compile.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features --test typed_join --test typed_join_layout
cargo test --locked -p cgp-concurrency --doc
```

Run the [fork handler tests](crates/tests/cgp-concurrency-tests/tests/fork_handlers.rs)
on both runtimes. Each exercises `SpawnLocalTasks` and `SpawnWorkerTasks`, including moving
two worker groups to another thread. Cancelling one group leaves the other group's token
clear, and workers that ignore the request return their application results.
The panic tests exercise a handler whose future panics during both polling and destruction.
A context clone also panics on drop. The group reports the first panic, checks that the destructors
ran outside an active unwind, and still collects the sibling's result.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features --test fork_handlers
```

Run just the [cancellation tests](crates/tests/cgp-concurrency-tests/tests/fork_handlers/cancellation.rs)
with the filter below. Both children observe `tasks.cancel()` before the caller polls its
returned future. The second finishes first, and the caller waits for the first child's
cleanup before receiving both results in declaration order.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features --test fork_handlers -E 'test(cancellation::)'
```

## Task ownership tests

The local-spawn test holds an `Rc` across an await on Tokio's `LocalSet` and Smol's `LocalExecutor`.
It calls the public spawn component through a test application's compile-time checked CGP wiring.
It observes a pending handle, releases the task, then checks that the returned `Rc` points to
the original allocation. A second case drops a pending owner and waits for the task's channel
receiver to be destroyed. [Local-spawn acceptance tests](crates/tests/cgp-concurrency-tests/tests/task_ownership/local_spawn.rs)

The multithreaded cases submit work through `CanSpawnTask` on a Tokio runtime handle and a Smol
executor with two workers. The test application wires each spawn component independently and
selects its runtime through CGP's `HasRuntime` getter. Its generic caller raises admission errors
with `CanRaiseError<SpawnError>` and recovers a typed application error inside `Completed`.
[Application wiring](crates/tests/cgp-concurrency-tests/tests/task_ownership/app.rs)

Each worker case observes the task's first poll on a different thread from the
caller, recovers a `Cell<u32>` as the output, and verifies that dropping a second pending owner
destroys its task. The facade doctests compile a future holding an `Arc` across an await and
check that the same cross-thread call with an `Rc` fails to compile.

The same facade scenarios run on Tokio current-thread and multithread runtimes and Smol local
and multithread executors. Bounded channels place a cooperative request before or after waiter
registration, and force abort or completion to win. The suite drains acknowledged and ignored
requests and observes destruction before drain returns. Two tasks register on one token before
a single request cancels both. A separate check records each waiter's wakeup directly.
Panic cases cover a poll and a destructor that both panic, a completed value discarded after
cleanup panics, a destructor that panics during abort, and a panic payload whose own destructor
panics. The suite refuses an unpolled future with a panicking destructor,
drops a local owner after publication, and drops a worker owner during its final poll. These cases
check cleanup even when nobody drains the result. A separate thread drains a transferred worker
handle and the caller observes destruction only after dropping the returned value. Smol also
exercises cleanup on shutdown before the first poll.
Separate shutdown cases poll owners while unrelated work is queued on an idle executor, then
drive the tasks and destroy the executor to verify `RuntimeFailure`. The nextest timeout guards
against a lost wake. [Shared scenarios](crates/tests/cgp-concurrency-tests/tests/task_ownership/scenarios.rs) · [Timeout configuration](.config/nextest.toml)

## How we will test parallel handlers

These cases are planned for the corresponding handler releases. Each test controls the order
of events explicitly so it also works when nextest runs a single test at a time. [Release tasks](TASKS.md)

| Operation | How the test drives it | What must hold |
| --- | --- | --- |
| Partial submission (v0.3) | Exercise fallible submission on both runtimes and both providers. Cancel before the first branch, then in a separate run refuse a later branch after a child starts. Record submission attempts and child destruction. | The first case starts zero children. In the second, submission stops at the refusal and all accepted children are aborted and drained before the error returns. |
| Bounded map (v0.4) | Set the limit to two and hold both active items at gates. Check the third has not started, then release the second and wait for the third to start while the first is still pending. Count active handlers and record the peak. | The peak is exactly two. Results, including item errors, follow input order. Empty input starts no handlers and a zero limit is rejected before any handler starts. |

For cross-thread cases, use atomics for counts and channels or barriers for handoffs.

## CI checks

CI runs the workspace suite with both providers, tests each provider separately, and runs
the typed join with both runtime features disabled. The documentation job runs the doctests.
[Full verification commands](CONTRIBUTING.md#verification) · [CI workflow](.github/workflows/tests.yml)
