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

The `impl_send_handler!` doctests exercise the worker's `Send` bound with a `u32` input
and output. The handler holds an `Arc` across an await in the passing case and an `Rc`
in the compile-fail case. The fork tests also run a worker handler with a `Cell` in its
child context and a local handler with an `Rc`.

The [admission tests](crates/tests/cgp-concurrency-tests/tests/fork_handlers/admission.rs) check
prior cancellation and join a successful group after dropping its parent. They check that
empty and nonempty groups remain joinable when the original child context panics on drop.
Dropping a submission before its first poll releases the context and every input with zero
provider calls. Both prior cancellation and a provider refusal release an unpolled handler's context and input,
even when each destructor panics.
A controlled spawner refuses the third branch. Destructors for the unused child context and both remaining inputs
panic during cleanup. Both accepted handles must receive abort before either drain starts.
The test opens the first drain gate and waits for its completion signal while the second gate
stays closed. On a worker, the first child’s completed output must already be released.
The test then either drops the wait and observes reclamation, or opens the second gate and
checks that both children have terminated when the admission error returns.

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features --test fork_handlers
```

Run just the [cancellation tests](crates/tests/cgp-concurrency-tests/tests/fork_handlers/cancellation.rs)
with the filter below. Both children observe `tasks.cancel()` before the caller polls its
returned future. The second finishes first, and the caller waits for the first child's
cleanup before receiving both results in declaration order. The drop tests let one child
return a value whose destructor signals and panics, and hold its sibling at a cleanup gate.
After a join or cancellation wait retains that value and returns `Pending`, the caller drops
the wait. The tests observe the value's destruction and the sibling's reclamation with the gate shut.

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
| Bounded map (v0.4) | Set the limit to two and hold both active items at gates. Check the third has not started, then release the second and wait for the third to start while the first is still pending. Count active handlers and record the peak. | The peak is exactly two. Results, including item errors, follow input order. Empty input starts no handlers and a zero limit is rejected before any handler starts. |

For cross-thread cases, use atomics for counts and channels or barriers for handoffs.

## Benchmark spawn and join

The `spawn_join` benchmark measures one complete `parent.spawn(...).join().await` for
groups of one or eight handlers. Each handler immediately returns its integer input.
Criterion reports elapsed time per group and throughput in child tasks per second.
Each sample reuses its executor across iterations. Executor startup, shutdown, and a check
of every child's result sit outside the timed loop.

From the workspace root, save a baseline before changing a feature, then compare on the
same machine with the same Rust toolchain and runtime configuration.

```sh
export CRITERION_HOME="$PWD/target/criterion"
cargo bench --locked -p cgp-concurrency-tests --all-features --bench spawn_join -- --save-baseline fork-handlers
# After the change
cargo bench --locked -p cgp-concurrency-tests --all-features --bench spawn_join -- --baseline fork-handlers
```

Setting `CRITERION_HOME` gives both runs the same baseline directory. Compare each case
with its own baseline. The local cases use the calling thread. The worker cases use the shared
test fixtures with two Tokio worker threads and one Smol worker thread.

### Large batches

`task_batches` submits groups of eight until 1,000 or 10,000 children are pending.
The direct comparison runs the same handler on the same executor using individual task
handles. Each sample waits for every child to register at its gate before finishing.

| Path | What the benchmark verifies |
| --- | --- |
| Join | Open the shared gate and collect every child's input index in declaration order. |
| Cancel and drain | Signal every group before draining the first. Each child observes its group's request and returns its index. |
| Abort on drop | Drop every owner with the gate closed and wait for the executor to destroy the last child's input. |

Run the two profiles separately on an idle machine. Each prints five samples per case as CSV.

```sh
cargo bench --locked -p cgp-concurrency-tests --features tokio,smol --bench task_batches > cpu.csv
cargo bench --locked -p cgp-concurrency-tests --all-features --bench task_batches > heap.csv
```

The first run records process CPU across all threads for submission, waiting for all
children to park, and finishing. Worker execution can overlap submission. The heap run
counts allocations and peak live bytes above the executor's starting level per child.
Its final column reports excess live bytes for the **whole batch** immediately after
reclamation, while the executor can still retain queue storage. Use the first run for
timing comparisons because the heap counters add work to every allocation.

## CI checks

CI runs the workspace suite with both providers, tests each provider separately, and runs
the typed join with both runtime features disabled. The documentation job runs the doctests.
[Full verification commands](CONTRIBUTING.md#verification) · [CI workflow](.github/workflows/tests.yml)
