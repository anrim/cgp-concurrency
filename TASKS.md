# Release tasks

## v0.1.0 — Spawn owned tasks

- [x] [Spawn and await local tasks](crates/tests/cgp-concurrency-tests/tests/task_ownership/local_spawn.rs) that hold non-`Send` values across suspension.
- [x] Build [deterministic task ownership fixtures](crates/tests/cgp-concurrency-tests/tests/task_ownership) for Tokio and Smol.
- [x] [Spawn and await worker tasks](crates/main/cgp-concurrency/README.md#spawn-owned-tasks) with `Send` futures and outputs.
- [x] Implement [Tokio](crates/rtes/cgp-concurrency-tokio) and [Smol](crates/rtes/cgp-concurrency-smol) adapters for local and worker tasks.
- [x] Support [admission refusal, cancellation, abort, drain, and failure reporting](crates/main/cgp-concurrency/README.md#cancellation-and-failure).
- [x] [Test cancellation races, panic precedence during cleanup, and reclamation](crates/tests/cgp-concurrency-tests/tests/task_ownership/scenarios.rs) in all four execution modes.
- [x] [Check CGP v0.8.0 compatibility](CONTRIBUTING.md#verification) with neither, either, and both runtime features.
- [x] [Verify CGP provider wiring and propagation of admission and application errors](crates/tests/cgp-concurrency-tests/tests/task_ownership/app.rs).

## v0.2.0 — Join handlers

- [x] [Join handlers in the caller’s task](crates/main/cgp-concurrency/src/join.rs) with borrowed inputs and non-`Send` futures.
- [x] [Test branch wakeups and typed result order](crates/tests/cgp-concurrency-tests/tests/typed_join.rs), including application errors.
- [x] [Bound join storage](crates/tests/cgp-concurrency-tests/tests/typed_join_layout.rs) to one pending future or completed result per branch.
- [x] [Test three interleaved branches](crates/tests/cgp-concurrency-tests/tests/typed_join/interleaved.rs) with a borrowed result and distinct errors.
- [x] [Compose a join with a reporting handler](crates/main/cgp-concurrency/src/join.rs) using `PipeHandlers`.
- [x] [Check typed joins in CI](.github/workflows/tests.yml) with no runtime, each provider separately, and the full workspace.

## v0.3.0 — Fork handlers

- [x] [Fork handlers](crates/tests/cgp-concurrency-tests/tests/fork_handlers.rs) on local or worker executors and join their outcomes in declaration order.
- [x] [Preserve sibling outcomes when a child panics](crates/tests/cgp-concurrency-tests/tests/fork_handlers.rs).
- [x] [Cancel and drain a handler group](crates/tests/cgp-concurrency-tests/tests/fork_handlers/cancellation.rs).
- [x] [Reclaim children and retained results when a pending cancellation wait is dropped](crates/tests/cgp-concurrency-tests/tests/fork_handlers/cancellation.rs).
- [x] [Return an owned join](crates/tests/cgp-concurrency-tests/tests/fork_handlers/cancellation.rs) that can outlive its parent.
- [x] [Benchmark spawn and join](crates/tests/cgp-concurrency-tests/benches/spawn_join.rs) for one and eight handlers on both runtimes and executor types.
- [ ] Document the public API in the crate guides and provide a runnable fork/join example.
- [ ] Implement `parent.try_spawn(...)` and abort and drain accepted children on refusal.
- [ ] Simplify `SendHandler` forwarding while preserving the worker future’s `Send` bound.
- [ ] Measure and reduce CPU and memory per task for 1,000 and 10,000 children across spawn, join, cancel-and-drain, and abort-on-drop.

## v0.4.0 — Map collections

- [ ] Adapt CGP’s `BoundedParallel` from `feature/concurrent-handlers` at `afa7224a` (`crates/extra/cgp-handler-parallel/src/bounded.rs`). Reuse its `FuturesUnordered` refill loop and indexed result slots to map finite inputs with a positive concurrency limit and return results in input order.
- [ ] [Test bounded admission](TESTING.md#how-we-will-test-parallel-handlers) with empty input, a zero limit, and two gated handlers completing out of order.

## v0.5.0 — Run blocking work

- [ ] Adapt `CanOffloadBlocking` from the CGP runtime experiment at `95fb7af1` (`crates/cgp-runtime/src/core/blocking.rs`). Submit `Send + 'static` closures through Tokio’s `Handle::spawn_blocking` and Smol’s `unblock`, holding a slot from admission until the native closure exits.
- [ ] Test cancellation with one running closure and one waiting for admission. Verify the waiting closure never starts, drain waits for the running closure to exit, and async work can progress.

## v0.6.0 — Run dependency graphs

- [ ] Validate a fixed graph of heterogeneous nodes with dependency-only edges. Reject cycles and unknown dependencies before starting work.
- [ ] Run ready nodes within a concurrency limit and test that a diamond dependency waits for both predecessors.
- [ ] Stop admission on node failure, drain running siblings, and report the failure without starting dependent nodes.

## v0.7.0 — Connect handler stages

- [ ] Connect stages with bounded channels, propagate backpressure upstream, and drain accepted items when input closes.
- [ ] Test ordered delivery after a consumer resumes and verify the final item arrives before the pipeline returns.
- [ ] Test cancellation, downstream closure, and middle-stage failure during a pending send. Wake blocked operations, drain stages, and propagate the stage error.

## v0.8.0 — Map streams

- [ ] Map an open stream in completion order, counting active handlers and buffered results against one limit.
- [ ] Test early delivery, backpressure from a paused consumer, and cancellation and drain of in-flight work.

## v0.9.0 — Merge streams

- [ ] Merge a fixed set of inputs with round-robin selection among ready inputs. Skip closed inputs and finish when all are exhausted.
- [ ] Test wakeups, service of a newly ready input within one rotation, and delivery of buffered items before end-of-stream.

## v0.10.0 — Route stream items

- [ ] Route each item to a caller-selected bounded output in order. Pause intake while its destination is full.
- [ ] Test that other outputs can drain while a send waits and that closing the selected destination wakes the sender with an error.

## v0.11.0 — Balance jobs

- [ ] Dispatch to a worker with capacity and wait when all are full. Transfer ownership on enqueue so cancelling a waiting submission leaves the job unexecuted.
- [ ] Test dispatch past a saturated worker and shutdown during admission. Verify rejection before enqueue or exactly one terminal outcome for each accepted job.

## v0.12.0 — Partition jobs by key

- [ ] Keep each key on a fixed partition for the pool’s lifetime and process its jobs serially in acceptance order, including concurrent submissions.
- [ ] Test independent progress across partitions and bounded queues under skewed input and routing backpressure.
