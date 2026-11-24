# Release tasks

**v0.2.0 typed-join acceptance is complete.** The facade joins borrowed handlers in the caller's
future and returns every branch's typed result in declaration order.
Each release runs its acceptance checks deterministically on Tokio and Smol and passes the shared
[verification gates](CONTRIBUTING.md#verification). The [design](DESIGN.md) defines ownership,
execution boundaries, and the bare-metal check required for a cross-target `no_std` claim.

## v0.1.0 — Own a portable task from spawn to termination

### Completed

- [x] **Local spawn and await.** [`cgp-concurrency-runtime`](crates/rtes/cgp-concurrency-runtime)
  exports `CanSpawnLocalTask` and `TaskOutcome`. The shared
  [acceptance test](crates/tests/cgp-concurrency-tests/tests/task_ownership/local_spawn.rs)
  runs a generic caller on Tokio and Smol, holds an `Rc` across suspension, and recovers the
  same allocation as `Completed` through a mutable borrow of the owned handle.
- [x] **Conformance fixtures.** The [shared suite](crates/tests/cgp-concurrency-tests) uses a
  first-poll signal, a completion gate, and a destruction witness to check task ownership on both
  providers. Run it with `cargo nextest run --locked -p cgp-concurrency-tests --all-features`.
- [x] **Cross-thread spawn and await.** `CanSpawnTask` accepts a `Send + 'static` future and
  output. The [facade examples](crates/main/cgp-concurrency/README.md) compile with an `Arc` and
  verify that the compiler rejects an `Rc` held across an await at the cross-thread call.
- [x] **Tokio and Smol spawn adapters.** Both local executors accept an `Rc`. The
  [multithreaded cases](TESTING.md#run-the-conformance-suite) drive the public adapters on worker
  threads, recover a completed value, and observe reclamation after dropping a pending owner.

- [x] **Cancellation and failure.** Fallible spawn refuses a previously cancelled token.
  Accepted tasks can acknowledge or ignore cooperative requests. The owner can abort and drain
  them. Completion preserves application errors, and both adapters distinguish cancellation,
  an observed panic, and executor loss. [Facade API](crates/main/cgp-concurrency/README.md#cancellation-and-failure)

### Release acceptance

- [x] Shared facade scenarios passed in all four execution modes, including both race winners,
  requests before and after waiter registration, abort followed by drain, and reclamation after
  drop. Both adapters preserved the refusal when cleanup panicked and reclaimed unclaimed task
  results when their destructors panicked. Polling a pending owner left unrelated queued work untouched.
- [x] The workspace builds against published CGP v0.8.0. The portable caller passed with each
  provider enabled separately and both together. The [full release checks](CONTRIBUTING.md#verification) passed.
- [x] Both spawn components passed through [checked CGP application wiring](crates/tests/cgp-concurrency-tests/tests/task_ownership/app.rs).
  The caller converted a refusal into its own error type and recovered an application error inside `Completed`.
  Drain preserved the original polling panic when task destruction also panicked.

## v0.2.0 — Join typed handlers in the current task

### Completed

- [x] [`JoinHandlers`](crates/main/cgp-concurrency/src/join.rs) joins a fixed list of handlers in
  the caller's future. Branches may borrow local data and hold non-`Send` values across suspension.
- [x] The [facade acceptance test](crates/tests/cgp-concurrency-tests/tests/typed_join.rs) held both
  branches pending, finished the second with an application error, and recovered both typed results
  in declaration order. Both gates woke the caller on Tokio, on Smol, and with no runtime feature.
- [x] Store one pending future or completed result per branch. With 1 KiB outputs at 4, 8, and 16
  branches, the [layout regression test](crates/tests/cgp-concurrency-tests/tests/typed_join_layout.rs)
  measured 4,160, 8,288, and 16,544 bytes, down from 10,480, 37,328, and 140,240 bytes
  (optimized Rust 1.100.0 on aarch64 macOS).

### Release acceptance

- [x] The [three-branch scenario](crates/tests/cgp-concurrency-tests/tests/typed_join/interleaved.rs)
  held all branches pending, completed them middle, last, then first, and recovered two distinct
  application errors and a borrowed value in declaration order on all three callers.
- [x] The [facade doctest](crates/main/cgp-concurrency/README.md#join-typed-handlers) runs the join
  inside `PipeHandlers`. The reporting handler recovers the borrowed label with both a valid and
  an invalid port.
- [x] The typed-join [PR checks](https://github.com/anrim/cgp-concurrency/pull/1/checks) cover
  the no-runtime join, each provider separately, and the full workspace suite.

## v0.3.0 — Run handler branches as owned child tasks

- [ ] Apply the typed join to independently spawned children using v0.1.0 handles. Support local
  children and recover `Send` only for work crossing threads.
- [ ] Cancel a fork with children pending. Verify that admission stops and the parent drains every
  child before returning. Panic one child in a separate run and verify the join reports its panic
  and each sibling's terminal outcome in branch order.
- [ ] Fail a later spawn after one child has started. Stop spawning, abort and drain the owned
  children, then return the spawn failure.

## v0.4.0 — Map a collection with bounded concurrency

- [ ] Run one handler over a finite input sequence with a positive concurrency limit. Return every
  result in input order, including item errors.
- [ ] Reject a zero limit before starting work. Fill all slots, then finish items out of order,
  including a failure. Check peak concurrency and the returned sequence. The limit bounds active
  handlers. The result collection grows with the input. Empty input starts no work.

## v0.5.0 — Run blocking closures with bounded admission

- [ ] Expose a portable blocking operation through `cgp-concurrency-runtime` that submits
  `Send + 'static` closures to each provider's native pool. Reserve a slot before submission
  and hold it until the native job terminates, including time queued in the pool.
- [ ] With one slot, hold a running closure at a barrier and a second submission waiting for admission.
  Request cancellation of both. The waiting closure never starts, and drain returns only after
  the running closure exits. Verify an async task makes progress while the closure is blocked.

## v0.6.0 — Run a finite dependency graph

- [ ] Validate a fixed graph of heterogeneous nodes with dependency-only edges. Reject a cycle or
  unknown dependency before any node starts.
- [ ] Run ready nodes within a concurrency limit. In a diamond graph, hold one predecessor pending
  after the other finishes and verify their dependent waits.
- [ ] Fail a node while a sibling runs. Stop admission, drain running nodes, and report the failure.
  Verify the failed node's dependents never start.

## v0.7.0 — Connect handlers in a bounded linear pipeline

- [ ] Connect a chain of handler stages with bounded channels. Propagate backpressure upstream and
  drain accepted items on normal input close.
- [ ] Pause the consumer until a send waits, then resume it and observe delivery in order. Close
  input and verify the last item arrives before the pipeline returns.
- [ ] With a send pending, cancel the pipeline, close the downstream stage, and fail a middle stage
  in separate runs. Verify blocked operations wake and every stage exits. In the failure case,
  return the stage error after draining.

## v0.8.0 — Transform a stream concurrently

- [ ] Apply a handler to an open stream and yield ready results as they finish. Count running work
  and buffered results against one limit so a slow consumer bounds upstream admission.
- [ ] Hold the first item pending and observe a later result before the input closes. Pause the
  consumer, verify admission stops at the limit, then cancel and drain the in-flight work.

## v0.9.0 — Merge ready streams fairly

- [ ] Merge a fixed set of inputs into one output. Use round-robin selection among ready inputs,
  skip closed inputs, and end the output when all inputs are exhausted.
- [ ] Start with all inputs pending and make one ready to verify the merger wakes. Keep it busy
  while a second becomes ready and serve the second within one rotation. Close both with items
  buffered and verify those items arrive before end-of-stream.

## v0.10.0 — Route stream items to fixed destinations

- [ ] Route each item to one caller-selected bounded output. Preserve order within each output.
  When the selected output fills, pause upstream intake until the pending item can be delivered.
- [ ] Fill one output and route another item to it. Verify the item waits for that destination,
  other consumers can drain already-routed items, and closing the destination wakes the pending
  sender with an error.

## v0.11.0 — Balance jobs across a fixed local worker pool

- [ ] Dispatch to a worker with capacity and wait when all are full. Enqueuing the job transfers
  ownership to the pool. Cancelling a submission before enqueue leaves the job unexecuted.
- [ ] Saturate one worker and verify another accepts the next job. Race shutdown against a waiting
  submission. Check that it is either rejected before enqueue or produces one terminal outcome.
  Verify every previously accepted job also reports exactly one outcome.

## v0.12.0 — Process jobs in order by key

- [ ] Map each key to a fixed partition for the lifetime of the pool. Process its jobs serially in
  acceptance order, including when two callers submit the same key concurrently.
- [ ] Hold one job pending and submit another with the same key. Verify it waits while an admitted
  job in another partition progresses. Skew input toward one partition and verify its queue stays
  bounded under the routing backpressure policy.

## Deferred work

Remote execution, detached tasks, supervisor trees, retry, hedging, speculative execution, full
Rayon semantics, library-owned work stealing, live typed values on graph edges, dynamic worker
membership, adaptive balancing, partition resizing, and durable stream state remain deferred.
Revisit an item when it has an owner, a portable contract, and independent user value.
