# Design

This document records stable ownership and execution contracts. For the release sequence, see the
[release tasks](TASKS.md). Unless a section says
otherwise, these are constraints for planned APIs rather than claims that the APIs already exist.

## Dependency direction and ownership

Dependencies point in one direction:

```text
cgp-concurrency → cgp-concurrency-runtime
cgp-concurrency → published cgp-core and cgp-handler
cgp-concurrency → optional Tokio / Smol adapter → cgp-concurrency-runtime
cgp-concurrency-runtime → published cgp-core and cgp-runtime
```

| Layer | Owns | Does not own |
| --- | --- | --- |
| CGP's `cgp-runtime` | Runtime selection through `HasRuntime` and `HasRuntimeType` | This workspace's task lifecycle |
| `cgp-concurrency-runtime` | Portable task lifecycle and future portable operations for blocking and channels | Native executor mechanics |
| `cgp-concurrency` | Structured handler combinators and scheduling policy | Process execution and runtime internals |
| Tokio and Smol providers | Adapters from portable traits to native operations | Cross-runtime policy |

A runtime primitive belongs in `cgp-concurrency-runtime` when a public operation needs it and both
providers can implement the same contract.

Native join, select, blocking, and channel operations are mechanisms. Handler composition, ordering,
failure policy, admission, DAG scheduling, and stream topology are policies owned here.

Providers map portable operations to native runtime primitives. They do not recreate executor
polling loops, drive unrelated executor work while awaiting a handle, or reimplement portable
orchestration policy.

## Workspace boundaries

| Crate | Boundary |
| --- | --- |
| `cgp-concurrency` | `no_std` facade and optional provider re-exports |
| `cgp-concurrency-runtime` | `no_std` task contracts shared by the facade and both adapters |
| `cgp-concurrency-tokio` | Tokio mechanics; may require `std` |
| `cgp-concurrency-smol` | Smol mechanics; may require `std` |
| `cgp-concurrency-tests` | Unpublished cross-provider conformance suite |

The contract and both adapters live under `crates/rtes/`. Adapters depend directly on the
contract crate so the facade can select either adapter without a dependency cycle.

## Execution contracts

### Work remains structured

- Every spawned task has an owner and exactly one terminal outcome.
- Cooperative cancellation, forced abort, and waiting for termination are distinct operations.
- Fallible spawn checks the cooperative token at admission. A prior request returns
  `SpawnError::Cancelled` before scheduling. Accepted work can acknowledge the request after cleanup
  or finish with a value. A shared token broadcasts to every attached task.
- Each owner has a separate abort signal. An abort wakes the scheduled future, which drops
  pending work at a poll boundary. Drain observes termination through the native join handle.
- Dropping an owned handle requests abortion; the provider remains responsible for reclamation.
- No public operation silently detaches work.
- Completion racing with cancellation reports the terminal state that actually wins.

### Concurrency lanes remain distinct

- Cooperative joins may borrow data and use non-`Send` futures in the current task.
- Local spawning accepts non-`Send` work only on a compatible local executor.
- `CanSpawnTask` submits a `Send + 'static` future with a `Send + 'static` output. Its owner
  can move across threads. Tokio uses the supplied runtime handle and Smol uses the supplied executor.
- `Sync` is required only when referenced state is genuinely shared across threads.
- Blocking work has its own `Send + 'static` closure boundary and runs on the provider's blocking pool.

Send requirements are recovered at the operation boundary, following CGP's `CanSendRun` pattern.
Handler traits and context types do not receive blanket `Send + Sync` bounds.

### Typed join in the caller's task

The facade's `JoinHandlers` pairs a fixed product of CGP handlers with a product of inputs.
The caller's future drives all branches and retains one typed result per completed branch.
Awaiting the join returns every result in declaration order, including application errors.
Dropping the join discards retained results and drops pending branch futures.

### Fork handlers

`CanSpawnHandlerTasks` separates task submission from joining. The parent wires
`HandlerTaskSpawnerComponent` to either `SpawnLocalTasks<Jobs>` or `SpawnWorkerTasks<Jobs>`.
Both take the same product of handler providers and use the parent's selected runtime. Each
child owns an input and a clone of the child context. Worker handlers recover the future's
`Send` bound through `SendHandler`. Local handlers can retain non-`Send` state.

`spawn` submits every child synchronously through the infallible spawn operations,
releases the original child context, and returns a `HandlerTasks` group. The executor can run
the children as soon as they are submitted. Both `tasks.join().await` and `tasks.await` return one
`TaskOutcome<Result<Output, Error>>` per branch in declaration order. Dropping the group
or an unfinished join releases the handles under their abort-on-drop contract. On native
runtimes with unwinding enabled, the group guards each retained outcome until it returns the
whole product. Each branch finishes its handler and releases its context clone before reporting
the first panic from the handler or cleanup.

For cooperative cancellation, `spawn_cancellable` gives the child-context constructor a fresh
`CancellationToken` and uses the same configured spawner. The returned group owns a clone
of the token. Calling `tasks.cancel()` signals it immediately and returns the join future.
Handlers can observe the request, perform async cleanup, and return their application results.
The join waits for every child even if a handler ignores the request. Dropping a pending
cancellation wait releases retained outcomes and requests abort for every unfinished child,
even during cleanup. The spawner's `Join` associated type lets a generic caller return
the owned wait after dropping the parent.

### Fork and join at the call site

The Smol-inspired fork API puts these goals into practice. `parent.spawn(code, child, inputs)`
submits work and returns the group that owns it. `tasks.join().await` shows where the caller
waits. Provider wiring selects local or worker execution, so both use the same call site.
[Runnable example](crates/main/cgp-concurrency/examples/fork_handlers.rs)

### Infrastructure and application outcomes remain separate

The portable task outcome has four variants.

```text
TaskOutcome<T>
├── Completed(T)
├── Cancelled
├── Panicked(Option<String>)
└── RuntimeFailure
```

Application errors remain inside `T`, commonly as `Completed(Result<Output, Error>)`. Panic detail
is available only where the runtime and unwind mode can observe it.

With unwinding enabled, both native adapters contain destructor panics when disposing of refused
work or an unclaimed result. A prior cancellation still yields `SpawnError::Cancelled`. Draining a
completed task transfers the value to the caller, who then owns its destruction.
If both polling the submitted future and subsequently dropping it panic, the owner receives
the polling panic. If the future returned a value and its destructor panics, the owner receives
the cleanup panic and the adapter discards the value.

### Bounds and ordering are explicit

Every public combinator documents:

- declaration, input, or completion ordering;
- what its concurrency and capacity limits cover;
- whether buffers count toward those limits;
- how errors affect admission and running work;
- whether cancellation can discard completed results; and
- graceful and forced shutdown behavior.

## Platform and feature boundaries

The facade source remains `#![no_std]` and has no default runtime. The `tokio` and `smol` features
independently select provider re-exports, and all four combinations must compile:

| Runtime features | Expected result |
| --- | --- |
| Neither | Portable facade only |
| Tokio only | Tokio provider, no Smol dependency |
| Smol only | Smol provider, no Tokio dependency |
| Both | Both providers without changing portable APIs |

Source-level `no_std` discipline is not a cross-target support claim. That claim requires the full
CGP dependency chain and a bare-metal target check to pass. All crates forbid unsafe code.

## Conformance strategy

Both providers must satisfy the same ownership, ordering, capacity, and shutdown contracts.
[TESTING.md](TESTING.md) describes the executor configurations and how the suite controls
branch progress to check those guarantees.

## Changing the design

A change to a public contract updates this document, API documentation, deterministic conformance
tests, and the [release tasks](TASKS.md) when sequencing changes. This repository owns
the reference documentation for its task components and releases them independently from CGP.
