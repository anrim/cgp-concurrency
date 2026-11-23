# Design

This document records stable ownership and execution contracts. For the release sequence, see the
[release tasks](TASKS.md). Unless a section says
otherwise, these are constraints for planned APIs rather than claims that the APIs already exist.

## Dependency direction and ownership

Dependencies point in one direction:

```text
cgp-concurrency → cgp-concurrency-runtime
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
- Cancellation stops admission, requests cancellation or abortion, and drains every owned task.
- Dropping an owned handle requests abortion; the provider remains responsible for reclamation.
- No public operation silently detaches work.
- Completion racing with cancellation reports the terminal state that actually wins.

### Concurrency lanes remain distinct

- Cooperative joins may borrow data and use non-`Send` futures in the current task.
- Local spawning accepts non-`Send` work only on a compatible local executor.
- Work crossing a multithreaded boundary requires `Send + 'static`.
- `Sync` is required only when referenced state is genuinely shared across threads.
- Blocking work has its own `Send + 'static` closure boundary and runs on the provider's blocking pool.

Send requirements are recovered at the operation boundary, following CGP's `CanSendRun` pattern.
Handler traits and context types do not receive blanket `Send + Sync` bounds.

### Infrastructure and application outcomes remain separate

The portable infrastructure outcome is conceptually:

```text
TaskOutcome<T>
├── Completed(T)
├── Cancelled
├── Panicked(...)
└── RuntimeFailure(...)
```

Application errors remain inside `T`, commonly as `Completed(Result<Output, Error>)`. Panic detail
is available only where the runtime and unwind mode can observe it.

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
