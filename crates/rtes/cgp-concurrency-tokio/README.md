# cgp-concurrency-tokio

Use `TokioLocalSpawner(&local_set)` to submit owned local tasks to a Tokio `LocalSet`. Drive the
set through completion or cleanup. Dropping the portable owner aborts its pending task.
Use `TokioSpawner(runtime.handle())` to submit a `Send` future to a specific Tokio runtime,
including from outside that runtime. On a multithread runtime its workers drive the task.
Both owners propagate a task panic and panic if shutdown prevents a task from returning.

See the repository's [design](../../../DESIGN.md) for the provider contract.
