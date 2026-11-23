# cgp-concurrency-tokio

Use `TokioLocalSpawner(&local_set)` to submit owned local tasks to a Tokio `LocalSet`. Drive the
set through completion or cleanup. Dropping the portable owner aborts its pending task.
Waiting propagates a task panic and also panics if the set shuts down before the task returns.

See the repository's [design](../../../DESIGN.md) for the provider contract.
