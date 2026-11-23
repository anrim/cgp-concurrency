# cgp-concurrency-smol

Use `SmolLocalSpawner(&executor)` to submit owned local tasks to a Smol `LocalExecutor`. Drive
the executor through completion or cleanup. Dropping the portable owner cancels its native task.
Waiting propagates a task panic and also panics if the executor drops the task before it returns.

See the repository's [design](../../../DESIGN.md) for the provider contract.
