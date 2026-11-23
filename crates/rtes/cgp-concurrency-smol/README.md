# cgp-concurrency-smol

Use `SmolLocalSpawner(&executor)` to submit owned local tasks to a Smol `LocalExecutor`. Drive
the executor through completion or cleanup. Dropping the portable owner cancels its native task.
Use `SmolSpawner(&executor)` with a `smol::Executor` to submit a `Send` future. The caller
drives that executor on its worker threads. Both owners report an observed panic as `Panicked` and executor loss as `RuntimeFailure`.

See the repository's [design](../../../DESIGN.md) for the provider contract.
