# cgp-concurrency-runtime

This `no_std` crate defines the portable task contract for CGP Concurrency. It re-exports
runtime selection from the published `cgp-runtime` crate. Applications can import both
through `cgp_concurrency::runtime`.

Use `CanSpawnLocalTask` to submit an owned `'static` future to a local executor. The future,
output, and handle may be non-`Send`. The executor can run the task before the first wait.
Await `&mut owner` to recover `TaskOutcome::Completed(value)`. Interrupting a pending borrowed
wait preserves the owner. Dropping the owner of a pending task requests abortion, and the
executor must keep running to reclaim it.
