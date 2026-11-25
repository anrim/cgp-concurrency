# cgp-concurrency-runtime

This `no_std` crate defines the portable task contract for CGP Concurrency. It re-exports
runtime selection from the published `cgp-runtime` crate. Applications can import both
through `cgp_concurrency::runtime`.

Use `CanSpawnLocalTask` to submit an owned `'static` future to a local executor. The future,
output, and handle may be non-`Send`. The executor can run the task before the first wait.
Await `&mut owner` to recover `TaskOutcome::Completed(value)`. Interrupting a pending borrowed
wait preserves the owner. Dropping the owner of a pending task requests abortion, and the
executor must keep running to reclaim it.

Use `CanSpawnTask::spawn_task` for work that may cross threads. The future, its output, and the
owned handle are all `Send + 'static`. The handle has the same completion and drop behavior as
the local handle.

Both spawn traits also offer fallible admission with a `CancellationToken`. Use
`TaskHandle::request_cancel` for a cooperative request, `abort` to stop pending work at a poll
boundary, and `drain` to await termination. The outcome distinguishes completed application
values, cancellation, an observed panic, and executor failure.

Both native adapters enable the optional `std` feature to convert unwind payloads into task
outcomes. String payloads retain their message. An opaque payload yields `Panicked(None)`
even if its destructor panics. If that destructor itself panics with another opaque payload,
the adapter leaves the secondary payload allocated to avoid recursively running arbitrary destructors.

When the task also owns context borrowed by the handler, construct the handler future with
`TaskResult::capture`, await `run()`, and call `finish(|| drop(context))`. The first panic
from construction, polling, or destruction is reported after the context has been released.
Use `await_with_cleanup` inside a task that drives a nested handler future. With `std`
it completes or catches a poll panic, destroys the future, and then delivers the output
or resumes the original panic for the adapter to report.
