# cgp-concurrency-runtime

This `no_std` crate defines owned tasks and re-exports runtime selection from CGP's
`cgp-runtime`. Applications can import the full API through `cgp_concurrency::runtime`.

## Quick start

A caller can accept any runtime that implements `CanSpawnTask`. Awaiting its handle
yields the task’s terminal outcome.

```rust
use cgp_concurrency_runtime::{CanSpawnTask, TaskOutcome};

async fn answer(runtime: &impl CanSpawnTask) -> TaskOutcome<u32> {
    runtime.spawn_task(async { 42 }).await
}
```

## API reference

### Runtime consumer traits

| Consumer trait | Provider trait | Caller API |
| --- | --- | --- |
| `HasRuntimeType` | `RuntimeTypeProvider` | `RuntimeOf<App>` names the runtime selected for `App`. |
| `HasRuntime` | `RuntimeGetter` | `app.runtime()` borrows that runtime. |
| `CanSpawnLocalTask` | `LocalTaskSpawner` | `spawn_local_task` and `try_spawn_local_task` submit work with a `LocalJoinHandle<T>`. |
| `CanSpawnTask` | `TaskSpawner` | `spawn_task` and `try_spawn_task` submit work with a `JoinHandle<T>`. |
| `TaskHandle<T>` | Implement on the returned handle. | Await the owner or call `drain().await` through a mutable borrow. |

Both spawn traits accept `'static` futures and outputs. Worker futures, outputs, and handles
must also be `Send`. Accepted work can run before the handle is polled.

### Cancellation and outcomes

Create a token with `CancellationToken::new()` and give a clone to the work submitted
with `try_spawn_local_task` or `try_spawn_task`. A prior request rejects admission with
`SpawnError::Cancelled`. After admission, `request_cancel()` on the token or owner signals every clone.
`is_cancelled()` stays true and `cancelled().await` yields `Cancelled`, including for
later waiters. The task can clean up and return `Err(Cancelled)` or finish with `Ok(value)`.

| `TaskOutcome<T>` | Meaning |
| --- | --- |
| `Completed(T)` | The task returned its value, including any application-level `Result`. |
| `Cancelled` | The task acknowledged cancellation or was aborted. |
| `Panicked(Option<String>)` | The task unwound. String payloads retain their message. |
| `RuntimeFailure` | The executor discarded accepted work before it produced an outcome. |

`abort()` stops pending work at a poll boundary and preserves an already completed outcome.
Dropping a pending owner also requests abort. Keep the executor running to reclaim the task.
A pending `drain()` can be retried and yields one terminal outcome after the task future is destroyed.
Call `outcome.discard()` for an unwanted result or `discard(value)` for an unused input.
With `std` and unwinding enabled, each call contains a panic from dropping that value so
cleanup can continue.

## Implementing a provider

### Implement a local spawn provider

Forward both methods to the runtime selected by `HasRuntime`. For a worker provider,
implement `TaskSpawner` and wire `TaskSpawnerComponent`.

```rust
extern crate cgp_core as cgp;
use cgp::component::UseFields;
use cgp::prelude::*;
use cgp_concurrency_runtime::*;

#[derive(HasField)]
struct App<R> { runtime: R }

#[cgp_impl(new SpawnOnRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl LocalTaskSpawner
where
    Self: HasRuntime,
    Runtime: CanSpawnLocalTask,
{
    type LocalJoinHandle<T: 'static> = <Runtime as CanSpawnLocalTask>::LocalJoinHandle<T>;

    fn spawn_local_task<F, T>(&self, work: F) -> Self::LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        self.runtime().spawn_local_task(work)
    }

    fn try_spawn_local_task<F, T>(&self, token: &CancellationToken, work: F)
        -> Result<Self::LocalJoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        self.runtime().try_spawn_local_task(token, work)
    }
}

delegate_and_check_components! {
    <R: CanSpawnLocalTask> App<R> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
        LocalTaskSpawnerComponent: SpawnOnRuntime,
    }
}
```

### Implement a native handle

When the task also owns context borrowed by the handler, construct the handler future with
`TaskResult::capture`, await `run()`, and call `finish(|| drop(context))`. The first panic
from construction, polling, or destruction is reported after the context has been released.
Use `await_with_cleanup` inside a task that drives a nested handler future. With `std`
it completes or catches a poll panic, destroys the future, and then delivers the output
or resumes the original panic for the adapter to report.

Use `TaskControl::returning_value` for ordinary work and `TaskControl::prepare` to check
admission and wrap the future. Expose the control via `TaskHandle::control` and arrange
for dropping the owner to abort pending work.

The following helpers require the `std` feature, which both native adapters enable.
`TaskCompletion::track` wraps the future and guards its output as `UnclaimedOutput<T>`
until the native join completes. Call
`TaskCompletion::finish` after task destruction to get the terminal outcome.
`TaskOutcome::from_panic` converts a native unwind and `panic_message` extracts an
optional string from its payload.

If polling and cleanup both panic, the polling panic wins. If only cleanup panics,
the handle reports that panic even when the task had returned a value.
