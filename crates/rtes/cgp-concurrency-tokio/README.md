# cgp-concurrency-tokio

Run local and worker tasks on a Tokio runtime through the portable task traits.

## Quick start

The example uses `cgp-concurrency-tokio` and Tokio 1 with the `rt` feature. If your application uses
the facade with its `tokio` feature, import the adapters and traits from
`cgp_concurrency::tokio` in place of `cgp_concurrency_tokio`.

```rust
use cgp_concurrency_tokio::{runtime::{CanSpawnTask, TaskOutcome}, TokioSpawner};
use tokio::runtime::Builder;

fn main() {
    let runtime = Builder::new_current_thread().build().unwrap();
    let task = TokioSpawner(runtime.handle()).spawn_task(async { 42 });
    assert_eq!(runtime.block_on(task), TaskOutcome::Completed(42));
}
```

## API reference

The adapters implement the consumer traits directly. Import the traits from this crate’s `runtime` module.

| Consumer trait | Provider trait | Adapter | Returned handle |
| --- | --- | --- | --- |
| `CanSpawnLocalTask` | `LocalTaskSpawner` | `TokioLocalSpawner(&local_set)` | `TokioLocalTask<T>` |
| `CanSpawnTask` | `TaskSpawner` | `TokioSpawner(runtime.handle())` | `TokioTask<T>` |

Drive the `LocalSet` on its owning thread for tasks that can hold non-`Send` values. The
selected Tokio runtime drives worker tasks. Its `Handle` accepts submissions from outside
the runtime.

`TokioLocalTask<T>` is an alias for `TokioTask<T>`. Both implement `TaskHandle<T>`.
Await the handle for a `TaskOutcome<T>`, or use `request_cancel()`, `abort()`, and
`drain().await` to manage its lifetime. Dropping a pending handle requests abort.

## Implement a worker provider

Forward both `TaskSpawner` methods to the Tokio `Handle` stored by the application.

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency_tokio::{runtime::*, TokioSpawner, TokioTask};
use tokio::runtime::{Builder, Handle};

struct App(Handle);

#[cgp_impl(new SpawnOnTokio)]
impl TaskSpawner for App {
    type JoinHandle<T: Send + 'static> = TokioTask<T>;

    fn spawn_task<F, T>(&self, work: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioSpawner(&self.0).spawn_task(work)
    }

    fn try_spawn_task<F, T>(&self, token: &CancellationToken, work: F)
        -> Result<TokioTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static,
    {
        TokioSpawner(&self.0).try_spawn_task(token, work)
    }
}

delegate_and_check_components! { App { TaskSpawnerComponent: SpawnOnTokio } }

let runtime = Builder::new_current_thread().build().unwrap();
let app = App(runtime.handle().clone());
let task = app.spawn_task(async { 42 });
assert_eq!(runtime.block_on(task), TaskOutcome::Completed(42));
```
