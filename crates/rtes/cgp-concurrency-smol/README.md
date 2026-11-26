# cgp-concurrency-smol

Run local and worker tasks on a Smol executor through the portable task traits.

## Quick start

The example uses `cgp-concurrency-smol` and Smol 2. If your application uses
the facade with its `smol` feature, import the adapters and traits from
`cgp_concurrency::smol` in place of `cgp_concurrency_smol`.

```rust
use cgp_concurrency_smol::{runtime::{CanSpawnLocalTask, TaskOutcome}, SmolLocalSpawner};
use smol::LocalExecutor;

fn main() {
    let executor = LocalExecutor::new();
    let task = SmolLocalSpawner(&executor).spawn_local_task(async { 42 });
    assert_eq!(smol::block_on(executor.run(task)), TaskOutcome::Completed(42));
}
```

## API reference

The adapters implement the consumer traits directly. Import the traits from this crate’s `runtime` module.

| Consumer trait | Provider trait | Adapter | Returned handle |
| --- | --- | --- | --- |
| `CanSpawnLocalTask` | `LocalTaskSpawner` | `SmolLocalSpawner(&local_executor)` | `SmolLocalTask<T>` |
| `CanSpawnTask` | `TaskSpawner` | `SmolSpawner(&executor)` | `SmolTask<T>` |

Drive the `LocalExecutor` on its owning thread for tasks that can hold non-`Send`
values. Drive the `Executor` on one or more threads for `Send` worker tasks.

`SmolLocalTask<T>` is an alias for `SmolTask<T>`. Both implement `TaskHandle<T>`.
Await the handle for a `TaskOutcome<T>`, or use `request_cancel()`, `abort()`, and
`drain().await` to manage its lifetime. Dropping a pending handle requests abort.

## Implement a local provider

Forward both `LocalTaskSpawner` methods to the app’s `LocalExecutor` to run a task that returns an `Rc`.

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency_smol::{runtime::*, SmolLocalSpawner, SmolLocalTask};
use smol::LocalExecutor;
use std::rc::Rc;

struct App(LocalExecutor<'static>);

#[cgp_impl(new SpawnOnSmol)]
impl LocalTaskSpawner for App {
    type LocalJoinHandle<T: 'static> = SmolLocalTask<T>;

    fn spawn_local_task<F, T>(&self, work: F) -> SmolLocalTask<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        SmolLocalSpawner(&self.0).spawn_local_task(work)
    }

    fn try_spawn_local_task<F, T>(&self, token: &CancellationToken, work: F)
        -> Result<SmolLocalTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        SmolLocalSpawner(&self.0).try_spawn_local_task(token, work)
    }
}

delegate_and_check_components! { App { LocalTaskSpawnerComponent: SpawnOnSmol } }

let app = App(LocalExecutor::new());
let value = Rc::new(42);
let task = app.spawn_local_task(async move { value });
assert_eq!(smol::block_on(app.0.run(task)), TaskOutcome::Completed(Rc::new(42)));
```
