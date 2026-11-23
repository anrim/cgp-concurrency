# cgp-concurrency

The facade exposes local and cross-thread task spawning and upstream CGP runtime selection through
`cgp_concurrency::runtime` and `cgp_concurrency::prelude`. Enable a runtime feature to use its
adapter. The default feature set is empty.

| Feature | Effect |
| --- | --- |
| `tokio` | Re-export `cgp-concurrency-tokio` |
| `smol` | Re-export `cgp-concurrency-smol` |

A generic caller can move an `Rc` into a task and wait through a mutable borrow of its owner.

```rust
use core::future::Future;
use std::rc::Rc;

use cgp_concurrency::prelude::{CanSpawnLocalTask, TaskOutcome};

async fn round_trip(
    runtime: &impl CanSpawnLocalTask,
    value: Rc<str>,
    gate: impl Future<Output = ()> + 'static,
) -> TaskOutcome<Rc<str>> {
    let mut owner = runtime.spawn_local_task(async move {
        gate.await;
        value
    });
    (&mut owner).await
}
```

Use `CanSpawnTask::spawn_task` to submit a `Send + 'static` future with a `Send + 'static`
output. The returned owner can also move between threads. Both spawn operations return
`TaskOutcome::Completed` on completion and request cancellation when a pending owner is dropped.

```rust
use core::future::Future;
use std::sync::Arc;
use cgp_concurrency::prelude::{CanSpawnTask, TaskOutcome};

async fn read_on_worker(runtime: &impl CanSpawnTask, gate: impl Future<Output = ()> + Send + 'static)
    -> TaskOutcome<u32>
{
    runtime.spawn_task(async move {
        let value = Arc::new(42_u32);
        gate.await;
        *value
    }).await
}
```

Holding an `Rc` across the await makes the future itself non-`Send`, even when its output is a `u32`.

```compile_fail
use core::future::Future;
use std::rc::Rc;
use cgp_concurrency::prelude::{CanSpawnTask, TaskOutcome};

async fn read_on_worker(runtime: &impl CanSpawnTask, gate: impl Future<Output = ()> + Send + 'static)
    -> TaskOutcome<u32>
{
    runtime.spawn_task(async move {
        let value = Rc::new(42_u32);
        gate.await;
        *value
    }).await
}
```

Run `cargo test --locked -p cgp-concurrency --doc` to check the cross-thread type boundary.
