# cgp-concurrency

The facade exposes local task spawning and upstream CGP runtime selection through
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

The [shared acceptance tests](../../tests/cgp-concurrency-tests/tests/task_ownership/local_spawn.rs)
exercise this contract on both local executors. The [release checklist](../../../TASKS.md)
tracks cancellation and cross-thread support.
