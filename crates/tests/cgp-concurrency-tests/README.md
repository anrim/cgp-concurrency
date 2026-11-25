# cgp-concurrency-tests

This unpublished crate tests and benchmarks the public facade with Tokio and Smol on
local and worker executors.

## Quick start

From the workspace root, run the conformance suite, time groups of one and eight, or
measure CPU per child for batches of 1,000 and 10,000. [Benchmark guide](../../../TESTING.md#benchmark-spawn-and-join)

```sh
cargo nextest run --locked -p cgp-concurrency-tests --all-features
cargo bench --locked -p cgp-concurrency-tests --all-features --bench spawn_join
cargo bench --locked -p cgp-concurrency-tests --features tokio,smol --bench task_batches
```

## Test coverage

| Consumer traits exercised | Providers | What the tests verify |
| --- | --- | --- |
| `HasRuntimeType`, `HasRuntime` | `UseType<R>`, `UseFields` | The same app can select and borrow either runtime. |
| `CanSpawnLocalTask`, `CanSpawnTask` | `SpawnOnRuntime` | Owned tasks can progress before join, acknowledge cancellation, abort, and drain. |
| `CanSpawnHandlerTasks`, `CanSpawnCancellableHandlerTasks` | `SpawnLocalTasks`, `SpawnWorkerTasks` | Forked children return ordered outcomes, including after group cancellation. |
| `CanComputeAsync` | `JoinHandlers` | The caller drives each branch and receives its typed result in declaration order. |

## Implement a test handler

Use a channel to control when the handler finishes. This test checks that the join stays
pending until the test sends the value.

```rust
extern crate cgp_core as cgp;
use async_channel::{bounded, Receiver};
use cgp::prelude::*;
use cgp_concurrency::JoinHandlers;
use cgp_handler::{AsyncComputerComponent, CanComputeAsync, Handler, HandlerComponent};

struct App;
impl HasErrorType for App { type Error = async_channel::RecvError; }

#[cgp_impl(new ReadValue)]
impl Handler<(), Receiver<u32>> for App {
    type Output = u32;

    async fn handle(&self, _: PhantomData<()>, input: Receiver<u32>)
        -> Result<u32, async_channel::RecvError> {
        input.recv().await
    }
}

delegate_and_check_components! {
    App {
        #[check_params(((), Product![Receiver<u32>]))]
        AsyncComputerComponent: JoinHandlers<Product![ReadValue]>,
    }
}

futures_lite::future::block_on(async {
    let (send, recv) = bounded(1);
    let join = App.compute_async(PhantomData::<()>, product![recv]);
    let mut join = core::pin::pin!(join);
    assert!(futures_lite::future::poll_once(&mut join).await.is_none());
    send.try_send(42).unwrap();
    assert_eq!(join.await, product![Ok(42)]);
});
```
