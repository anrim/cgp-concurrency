# cgp-concurrency

The facade exposes local and cross-thread task spawning and upstream CGP runtime selection through
`cgp_concurrency::runtime` and `cgp_concurrency::prelude`. Enable a runtime feature to use its
adapter. The default feature set is empty.

| Feature | Effect |
| --- | --- |
| `tokio` | Re-export `cgp-concurrency-tokio` |
| `smol` | Re-export `cgp-concurrency-smol` |

## Join typed handlers

`JoinHandlers<Product![A, B, …]>` polls all handlers in the caller's future. Wire it to
CGP's `AsyncComputerComponent` and pass one input per handler in the same order. Each output
slot holds that handler's own `Result`, even if another handler returns an error first. The
join needs no runtime feature and accepts borrowed inputs and non-`Send` futures.

This pipeline keeps a borrowed server label and parses a port. The next handler receives both
results and reports the port as optional, so an invalid port still produces a labeled report.
`Promote` lets the join serve as the first handler in `PipeHandlers`.

```rust
extern crate cgp_core as cgp;

use core::num::ParseIntError;
use cgp::base::types::Cons;
use cgp::prelude::*;
use cgp_concurrency::prelude::JoinHandlers;
use cgp_handler::{CanHandle, Handler, HandlerComponent, PipeHandlers, Promote, ReturnInput};

struct App;
impl HasErrorType for App {
    type Error = ParseIntError;
}

#[cgp_impl(new ParsePort)]
#[use_type(HasErrorType.Error = ParseIntError)]
impl<'a> Handler<(), &'a str> {
    type Output = u16;

    async fn handle(&self, _: PhantomData<()>, port: &'a str) -> Result<u16, Error> {
        port.parse()
    }
}

type Joined<'a> = Product![Result<&'a str, ParseIntError>, Result<u16, ParseIntError>];

#[cgp_impl(new MakeReport)]
#[use_type(HasErrorType.Error = ParseIntError)]
impl<'a> Handler<(), Joined<'a>> {
    type Output = (&'a str, Option<u16>);

    async fn handle(&self, _: PhantomData<()>, results: Joined<'a>) -> Result<Self::Output, Error> {
        let Cons(label, Cons(port, _)) = results;
        Ok((label?, port.ok()))
    }
}

type ReadEndpoint = JoinHandlers<Product![ReturnInput, ParsePort]>;

delegate_and_check_components! {
    App {
        #[check_params(((), Product![&'static str, &'static str]))]
        HandlerComponent: PipeHandlers<Product![Promote<ReadEndpoint>, MakeReport]>,
    }
}

futures_lite::future::block_on(async {
    let label = String::from("server");
    for (port, expected) in [("443", Some(443)), ("invalid", None)] {
        let inputs = product![label.as_str(), port];
        let report = App.handle(PhantomData::<()>, inputs).await.unwrap();
        assert_eq!(report, (label.as_str(), expected));
    }
});
```

A join with one handler rejects an input product with two elements.

```compile_fail
extern crate cgp_core as cgp;

use cgp::prelude::*;
use cgp_concurrency::JoinHandlers;
use cgp_handler::{AsyncComputer, ReturnInput};

async fn surplus_input(app: &impl HasErrorType) {
    type One = JoinHandlers<Product![ReturnInput]>;
    One::compute_async(app, PhantomData::<()>, product![42_u8, true]).await;
}
```

## Spawn owned tasks

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

## Cancellation and failure

Use `try_spawn_task` or `try_spawn_local_task` when the task needs to observe a request.
Pass a token to admission and a clone to the future. The owner can request cancellation,
abort pending work, and drain the task.

```rust
use cgp_concurrency::prelude::*;

async fn stop_worker(runtime: &impl CanSpawnTask) -> Result<TaskOutcome<()>, SpawnError> {
    let token = CancellationToken::new();
    let worker = token.clone();
    let mut owner = runtime.try_spawn_task(&token, async move {
        let cancelled = worker.cancelled().await;
        // Finish asynchronous cleanup here before acknowledging the request.
        Err::<(), _>(cancelled)
    })?;
    owner.request_cancel();
    Ok(owner.drain().await)
}
```

A token cancelled before admission yields `Err(SpawnError::Cancelled)`. After admission, returning
`Ok(value)` yields `Completed(value)`, including when the application value is itself an error.
Returning `Err(Cancelled)` acknowledges the request. Work may ignore a request and finish normally.

Call `owner.abort()` to stop pending work at a poll boundary, then `owner.drain().await` to wait
for destruction. Aborting after completion preserves the published outcome. Dropping a pending
drain leaves the owner available for another wait. Dropping the owner requests native cancellation.

An unwinding task panic yields `Panicked(message)`, with text for string payloads. Executor
shutdown that discards an accepted task yields `RuntimeFailure`. Tokio and Smol both return
native handles on submission, so shutdown can surface when the owner drains.

A polling panic takes precedence over a later panic while destroying the submitted future.
If the future returned a value and its destructor panics, drain reports the cleanup panic.

With unwinding enabled, both adapters contain destructor panics while discarding refused work or
an unclaimed result. Once `drain()` returns `Completed(value)`, the caller owns the value and its
destructor runs normally.

The [shared acceptance tests](../../tests/cgp-concurrency-tests/tests/task_ownership) exercise
these outcomes on both local and multithreaded executors.
