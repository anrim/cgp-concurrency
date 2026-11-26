# cgp-concurrency

Fork owned handler tasks on Tokio or Smol, or join handlers that borrow data in the caller’s task.

## Installation

From an application next to this checkout, add the facade with the runtime you use.

```sh
cargo add --path ../cgp-concurrency/crates/main/cgp-concurrency --features smol
```

The optional `smol` and `tokio` features expose the corresponding adapter modules.
The default feature set supports caller-driven joins. To run the handler examples in
your own application, add their CGP and executor dependencies as well.

```sh
cargo add cgp-core@0.8 cgp-handler@0.8 futures-lite@2
```

## Usage

### Fork handlers

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency::CanSpawnHandlerTasks;

async fn run_pair<Code, Child, Inputs, A, B>(
    parent: &impl CanSpawnHandlerTasks<Code, Child, Inputs, Outcomes = Product![A, B]>,
    code: PhantomData<Code>, child: Child, inputs: Inputs,
) -> (A, B) {
    let tasks = parent.spawn(code, child, inputs);
    let product![first, second] = tasks.join().await;
    (first, second)
}
```

`spawn` submits the handlers in declaration order before returning. Each child owns its
input and a clone of the child context. Both `tasks.join().await` and `tasks.await` yield
one `TaskOutcome<Result<Output, Error>>` per handler in that order. Dropping the group or
a pending join requests abort for unfinished children.

#### Worker handlers

`SpawnLocalTasks` accepts a plain `Handler`. `SpawnWorkerTasks` requires `SendHandler`
because its executor can move a suspended future to another thread. If your custom
handler only implements `Handler`, use `impl_send_handler!` to forward its existing
method. Rust checks that the returned future is `Send`. For example, holding an `Rc`
across an `.await` would fail that check.

The facade already implements `SendHandler` for `ReturnInput`. Its worker future owns
only the input and returns it unchanged. Of the two handlers below, only `Double`
needs a forwarding declaration.

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency::{SendHandler, impl_send_handler};
use cgp_handler::{Handler, HandlerComponent, ReturnInput};
use futures_lite::future::block_on;

struct Child;
impl HasErrorType for Child { type Error = (); }

#[cgp_impl(new Double)]
impl Handler<(), u32> for Child {
    type Output = u32;
    async fn handle(&self, _: PhantomData<()>, value: u32) -> Result<u32, ()> {
        Ok(value * 2)
    }
}

impl_send_handler!((Child, (), u32) for Double);

let code = PhantomData::<()>;
assert_eq!(block_on(ReturnInput::handle_send(&Child, code, 3_u32)), Ok(3));
assert_eq!(block_on(Double::handle_send(&Child, code, 3)), Ok(6));
```

### Fallible submission

```rust
use cgp_concurrency::prelude::*;
use core::marker::PhantomData;

async fn run<Code, Child, Inputs, P: CanTrySpawnHandlerTasks<Code, Child, Inputs>>(
    parent: &P, token: &CancellationToken, code: PhantomData<Code>, child: Child, inputs: Inputs,
) -> Result<P::Outcomes, SpawnError> {
    let tasks = parent.try_spawn(token, code, child, inputs).await?;
    Ok(tasks.join().await)
}
```

The first await attempts admission in declaration order. On refusal, submission stops and
every accepted child receives an abort request before the parent waits for their termination.
Cleanup releases each outcome as it becomes ready, even while a sibling is still draining.
Dropping this wait releases the handles so the executor can finish cleanup. On success the
group can outlive the parent. Handler errors appear in its ordered outcomes.

### Cancel and drain a group

```rust
use cgp_concurrency::prelude::*;
use std::marker::PhantomData;

#[derive(Clone)]
struct Child { cancel: CancellationToken }

async fn stop<Code, Inputs, P: CanSpawnHandlerTasks<Code, Child, Inputs>>(
    parent: &P, code: PhantomData<Code>, inputs: Inputs,
) -> P::Outcomes {
    let tasks = parent.spawn_cancellable(code, |cancel| Child { cancel }, inputs);
    tasks.cancel().await
}
```

`cancel()` signals the token immediately and returns `P::Join`. Children can await
`cancel.cancelled()`, finish cleanup, and return their application results. A child that
returns an error after cleanup still yields `TaskOutcome::Completed(Err(error))`. The wait
includes children that ignore the request. Dropping it releases retained outcomes and
requests abort for unfinished children. To signal several groups together, give their
child contexts clones of one token through `spawn`, request cancellation, then join each group.

### Join typed handlers

`JoinHandlers` polls the handlers in the caller's task. Inputs can borrow local data,
and the join returns each handler's `Result` in declaration order. This works with both
runtime features disabled.

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency::JoinHandlers;
use cgp_handler::{AsyncComputer, ReturnInput};

struct App;
impl HasErrorType for App { type Error = core::convert::Infallible; }

type Endpoint = JoinHandlers<Product![ReturnInput, ReturnInput]>;

futures_lite::future::block_on(async {
    let name = String::from("server");
    let inputs = product![name.as_str(), 443_u16];
    let product![label, port] = Endpoint::compute_async(&App, PhantomData::<()>, inputs).await;
    assert_eq!(label, Ok("server"));
    assert_eq!(port, Ok(443));
});
```

The handler and input products must have the same length. Dropping a pending join drops its
branch futures and retained results. To feed the results into another handler, use
`Promote<JoinHandlers<Jobs>>` in a `PipeHandlers` pipeline. The `JoinHandlers` type documentation
includes a complete example with a fallible branch.

### Spawn owned tasks

The prelude re-exports `CanSpawnLocalTask` and `CanSpawnTask` for individual futures.
Both operations submit immediately and return a handle implementing `TaskHandle<T>`.
Await it for its `TaskOutcome<T>`, or call `drain().await` to wait through a mutable borrow.

```rust
use cgp_concurrency::prelude::*;
use std::{future::Future, rc::Rc};

async fn local(runtime: &impl CanSpawnLocalTask, gate: impl Future<Output = ()> + 'static)
    -> TaskOutcome<Rc<str>>
{
    let value: Rc<str> = Rc::from("local");
    let mut task = runtime.spawn_local_task(async move {
        gate.await;
        value
    });
    task.drain().await
}
```

<details>
<summary>Which futures can run on a worker?</summary>

The worker future and its output must be `Send + 'static`. Rust checks the values the
future holds across suspension, even when its output is a plain integer.

```rust
use cgp_concurrency::prelude::*;
use std::{future::Future, sync::Arc};

async fn read(runtime: &impl CanSpawnTask, gate: impl Future<Output = ()> + Send + 'static) {
    runtime.spawn_task(async move {
        let value = Arc::new(42);
        gate.await;
        *value
    }).await;
}
```

Changing the captured value to an `Rc` fails the same check.

```compile_fail
use cgp_concurrency::prelude::*;
use std::{future::Future, rc::Rc};

async fn read(runtime: &impl CanSpawnTask, gate: impl Future<Output = ()> + Send + 'static) {
    runtime.spawn_task(async move {
        let value = Rc::new(42);
        gate.await;
        *value
    }).await;
}
```

</details>

### Cancellation and failure

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

A prior cancellation returns `Err(SpawnError::Cancelled)` at admission. Once accepted,
work that returns `Err(Cancelled)` yields `TaskOutcome::Cancelled`. A value or application
error yields `Completed`, an unwind yields `Panicked`, and executor loss before completion
yields `RuntimeFailure`. Calling `abort()` stops pending work at a poll boundary. Dropping
a pending owner also requests abort. Keep the executor running to reclaim the task.

## API reference

Import `cgp_concurrency::prelude::*` for handler groups and portable task operations.
The `runtime` module re-exports the full `cgp-concurrency-runtime` API.

### Handler traits

| Consumer trait | Provider | Operation |
| --- | --- | --- |
| `CanSpawnHandlerTasks` | Wire `HandlerTaskSpawnerComponent` to a `HandlerTaskSpawner`. | `spawn(code, child, inputs)` returns `HandlerTasks<P::Join>` for parent type `P`. Its owned join future yields `P::Outcomes` and can outlive the parent. |
| `CanTrySpawnHandlerTasks` | Wire `TryHandlerTaskSpawnerComponent` to a `TryHandlerTaskSpawner`. | `try_spawn(token, code, child, inputs).await` returns an owned group or the admission error after cleanup. |
| `CanSpawnCancellableHandlerTasks` | Available on every `CanSpawnHandlerTasks` implementation. | `spawn_cancellable(code, make_child, inputs)` gives the child constructor a fresh token and returns a group with `cancel()`. |
| `cgp_handler::CanComputeAsync` | Wire `AsyncComputerComponent` to `JoinHandlers<Jobs>`. | `compute_async(code, inputs).await` joins handlers inside the caller's task. |

With a `CanSpawnLocalTask` runtime, select `SpawnLocalTasks<Jobs>` for local handlers,
including those holding an `Rc` across an await. With a `CanSpawnTask` runtime, select
`SpawnWorkerTasks<Jobs>` for handlers implementing `SendHandler`. [Worker handlers](#worker-handlers)
Both spawn providers require `'static` child contexts, inputs, outputs, and errors. Worker
tasks also require these types and the handler future to be `Send`.
To expose both submission methods, wire `[HandlerTaskSpawnerComponent, TryHandlerTaskSpawnerComponent]`
to the same local or worker provider.

## Provider example

An application wrapper can reuse its inner context’s handler-task provider. Forward
`spawn` and reuse its `Join` and `Outcomes` types so the returned group keeps ownership
of the children.

```rust
extern crate cgp_core as cgp;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;

struct App<P>(P);

#[cgp_impl(new ForwardTasks)]
impl<P, Code, Child, Inputs> HandlerTaskSpawner<Code, Child, Inputs> for App<P>
where
    P: CanSpawnHandlerTasks<Code, Child, Inputs>,
{
    type Outcomes = P::Outcomes;
    type Join = P::Join;

    fn spawn(&self, code: PhantomData<Code>, child: Child, inputs: Inputs)
        -> HandlerTasks<Self::Join>
    {
        self.0.spawn(code, child, inputs)
    }
}

delegate_components! {
    <P> App<P> { HandlerTaskSpawnerComponent: ForwardTasks }
}
```

A provider that submits its own tasks can wrap their join future with `HandlerTasks::new(join)`.
For a complete worker example, run the following command from the workspace root.
It wires the worker provider directly, submits a server label and port, and joins both outcomes.

```sh
cargo run --locked -p cgp-concurrency --features smol --example fork_handlers
```
