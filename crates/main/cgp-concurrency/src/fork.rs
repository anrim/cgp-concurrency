use core::future::{Future, Ready};
use core::pin::Pin;
use core::task::{Context, Poll};

use cgp::base::types::{Cons, Nil};
use cgp::prelude::*;
use cgp_handler::{Computer, Handler, ReturnInput};
use futures_util::future::{Either, FutureExt, join};
use pin_project_lite::pin_project;

use crate::join::{BranchList, JoinBranches, TaskBranches, join_branches};
use crate::runtime::{
    CanSpawnLocalTask, CanSpawnTask, CancellationToken, HasRuntime, HasRuntimeType,
    LocalTaskSpawnerComponent, SpawnError, TaskControl, TaskHandle, TaskOutcome, TaskResult,
    TaskSpawnerComponent, discard,
};

/// Spawns one owned task per configured handler and returns the group immediately.
///
/// Wire [`HandlerTaskSpawnerComponent`] to [`SpawnLocalTasks`] or [`SpawnWorkerTasks`].
/// Both providers submit all branches in declaration order before returning, using the
/// parent's selected runtime. Each child gets its own context clone, code tag, and input.
/// The empty product starts no tasks. An application error does not stop its siblings.
#[cgp_component(HandlerTaskSpawner)]
pub trait CanSpawnHandlerTasks<Code, Child, Inputs> {
    /// Product of terminal task outcomes in handler declaration order.
    type Outcomes;

    /// The owned join future can outlive the parent and its runtime borrow.
    type Join: Future<Output = Self::Outcomes> + 'static;

    /// Submits every handler before returning. The group can outlive this borrow of the parent.
    fn spawn(
        &self,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> HandlerTasks<Self::Join>;
}

/// Attempts to submit a group through the selected runtime's fallible spawner.
#[cgp_component(TryHandlerTaskSpawner)]
pub trait CanTrySpawnHandlerTasks<Code, Child, Inputs> {
    /// Product of terminal outcomes in handler declaration order.
    type Outcomes;

    /// An accepted group's join future can outlive its parent.
    type Join: Future<Output = Self::Outcomes> + 'static;

    /// On poll, admits children in declaration order using the supplied token.
    /// An empty group succeeds. On refusal, stops admission, aborts every accepted child,
    /// and waits for their termination before returning the admission error.
    /// Dropping that wait releases the handles so the executor can finish reclamation.
    fn try_spawn(
        &self,
        token: &CancellationToken,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> impl Future<Output = Result<HandlerTasks<Self::Join>, SpawnError>>;
}

/// Starts a group with its own cooperative cancellation token, using the configured spawner.
pub trait CanSpawnCancellableHandlerTasks<Code, Child, Inputs>:
    CanSpawnHandlerTasks<Code, Child, Inputs>
{
    /// Builds the child context with a fresh token and submits the tasks before returning.
    fn spawn_cancellable(
        &self,
        code: PhantomData<Code>,
        make_child: impl FnOnce(CancellationToken) -> Child,
        inputs: Inputs,
    ) -> HandlerTasks<Self::Join, CancellationToken> {
        let token = CancellationToken::new();
        let tasks = self.spawn(code, make_child(token.clone()), inputs);
        HandlerTasks {
            join: tasks.join(),
            cancellation: token,
        }
    }
}

impl<App: ?Sized, Code, Child, Inputs> CanSpawnCancellableHandlerTasks<Code, Child, Inputs> for App where
    App: CanSpawnHandlerTasks<Code, Child, Inputs>
{
}

pin_project! {
    /// Owns the children of one submission. Both `tasks.join().await` and `tasks.await`
    /// wait for every child and yield their outcomes in declaration order. Application
    /// errors are retained inside [`TaskOutcome::Completed`](crate::runtime::TaskOutcome::Completed).
    ///
    /// Groups created with [`CanSpawnCancellableHandlerTasks::spawn_cancellable`] also support
    /// [`cancel`](Self::cancel) to request cooperative cancellation and drain every child.
    ///
    /// Dropping the group or a pending join drops the owned handles and any retained outcomes.
    /// Dropping a pending handle requests abortion. Reclamation requires executor progress.
    #[must_use = "dropping the group requests abortion of its pending children"]
    pub struct HandlerTasks<F, C = ()> {
        #[pin]
        join: F,
        cancellation: C,
    }
}

impl<F: Future> HandlerTasks<F> {
    /// Wraps a join future that owns the already-submitted tasks.
    pub fn new(join: F) -> Self {
        Self {
            join,
            cancellation: (),
        }
    }
}

impl<F: Future, C> HandlerTasks<F, C> {
    /// Returns a future that waits for every child and yields their outcomes in declaration order.
    pub fn join(self) -> F {
        self.join
    }
}

impl<F: Future> HandlerTasks<F, CancellationToken> {
    /// Immediately signals this group's children. Await the returned join future to wait for
    /// cleanup and collect all outcomes, including results from children that keep working.
    /// Dropping the pending future releases retained outcomes and requests abort for unfinished children.
    pub fn cancel(self) -> F {
        self.cancellation.request_cancel();
        self.join
    }
}

impl<F: Future, C> Future for HandlerTasks<F, C> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().join.poll(cx)
    }
}

/// Spawns every handler in a product on the local executor. Child state may be non-`Send`.
pub type SpawnLocalTasks<Handlers> = SpawnHandlers<Handlers, LocalTaskSpawnerComponent>;

/// Spawns every handler in a product on the worker executor using its [`SendHandler`] impl.
#[cfg_attr(feature = "smol", doc = concat!(
    "\n## Fork and join two Smol worker tasks\n\n```rust\n",
    include_str!("../examples/fork_handlers.rs"),
    "```",
))]
pub type SpawnWorkerTasks<Handlers> = SpawnHandlers<Handlers, TaskSpawnerComponent>;

pub struct SpawnHandlers<Handlers, Spawner>(PhantomData<(Handlers, Spawner)>);

/// Recovers a [`Handler`]'s `Send` future for a worker branch.
///
/// [`ReturnInput`] supports workers directly. For a custom handler, use
/// [`impl_send_handler!`](crate::impl_send_handler) to forward to [`Handler::handle`].
/// The compiler checks that the forwarded future is `Send`.
/// The handler's existing output and application error types carry through to the join.
pub trait SendHandler<App: HasErrorType, Code, Input>: Handler<App, Code, Input> {
    /// Returns the handler future that the worker will drive.
    fn handle_send(
        app: &App,
        code: PhantomData<Code>,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, App::Error>> + Send;
}

impl<App: HasErrorType, Code, Input: Send> SendHandler<App, Code, Input> for ReturnInput {
    fn handle_send(
        app: &App,
        code: PhantomData<Code>,
        input: Input,
    ) -> impl Future<Output = Result<Input, App::Error>> + Send {
        // Use the synchronous identity operation so the future holds only the output.
        let output = Self::compute(app, code, input);
        async move { Ok(output) }
    }
}

/// Implements [`SendHandler`] by returning the provider's [`Handler::handle`] future directly.
///
/// Specify `(Child, Code, Input) for Provider`. Put impl parameters in brackets before
/// the tuple for a generic declaration. A trailing `where` clause can supply bounds.
///
/// A worker handler can hold an `Arc` across an await and return the value it contains.
///
/// ```rust
/// extern crate cgp_core as cgp;
/// use cgp::prelude::*;
/// use cgp_concurrency::{SendHandler, impl_send_handler};
/// use cgp_handler::{Handler, HandlerComponent};
/// use std::sync::Arc;
///
/// struct Child;
/// impl HasErrorType for Child { type Error = (); }
///
/// #[cgp_impl(new ReadValue)]
/// impl Handler<(), u32> for Child {
///     type Output = u32;
///     async fn handle(&self, _: PhantomData<()>, input: u32) -> Result<u32, ()> {
///         let value = Arc::new(input);
///         futures_lite::future::yield_now().await;
///         Ok(*value)
///     }
/// }
///
/// impl_send_handler!((Child, (), u32) for ReadValue);
/// assert_eq!(futures_lite::future::block_on(ReadValue::handle_send(&Child, PhantomData, 7)), Ok(7));
/// ```
///
/// Even with a `u32` input and output, a handler holding an `Rc` across an await can
/// only run locally. The compiler checks this at the forwarding declaration.
///
/// ```compile_fail
/// extern crate cgp_core as cgp;
/// use cgp::prelude::*;
/// use cgp_concurrency::impl_send_handler;
/// use cgp_handler::{Handler, HandlerComponent};
/// use std::rc::Rc;
///
/// struct Child;
/// impl HasErrorType for Child { type Error = (); }
///
/// #[cgp_impl(new ReadValue)]
/// impl Handler<(), u32> for Child {
///     type Output = u32;
///     async fn handle(&self, _: PhantomData<()>, input: u32) -> Result<u32, ()> {
///         let value = Rc::new(input);
///         futures_lite::future::yield_now().await;
///         Ok(*value)
///     }
/// }
///
/// impl_send_handler!((Child, (), u32) for ReadValue);
/// ```
#[macro_export]
macro_rules! impl_send_handler {
    (
        $([$($param:tt)*])?
        ($child:ty, $code:ty, $input:ty $(,)?) for $handler:ty
        $(where $($bounds:tt)+)?
    ) => {
        impl $(<$($param)*>)? $crate::SendHandler<$child, $code, $input> for $handler
        $(where $($bounds)+)?
        {
            fn handle_send(
                child: &$child,
                code: ::core::marker::PhantomData<$code>,
                input: $input,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<
                <Self as $crate::__private::Handler<$child, $code, $input>>::Output,
                <$child as $crate::__private::HasErrorType>::Error,
            >> + ::core::marker::Send {
                <Self as $crate::__private::Handler<$child, $code, $input>>::handle(child, code, input)
            }
        }
    };
}

#[cgp_impl(SpawnHandlers<Handlers, Spawner>)]
#[uses(HasRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl<Handlers, Spawner, Child, Code, Inputs, Outcomes, Tasks>
    HandlerTaskSpawner<Code, Child, Inputs>
where
    Handlers: SpawnList<Spawner, Runtime, Child, Code, Inputs, Tasks = Tasks>,
    Tasks: BranchList<Output = Outcomes> + 'static,
{
    type Outcomes = Outcomes;
    type Join = JoinBranches<Tasks>;

    fn spawn(
        &self,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> HandlerTasks<Self::Join> {
        let tasks = Handlers::spawn(self.runtime(), child, code, inputs);
        HandlerTasks::new(join_branches(tasks))
    }
}

#[cgp_impl(SpawnHandlers<Handlers, Spawner>)]
#[uses(HasRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl<Handlers, Spawner, Child, Code, Inputs, Outcomes, Tasks>
    TryHandlerTaskSpawner<Code, Child, Inputs>
where
    Handlers: SpawnList<Spawner, Runtime, Child, Code, Inputs, Tasks = Tasks>,
    Tasks: BranchList<Output = Outcomes> + 'static,
{
    type Outcomes = Outcomes;
    type Join = JoinBranches<Tasks>;

    fn try_spawn(
        &self,
        token: &CancellationToken,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> impl Future<Output = Result<HandlerTasks<Self::Join>, SpawnError>> {
        // Acquire ownership before creating the lazy submission. Dropping it without
        // polling must dispose of each capture independently, without touching the runtime.
        let child = Guard::new(child, discard);
        let inputs = Guard::new(inputs, Handlers::discard_inputs);
        async move {
            match Handlers::try_spawn(self.runtime(), token, child.take(), code, inputs.take()) {
                Ok(tasks) => Ok(HandlerTasks::new(join_branches(tasks))),
                Err(cleanup) => Err(cleanup.await),
            }
        }
    }
}

// An unclaimed value is released by its own cleanup operation, including before first poll.
struct Guard<T, D: FnOnce(T)>(Option<(T, D)>);

impl<T, D: FnOnce(T)> Guard<T, D> {
    fn new(value: T, dispose: D) -> Self {
        Self(Some((value, dispose)))
    }

    fn get(&self) -> &T {
        &self.0.as_ref().expect("guard owns its value").0
    }

    fn take(mut self) -> T {
        self.0.take().expect("guard owns its value").0
    }
}

impl<T, D: FnOnce(T)> Drop for Guard<T, D> {
    fn drop(&mut self) {
        if let Some((value, dispose)) = self.0.take() {
            dispose(value);
        }
    }
}

fn local_handler_task<H, Code, Child, Input>(
    child: Child,
    input: Input,
) -> impl Future<Output = Result<H::Output, Child::Error>> + 'static
where
    H: Handler<Child, Code, Input>,
    Child: HasErrorType + 'static,
    Input: 'static,
{
    let child = Guard::new(child, discard);
    let input = Guard::new(input, discard);
    async move {
        let work = TaskResult::capture(|| H::handle(child.get(), PhantomData, input.take()));
        work.run().await.finish(|| drop(child.take()))
    }
}

fn worker_handler_task<H, Code, Child, Input>(
    child: Child,
    input: Input,
) -> impl Future<Output = Result<H::Output, Child::Error>> + 'static
where
    H: SendHandler<Child, Code, Input>,
    Child: HasErrorType + 'static,
    Input: 'static,
{
    let child = Guard::new(child, discard);
    let input = Guard::new(input, discard);
    async move {
        // Construct the tag on the worker so only the child and input cross threads.
        let work = TaskResult::capture(|| H::handle_send(child.get(), PhantomData, input.take()));
        work.run().await.finish(|| drop(child.take()))
    }
}

trait SpawnBranch<R, Child, Code, H, Input> {
    type Value: 'static;
    type Handle: TaskHandle<Self::Value> + 'static;

    fn spawn(runtime: &R, child: Child, code: PhantomData<Code>, input: Input) -> Self::Handle;

    fn try_spawn(
        runtime: &R,
        token: &CancellationToken,
        child: Child,
        code: PhantomData<Code>,
        input: Input,
    ) -> Result<Self::Handle, SpawnError>;
}

impl<R, Child, Code, H, Input> SpawnBranch<R, Child, Code, H, Input> for LocalTaskSpawnerComponent
where
    R: CanSpawnLocalTask,
    Child: HasErrorType + 'static,
    H: Handler<Child, Code, Input>,
    Input: 'static,
    H::Output: 'static,
    Child::Error: 'static,
{
    type Value = Result<H::Output, Child::Error>;
    type Handle = R::LocalJoinHandle<Self::Value>;

    fn spawn(runtime: &R, child: Child, _: PhantomData<Code>, input: Input) -> Self::Handle {
        runtime.spawn_local_task(local_handler_task::<H, Code, _, _>(child, input))
    }

    fn try_spawn(
        runtime: &R,
        token: &CancellationToken,
        child: Child,
        _: PhantomData<Code>,
        input: Input,
    ) -> Result<Self::Handle, SpawnError> {
        let work = local_handler_task::<H, Code, _, _>(child, input);
        runtime.try_spawn_local_task(token, TaskControl::returning_value(work))
    }
}

impl<R, Child, Code, H, Input> SpawnBranch<R, Child, Code, H, Input> for TaskSpawnerComponent
where
    R: CanSpawnTask,
    Child: HasErrorType + Send + 'static,
    H: SendHandler<Child, Code, Input>,
    Input: Send + 'static,
    H::Output: Send + 'static,
    Child::Error: Send + 'static,
{
    type Value = Result<H::Output, Child::Error>;
    type Handle = R::JoinHandle<Self::Value>;

    fn spawn(runtime: &R, child: Child, _: PhantomData<Code>, input: Input) -> Self::Handle {
        runtime.spawn_task(worker_handler_task::<H, Code, _, _>(child, input))
    }

    fn try_spawn(
        runtime: &R,
        token: &CancellationToken,
        child: Child,
        _: PhantomData<Code>,
        input: Input,
    ) -> Result<Self::Handle, SpawnError> {
        let work = worker_handler_task::<H, Code, _, _>(child, input);
        runtime.try_spawn_task(token, TaskControl::returning_value(work))
    }
}

trait SpawnList<Spawner, R, Child, Code, Inputs> {
    type Tasks: BranchList + 'static;

    fn spawn(runtime: &R, child: Child, code: PhantomData<Code>, inputs: Inputs) -> Self::Tasks;

    fn discard_inputs(inputs: Inputs);

    // Admission and abort are synchronous. Only draining a refused group's accepted prefix can wait.
    fn try_spawn(
        runtime: &R,
        token: &CancellationToken,
        child: Child,
        code: PhantomData<Code>,
        inputs: Inputs,
    ) -> Result<Self::Tasks, impl Future<Output = SpawnError>>;
}

impl<Spawner, R, Child, Code> SpawnList<Spawner, R, Child, Code, Nil> for Nil {
    type Tasks = Nil;

    fn spawn(_: &R, child: Child, _: PhantomData<Code>, _: Nil) -> Self::Tasks {
        discard(child);
        Nil
    }

    fn discard_inputs(_: Nil) {}

    fn try_spawn(
        _: &R,
        _: &CancellationToken,
        child: Child,
        _: PhantomData<Code>,
        _: Nil,
    ) -> Result<Nil, impl Future<Output = SpawnError>> {
        discard(child);
        Ok::<_, Ready<SpawnError>>(Nil)
    }
}

impl<Spawner, R, Child, Code, Head, Tail, Input, Inputs>
    SpawnList<Spawner, R, Child, Code, Cons<Input, Inputs>> for Cons<Head, Tail>
where
    Child: Clone,
    Spawner: SpawnBranch<R, Child, Code, Head, Input>,
    Tail: SpawnList<Spawner, R, Child, Code, Inputs>,
{
    type Tasks = TaskBranches<Spawner::Handle, Tail::Tasks>;

    fn spawn(
        runtime: &R,
        child: Child,
        code: PhantomData<Code>,
        inputs: Cons<Input, Inputs>,
    ) -> Self::Tasks {
        let head = Spawner::spawn(runtime, child.clone(), code, inputs.0);
        TaskBranches::new(head, Tail::spawn(runtime, child, code, inputs.1))
    }

    fn discard_inputs(inputs: Cons<Input, Inputs>) {
        discard(inputs.0);
        Tail::discard_inputs(inputs.1);
    }

    fn try_spawn(
        runtime: &R,
        token: &CancellationToken,
        child: Child,
        code: PhantomData<Code>,
        inputs: Cons<Input, Inputs>,
    ) -> Result<Self::Tasks, impl Future<Output = SpawnError>> {
        let head = match Spawner::try_spawn(runtime, token, child.clone(), code, inputs.0) {
            Ok(head) => head,
            Err(error) => {
                // Let every enclosing branch signal abort before running user destructors.
                return Err(Either::Left(async move {
                    discard(child);
                    Tail::discard_inputs(inputs.1);
                    error
                }));
            }
        };
        match Tail::try_spawn(runtime, token, child, code, inputs.1) {
            Ok(tail) => Ok(TaskBranches::new(head, tail)),
            Err(cleanup) => {
                head.abort();
                let drains = join(cleanup, head.map(TaskOutcome::discard));
                Err(Either::Right(drains.map(|(error, ())| error)))
            }
        }
    }
}
