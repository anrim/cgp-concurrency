use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use cgp::base::types::{Cons, Nil};
use cgp::prelude::*;
use cgp_handler::Handler;
use pin_project_lite::pin_project;

use crate::join::{BranchList, Branches, join_branches};
use crate::runtime::{
    CanSpawnLocalTask, CanSpawnTask, HasRuntime, HasRuntimeType, LocalTaskSpawnerComponent,
    TaskHandle, TaskOutcome, TaskSpawnerComponent,
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

    /// Submits every handler before returning. The group can outlive this borrow of the parent.
    fn spawn(
        &self,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> HandlerTasks<impl Future<Output = Self::Outcomes> + 'static>;
}

pin_project! {
    /// Owns the children of one submission. Both `tasks.join().await` and `tasks.await`
    /// wait for every child and yield their outcomes in declaration order. Application
    /// errors are retained inside [`TaskOutcome::Completed`].
    ///
    /// Dropping the group or a pending join drops the owned handles and any retained outcomes.
    /// Dropping a pending handle requests abortion. Reclamation requires executor progress.
    #[must_use = "dropping the group requests abortion of its pending children"]
    pub struct HandlerTasks<F> {
        #[pin]
        join: F,
    }
}

impl<F: Future> HandlerTasks<F> {
    /// Wraps a join future that owns the already-submitted tasks.
    pub fn new(join: F) -> Self {
        Self { join }
    }

    /// Returns a future that waits for every child and yields their outcomes in declaration order.
    pub fn join(self) -> F {
        self.join
    }
}

impl<F: Future> Future for HandlerTasks<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().join.poll(cx)
    }
}

/// Spawns every handler in a product on the local executor. Child state may be non-`Send`.
pub type SpawnLocalTasks<Handlers> = SpawnHandlers<Handlers, LocalTaskSpawnerComponent>;

/// Spawns every handler in a product on the worker executor using its [`SendHandler`] impl.
pub type SpawnWorkerTasks<Handlers> = SpawnHandlers<Handlers, TaskSpawnerComponent>;

pub struct SpawnHandlers<Handlers, Spawner>(PhantomData<(Handlers, Spawner)>);

/// Recovers a [`Handler`]'s `Send` future for a worker branch.
///
/// Implement this trait for a concrete handler and child context by forwarding to
/// [`Handler::handle`]. The compiler checks that the forwarded future is `Send`.
/// The handler's existing output and application error types carry through to the join.
pub trait SendHandler<App: HasErrorType, Code, Input>: Handler<App, Code, Input> {
    /// Returns the handler future that the worker will drive.
    fn handle_send(
        app: &App,
        code: PhantomData<Code>,
        input: Input,
    ) -> impl Future<Output = Result<Self::Output, App::Error>> + Send;
}

#[cgp_impl(SpawnHandlers<Handlers, Spawner>)]
#[uses(HasRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl<Handlers, Spawner, Child, Code, Inputs, Outcomes> HandlerTaskSpawner<Code, Child, Inputs>
where
    Handlers: SpawnList<Spawner, Runtime, Child, Code, Inputs, Output = Outcomes>,
{
    type Outcomes = Outcomes;

    fn spawn(
        &self,
        code: PhantomData<Code>,
        child: Child,
        inputs: Inputs,
    ) -> HandlerTasks<impl Future<Output = Outcomes> + 'static> {
        let tasks = Handlers::spawn(self.runtime(), child, code, inputs);
        HandlerTasks::new(join_branches(tasks))
    }
}

trait SpawnBranch<R, Child, Code, H, Input> {
    type Value: 'static;

    fn spawn(
        runtime: &R,
        child: Child,
        code: PhantomData<Code>,
        input: Input,
    ) -> impl TaskHandle<Self::Value> + 'static;
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

    fn spawn(
        runtime: &R,
        child: Child,
        _: PhantomData<Code>,
        input: Input,
    ) -> impl TaskHandle<Self::Value> + 'static {
        runtime.spawn_local_task(async move { H::handle(&child, PhantomData, input).await })
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

    fn spawn(
        runtime: &R,
        child: Child,
        _: PhantomData<Code>,
        input: Input,
    ) -> impl TaskHandle<Self::Value> + 'static {
        // Construct the code tag on the worker so only the child and input cross threads.
        runtime.spawn_task(async move { H::handle_send(&child, PhantomData, input).await })
    }
}

trait SpawnList<Spawner, R, Child, Code, Inputs> {
    type Output;

    fn spawn(
        runtime: &R,
        child: Child,
        code: PhantomData<Code>,
        inputs: Inputs,
    ) -> impl BranchList<Output = Self::Output> + 'static;
}

impl<Spawner, R, Child, Code> SpawnList<Spawner, R, Child, Code, Nil> for Nil {
    type Output = Nil;

    fn spawn(
        _: &R,
        _: Child,
        _: PhantomData<Code>,
        _: Nil,
    ) -> impl BranchList<Output = Nil> + 'static {
        Nil
    }
}

impl<Spawner, R, Child, Code, Head, Tail, Input, Inputs>
    SpawnList<Spawner, R, Child, Code, Cons<Input, Inputs>> for Cons<Head, Tail>
where
    Child: Clone,
    Spawner: SpawnBranch<R, Child, Code, Head, Input>,
    Tail: SpawnList<Spawner, R, Child, Code, Inputs>,
{
    type Output = Cons<TaskOutcome<Spawner::Value>, Tail::Output>;

    fn spawn(
        runtime: &R,
        child: Child,
        code: PhantomData<Code>,
        inputs: Cons<Input, Inputs>,
    ) -> impl BranchList<Output = Self::Output> + 'static {
        let head = Spawner::spawn(runtime, child.clone(), code, inputs.0);
        Branches::new(head, Tail::spawn(runtime, child, code, inputs.1))
    }
}
