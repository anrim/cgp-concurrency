use core::pin::pin;

use async_channel::{Receiver, Sender, bounded};
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, HandlerComponent};
use futures_lite::future;

use super::{Parent, ReadPair, Rejected, executors, join_after_pending};

#[derive(Clone, HasField)]
struct Child {
    cancel: CancellationToken,
    waiting: Sender<()>,
    cleaning: Sender<()>,
}

impl HasErrorType for Child {
    type Error = Rejected;
}

type Cleanup = (Receiver<()>, Result<u8, Rejected>);

#[cgp_impl(new FinishOnCancel)]
#[use_type(HasErrorType.Error = Rejected)]
impl Handler<ReadPair, Cleanup> {
    type Output = u8;

    async fn handle(
        &self,
        _: PhantomData<ReadPair>,
        input: Cleanup,
        #[implicit] cancel: &CancellationToken,
        #[implicit] waiting: &Sender<()>,
        #[implicit] cleaning: &Sender<()>,
    ) -> Result<u8, Rejected> {
        let (finish, result) = input;
        let mut request = pin!(cancel.cancelled());
        assert!(future::poll_once(&mut request).await.is_none());
        waiting.try_send(()).unwrap();
        request.await;
        cleaning.try_send(()).unwrap();
        finish.recv().await.unwrap();
        result
    }
}

impl SendHandler<Child, ReadPair, Cleanup> for FinishOnCancel {
    fn handle_send(
        child: &Child,
        code: PhantomData<ReadPair>,
        input: Cleanup,
    ) -> impl Future<Output = Result<u8, Rejected>> + Send {
        Self::handle(child, code, input)
    }
}

type Jobs = Product![FinishOnCancel, FinishOnCancel];
type Local = SpawnLocalTasks<Jobs>;
type Workers = SpawnWorkerTasks<Jobs>;
type Inputs = Product![Cleanup, Cleanup];
type Outcome = TaskOutcome<Result<u8, Rejected>>;

delegate_and_check_components! {
    #[check_trait(CheckLocalCleanup)]
    <R: CanSpawnLocalTask> Parent<R, Local> {
        #[check_params((ReadPair, Child, Inputs))]
        HandlerTaskSpawnerComponent: Local,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckWorkerCleanup)]
    <R: CanSpawnTask> Parent<R, Workers> {
        #[check_params((ReadPair, Child, Inputs))]
        HandlerTaskSpawnerComponent: Workers,
    }
}

async fn cancel_and_drain<Spawner, R>(runtime: R)
where
    Parent<R, Spawner>:
        CanSpawnHandlerTasks<ReadPair, Child, Inputs, Outcomes = Product![Outcome, Outcome]>,
{
    let parent = Parent {
        runtime,
        branches: PhantomData::<Spawner>,
    };
    let (waiting, started) = bounded(2);
    let (cleaning, cleaning_up) = bounded(2);
    let (finish_first, first) = bounded(1);
    let (finish_second, second) = bounded(1);
    let tasks = parent.spawn_cancellable(
        PhantomData,
        |cancel| Child {
            cancel,
            waiting,
            cleaning,
        },
        product![(first, Ok(7)), (second, Err(Rejected(17)))],
    );
    for _ in 0..2 {
        started.recv().await.unwrap();
    }

    let drain = tasks.cancel();
    for _ in 0..2 {
        cleaning_up.recv().await.unwrap();
    }
    finish_second.try_send(()).unwrap();
    finish_second.closed().await;
    assert_eq!(
        join_after_pending(drain, &finish_first).await,
        product![
            TaskOutcome::Completed(Ok(7)),
            TaskOutcome::Completed(Err(Rejected(17)))
        ]
    );
    // Only the child contexts owned senders. Joining must wait for the last one to drop.
    assert!(cleaning_up.is_closed());
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_local() {
    executors::tokio_local(async |runtime| cancel_and_drain::<Local, _>(runtime).await);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_worker() {
    executors::tokio_worker(async |runtime| cancel_and_drain::<Workers, _>(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_local() {
    executors::smol_local(async |runtime| cancel_and_drain::<Local, _>(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_worker() {
    executors::smol_worker(async |runtime| cancel_and_drain::<Workers, _>(runtime).await);
}
