use core::pin::pin;

use async_channel::{Receiver, Sender, bounded};
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, HandlerComponent};
use futures_lite::future;

use super::join_notification::join_notification;
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

type Cleanup<T> = (Receiver<()>, Result<T, Rejected>);

#[cgp_impl(new FinishOnCancel)]
#[use_type(HasErrorType.Error = Rejected)]
impl<T> Handler<ReadPair, Cleanup<T>> {
    type Output = T;

    async fn handle(
        &self,
        _: PhantomData<ReadPair>,
        input: Cleanup<T>,
        #[implicit] cancel: &CancellationToken,
        #[implicit] waiting: &Sender<()>,
        #[implicit] cleaning: &Sender<()>,
    ) -> Result<T, Rejected> {
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

impl_send_handler!([T: Send] (Child, ReadPair, Cleanup<T>) for FinishOnCancel);

type Jobs = Product![FinishOnCancel, FinishOnCancel];
type Local = SpawnLocalTasks<Jobs>;
type Workers = SpawnWorkerTasks<Jobs>;
type Inputs<T> = Product![Cleanup<T>, Cleanup<T>];
type Outcome<T> = TaskOutcome<Result<T, Rejected>>;
type Outcomes<T> = Product![Outcome<T>, Outcome<T>];

delegate_and_check_components! {
    #[check_trait(CheckLocalCleanup)]
    <R: CanSpawnLocalTask> Parent<R, Local> {
        #[check_params((ReadPair, Child, Inputs<u8>))]
        HandlerTaskSpawnerComponent: Local,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckWorkerCleanup)]
    <R: CanSpawnTask> Parent<R, Workers> {
        #[check_params((ReadPair, Child, Inputs<u8>))]
        HandlerTaskSpawnerComponent: Workers,
    }
}

async fn wait_for_cleanup_start<T>(
    parent: impl CanSpawnHandlerTasks<ReadPair, Child, Inputs<T>, Outcomes = Outcomes<T>>,
    inputs: Inputs<T>,
    cancel_wait: bool,
) -> (impl Future<Output = Outcomes<T>> + 'static, Receiver<()>) {
    let (waiting, started) = bounded(2);
    let (cleaning, cleaning_up) = bounded(2);
    let (publish, request) = bounded(1);
    let tasks = parent.spawn_cancellable(
        PhantomData,
        move |cancel| {
            publish.try_send(cancel.clone()).unwrap();
            Child {
                cancel,
                waiting,
                cleaning,
            }
        },
        inputs,
    );
    // An owned group survives its parent even when the caller knows only the consumer trait.
    drop(parent);
    for _ in 0..2 {
        started.recv().await.unwrap();
    }

    let drain = if cancel_wait {
        tasks.cancel()
    } else {
        request.try_recv().unwrap().request_cancel();
        tasks.join()
    };
    for _ in 0..2 {
        cleaning_up.recv().await.unwrap();
    }
    (drain, cleaning_up)
}

async fn cancel_and_drain<Spawner, R>(runtime: R)
where
    Parent<R, Spawner>: CanSpawnHandlerTasks<ReadPair, Child, Inputs<u8>, Outcomes = Outcomes<u8>>,
{
    let (finish_first, first) = bounded(1);
    let (finish_second, second) = bounded(1);
    let inputs = product![(first, Ok(7)), (second, Err(Rejected(17)))];
    let parent = Parent {
        runtime,
        branches: PhantomData::<Spawner>,
    };
    let (drain, cleaning_up) = wait_for_cleanup_start(parent, inputs, true).await;
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

struct Retained(Sender<()>);

impl Drop for Retained {
    fn drop(&mut self) {
        self.0.try_send(()).unwrap();
        panic!("discarded group output");
    }
}

async fn drop_pending_wait<Spawner, R>(runtime: R, cancel_wait: bool)
where
    Parent<R, Spawner>:
        CanSpawnHandlerTasks<ReadPair, Child, Inputs<Retained>, Outcomes = Outcomes<Retained>>,
{
    let (finish_first, first) = bounded(1);
    let (finish_second, second) = bounded(1);
    let (value, released) = bounded(1);
    let inputs = product![(first, Ok(Retained(value))), (second, Err(Rejected(17)))];
    let parent = Parent {
        runtime,
        branches: PhantomData::<Spawner>,
    };
    let (drain, cleaning_up) = wait_for_cleanup_start(parent, inputs, cancel_wait).await;
    {
        let mut drain = pin!(drain);
        // Only the first child can finish and wake this join. Repoll to retain its result.
        let published = join_notification(&mut drain);
        finish_first.try_send(()).unwrap();
        published.recv().await.unwrap();
        assert!(future::poll_once(&mut drain).await.is_none());
        assert!(!released.is_closed());
        assert!(!finish_second.is_closed());
    }
    // Dropping the wait releases the sole output sender and aborts the still-gated sibling.
    assert_eq!(released.try_recv(), Ok(()));
    assert!(released.is_closed());
    finish_second.closed().await;
    assert!(cleaning_up.recv().await.is_err());
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

#[cfg(feature = "tokio")]
#[test]
fn tokio_local_drops_pending_wait() {
    for cancel_wait in [false, true] {
        executors::tokio_local(async |runtime| {
            drop_pending_wait::<Local, _>(runtime, cancel_wait).await
        });
    }
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_worker_drops_pending_wait() {
    for cancel_wait in [false, true] {
        executors::tokio_worker(async |runtime| {
            drop_pending_wait::<Workers, _>(runtime, cancel_wait).await
        });
    }
}

#[cfg(feature = "smol")]
#[test]
fn smol_local_drops_pending_wait() {
    for cancel_wait in [false, true] {
        executors::smol_local(async |runtime| {
            drop_pending_wait::<Local, _>(runtime, cancel_wait).await
        });
    }
}

#[cfg(feature = "smol")]
#[test]
fn smol_worker_drops_pending_wait() {
    for cancel_wait in [false, true] {
        executors::smol_worker(async |runtime| {
            drop_pending_wait::<Workers, _>(runtime, cancel_wait).await
        });
    }
}
