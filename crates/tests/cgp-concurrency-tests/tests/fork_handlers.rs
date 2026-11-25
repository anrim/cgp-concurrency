//! Both children start before the caller joins. The second passes its gate first,
//! and the join waits for the first child before returning outcomes in declaration order.

#![cfg(any(feature = "smol", feature = "tokio"))]

extern crate cgp_core as cgp;

#[path = "support/executors.rs"]
mod executors;

use core::future::poll_fn;
use core::pin::pin;
use std::cell::Cell;
use std::rc::Rc;
use std::thread::{self, ThreadId};

use async_channel::{Receiver, Sender, bounded};
use cgp::component::UseFields;
use cgp::error::ErrorTypeProviderComponent;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, HandlerComponent, ReturnInput};
use futures_lite::future;

#[derive(HasField)]
struct Parent<R, Branches> {
    runtime: R,
    branches: PhantomData<Branches>,
}

delegate_and_check_components! {
    <R, B> Parent<R, B> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
    }
}

#[derive(Clone, HasField)]
struct Child<T> {
    value: T,
}

#[derive(Debug, PartialEq)]
struct Rejected(u16);

delegate_and_check_components! {
    <T> Child<T> {
        ErrorTypeProviderComponent: UseType<Rejected>,
    }
}

struct ReadPair;

#[derive(Clone)]
struct Gate {
    start: Sender<ThreadId>,
    release: Receiver<()>,
}

impl Gate {
    async fn wait(&self) {
        self.start.try_send(thread::current().id()).unwrap();
        self.release.recv().await.unwrap();
    }
}

#[cgp_impl(new ReadValue)]
#[use_type(HasErrorType.Error)]
impl<T: Clone> Handler<ReadPair, Gate> {
    type Output = T;

    fn handle(
        &self,
        _: PhantomData<ReadPair>,
        gate: Gate,
        #[implicit] value: &T,
    ) -> impl Future<Output = Result<T, Error>> {
        let value = value.clone();
        async move {
            gate.wait().await;
            Ok(value)
        }
    }
}

type Record = (Gate, Cell<u16>);

#[cgp_impl(new RejectRecord)]
#[use_type(HasErrorType.Error = Rejected)]
impl Handler<ReadPair, Record> {
    type Output = bool;

    fn handle(
        &self,
        _: PhantomData<ReadPair>,
        record: Record,
    ) -> impl Future<Output = Result<bool, Error>> {
        let (gate, number) = record;
        async move {
            gate.wait().await;
            Err(Rejected(number.get()))
        }
    }
}

type WorkerChild = Child<Cell<u32>>;

impl SendHandler<WorkerChild, ReadPair, Gate> for ReadValue {
    fn handle_send(
        child: &WorkerChild,
        code: PhantomData<ReadPair>,
        gate: Gate,
    ) -> impl Future<Output = Result<Cell<u32>, Rejected>> + Send {
        Self::handle(child, code, gate)
    }
}

impl<T> SendHandler<Child<T>, ReadPair, Record> for RejectRecord {
    fn handle_send(
        child: &Child<T>,
        code: PhantomData<ReadPair>,
        record: Record,
    ) -> impl Future<Output = Result<bool, Rejected>> + Send {
        Self::handle(child, code, record)
    }
}

type Jobs = Product![ReadValue, RejectRecord];
type Local = SpawnLocalTasks<Jobs>;
type Workers = SpawnWorkerTasks<Jobs>;
type Inputs = Product![Gate, Record];
type Outcomes<T> = Product![
    TaskOutcome<Result<T, Rejected>>,
    TaskOutcome<Result<bool, Rejected>>
];

delegate_and_check_components! {
    #[check_trait(CheckLocalFork)]
    <R: CanSpawnLocalTask> Parent<R, Local> {
        #[check_params((ReadPair, Child<Rc<u32>>, Inputs))]
        HandlerTaskSpawnerComponent: Local,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckWorkerFork)]
    <R: CanSpawnTask> Parent<R, Workers> {
        #[check_params((ReadPair, Child<Cell<u32>>, Inputs))]
        HandlerTaskSpawnerComponent: Workers,
    }
}

async fn reverse_completion<T, P>(parent: &P, value: T, local: bool) -> T
where
    P: CanSpawnHandlerTasks<ReadPair, Child<T>, Inputs, Outcomes = Outcomes<T>>,
{
    let caller = thread::current().id();
    let (start, started) = bounded(2);
    let (first, first_gate) = bounded(1);
    let (second, second_gate) = bounded(1);
    let inputs = product![
        Gate {
            start: start.clone(),
            release: first_gate
        },
        (
            Gate {
                start,
                release: second_gate
            },
            Cell::new(17)
        ),
    ];
    let tasks = parent.spawn(PhantomData, Child { value }, inputs);

    // Observe both children start and the second pass its gate before calling join.
    for _ in 0..2 {
        assert_eq!(started.recv().await.unwrap() == caller, local);
    }
    second.try_send(()).unwrap();
    second.closed().await;
    let mut joined = pin!(tasks.join());
    let mut release_first = Some(first);
    let product![first, second] = poll_fn(|cx| {
        let state = joined.as_mut().poll(cx);
        if let Some(release) = release_first.take() {
            assert!(state.is_pending());
            release.try_send(()).unwrap();
        }
        state
    })
    .await;
    assert_eq!(second, TaskOutcome::Completed(Err(Rejected(17))));
    match first {
        TaskOutcome::Completed(Ok(value)) => value,
        _ => panic!("first child did not return its value"),
    }
}

async fn drop_pending<P, Spawner>(parent: &P)
where
    Spawner: HandlerTaskSpawner<P, ReadPair, Child<Sender<()>>, Product![Record, Record]>,
{
    for join_first in [false, true] {
        let (value, contexts) = bounded::<()>(1);
        let (start, started) = bounded(2);
        let (release, blocked) = bounded(1);
        let gate = Gate {
            start,
            release: blocked,
        };
        let records = product![(gate.clone(), Cell::new(1)), (gate, Cell::new(2))];
        let tasks = Spawner::spawn(parent, PhantomData, Child { value }, records);
        for _ in 0..2 {
            started.recv().await.unwrap();
        }
        // Each pending child holds a sender. A sender retained by the parent could
        // keep a channel open even after every child has released its copy.
        assert_eq!(contexts.sender_count(), 2);
        if join_first {
            // poll_once consumes the join future, dropping it after its first Pending.
            assert!(future::poll_once(tasks.join()).await.is_none());
        } else {
            drop(tasks);
        }
        // The executor reclaims both aborted children while their gate stays shut.
        release.closed().await;
        assert!(contexts.recv().await.is_err());
    }
}

async fn local(runtime: impl CanSpawnLocalTask) {
    let parent = Parent {
        runtime,
        branches: PhantomData::<Local>,
    };
    let value = Rc::new(42);
    let returned = reverse_completion(&parent, value.clone(), true).await;
    assert!(Rc::ptr_eq(&returned, &value));
    type Pending = SpawnLocalTasks<Product![RejectRecord, RejectRecord]>;
    drop_pending::<_, Pending>(&parent).await;

    type Empty = SpawnLocalTasks<Product![]>;
    assert_eq!(
        Empty::spawn(&parent, PhantomData::<ReadPair>, (), product![])
            .join()
            .await,
        product![]
    );
    type One = SpawnLocalTasks<Product![ReturnInput]>;
    let one = One::spawn(
        &parent,
        PhantomData::<ReadPair>,
        Child { value: () },
        product![7],
    );
    drop(parent);
    assert_eq!(one.await, product![TaskOutcome::Completed(Ok(7))]);
}

async fn worker(runtime: impl CanSpawnTask) {
    let parent = Parent {
        runtime,
        branches: PhantomData::<Workers>,
    };
    assert_eq!(
        reverse_completion(&parent, Cell::new(42), false)
            .await
            .get(),
        42
    );
    type Pending = SpawnWorkerTasks<Product![RejectRecord, RejectRecord]>;
    drop_pending::<_, Pending>(&parent).await;

    // Move the group out of the parent and join its worker tasks on another thread.
    let (start, _started) = bounded(2);
    let (release, blocked) = bounded(2);
    let gate = Gate {
        start,
        release: blocked,
    };
    let inputs = product![gate.clone(), (gate, Cell::new(7))];
    let tasks = parent.spawn(
        PhantomData::<ReadPair>,
        Child {
            value: Cell::new(3),
        },
        inputs,
    );
    drop(parent);
    release.try_send(()).unwrap();
    release.try_send(()).unwrap();
    let results = thread::spawn(move || future::block_on(tasks))
        .join()
        .unwrap();
    assert_eq!(
        results,
        product![
            TaskOutcome::Completed(Ok(Cell::new(3))),
            TaskOutcome::Completed(Err(Rejected(7)))
        ]
    );
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_local_children_run_before_join() {
    executors::tokio_local(async |runtime| local(runtime).await);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_worker_children_run_before_join() {
    executors::tokio_worker(async |runtime| worker(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_local_children_run_before_join() {
    executors::smol_local(async |runtime| local(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_worker_children_run_before_join() {
    executors::smol_worker(async |runtime| worker(runtime).await);
}
