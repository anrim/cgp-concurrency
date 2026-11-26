//! Both children start before the caller joins. The second passes its gate first,
//! and the join waits for the first child before returning outcomes in declaration order.

#![cfg(any(feature = "smol", feature = "tokio"))]

extern crate cgp_core as cgp;

#[path = "fork_handlers/admission.rs"]
mod admission;
#[path = "fork_handlers/cancellation.rs"]
mod cancellation;
#[path = "support/executors.rs"]
mod executors;
#[path = "support/join_notification.rs"]
mod join_notification;
#[path = "fork_handlers/panic.rs"]
mod panic;

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
        self.release.recv().await.expect("branch gate closed");
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

impl_send_handler!([T] (Child<T>, ReadPair, Gate) for ReadValue where T: Clone + Send);

impl_send_handler!([T] (Child<T>, ReadPair, Record) for RejectRecord);

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
        [HandlerTaskSpawnerComponent, TryHandlerTaskSpawnerComponent]: Local,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckWorkerFork)]
    <R: CanSpawnTask> Parent<R, Workers> {
        #[check_params((ReadPair, Child<Cell<u32>>, Inputs))]
        [HandlerTaskSpawnerComponent, TryHandlerTaskSpawnerComponent]: Workers,
    }
}

struct PairGates {
    first: Sender<()>,
    second: Sender<()>,
    started: Receiver<ThreadId>,
}

impl PairGates {
    fn inputs() -> (Self, Inputs) {
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
        (
            Self {
                first,
                second,
                started,
            },
            inputs,
        )
    }

    async fn join<T>(
        self,
        tasks: HandlerTasks<impl Future<Output = Outcomes<T>>>,
        local: bool,
    ) -> T {
        let caller = thread::current().id();
        // Observe both children start and the second pass its gate before calling join.
        for _ in 0..2 {
            assert_eq!(self.started.recv().await.unwrap() == caller, local);
        }
        self.second.try_send(()).unwrap();
        self.second.closed().await;
        let product![first, second] = join_after_pending(tasks.join(), &self.first).await;
        assert_eq!(second, TaskOutcome::Completed(Err(Rejected(17))));
        match first {
            TaskOutcome::Completed(Ok(value)) => value,
            _ => panic!("first child did not return its value"),
        }
    }
}

async fn reverse_completion<T, P>(parent: &P, value: T, local: bool) -> T
where
    P: CanSpawnHandlerTasks<ReadPair, Child<T>, Inputs, Outcomes = Outcomes<T>>,
{
    let (gates, inputs) = PairGates::inputs();
    let tasks = parent.spawn(PhantomData, Child { value }, inputs);
    gates.join(tasks, local).await
}

// Open the last gate after the join registers its waker, then let the executor repoll it.
async fn join_after_pending<F: Future>(join: F, release: &Sender<()>) -> F::Output {
    let mut joined = pin!(join);
    let mut gate = Some(release);
    poll_fn(|cx| {
        let state = joined.as_mut().poll(cx);
        if let Some(gate) = gate.take() {
            assert!(state.is_pending());
            gate.try_send(()).unwrap();
        }
        state
    })
    .await
}

async fn child_panic_preserves_sibling<T, P>(parent: &P, value: T)
where
    P: CanSpawnHandlerTasks<ReadPair, Child<T>, Inputs, Outcomes = Outcomes<T>>,
{
    let (first_start, first_started) = bounded(1);
    let (fail, first_gate) = bounded(1);
    let (second_start, second_started) = bounded(1);
    let (resume, second_gate) = bounded(1);
    let inputs = product![
        Gate {
            start: first_start,
            release: first_gate
        },
        (
            Gate {
                start: second_start,
                release: second_gate
            },
            Cell::new(17)
        )
    ];
    let tasks = parent.spawn(PhantomData, Child { value }, inputs);
    first_started.recv().await.unwrap();
    second_started.recv().await.unwrap();
    drop(fail);
    // Unwinding drops the first handler's only sender while its sibling is still gated.
    assert!(first_started.recv().await.is_err());
    let mut joined = pin!(tasks.join());
    assert!(future::poll_once(&mut joined).await.is_none());
    resume.try_send(()).unwrap();

    let product![first, second] = joined.await;
    assert!(
        matches!(first, TaskOutcome::Panicked(Some(message)) if message.starts_with("branch gate closed"))
    );
    assert_eq!(second, TaskOutcome::Completed(Err(Rejected(17))));
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
    child_panic_preserves_sibling(&parent, value).await;
    panic::check::<_, SpawnLocalTasks<panic::Jobs>>(&parent).await;
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
    child_panic_preserves_sibling(&parent, Cell::new(42)).await;
    panic::check::<_, SpawnWorkerTasks<panic::Jobs>>(&parent).await;
    type Pending = SpawnWorkerTasks<Product![RejectRecord, RejectRecord]>;
    drop_pending::<_, Pending>(&parent).await;

    // ReturnInput needs neither a Sync child context nor a Send code tag.
    type One = SpawnWorkerTasks<Product![ReturnInput]>;
    let one = One::spawn(
        &parent,
        PhantomData::<Rc<()>>,
        Child {
            value: Cell::new(0),
        },
        product![Cell::new(7)],
    );
    assert_eq!(
        one.await,
        product![TaskOutcome::Completed(Ok(Cell::new(7)))]
    );

    // Move both groups out of the parent. The workers ignore cooperative requests.
    let (start, _started) = bounded(4);
    let (release, blocked) = bounded(4);
    let gate = Gate {
        start,
        release: blocked,
    };
    let inputs = product![gate.clone(), (gate, Cell::new(7))];
    let spawn = || {
        parent.spawn_cancellable(
            PhantomData::<ReadPair>,
            |token| Child {
                value: (Cell::new(3), token),
            },
            inputs.clone(),
        )
    };
    let cancelled = spawn();
    let joined = spawn();
    drop(parent);
    let (cancelled, joined) = thread::spawn(move || {
        let drain = cancelled.cancel();
        for _ in 0..4 {
            release.try_send(()).unwrap();
        }
        future::block_on(future::zip(drain, joined))
    })
    .join()
    .unwrap();
    for (product![first, second], requested) in [(cancelled, true), (joined, false)] {
        let TaskOutcome::Completed(Ok((value, token))) = first else {
            panic!("worker did not return its value and group token");
        };
        assert_eq!(value.get(), 3);
        assert_eq!(token.is_cancelled(), requested);
        assert_eq!(second, TaskOutcome::Completed(Err(Rejected(7))));
    }
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
