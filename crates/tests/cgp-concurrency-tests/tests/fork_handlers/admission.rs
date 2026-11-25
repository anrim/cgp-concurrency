use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_channel::{Receiver, Sender, bounded};
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_concurrency::runtime::TaskControl;
use cgp_handler::{Handler, HandlerComponent};
use futures_lite::{Stream, future};

use super::{
    Child, Gate, Inputs, Local, Outcomes, PairGates, Parent, ReadPair, Rejected, Workers, executors,
};

async fn accepted<T: Clone, P>(parent: P, value: T, local: bool) -> T
where
    P: CanSpawnHandlerTasks<
            ReadPair,
            Child<(T, PanicOnDrop)>,
            Inputs,
            Outcomes = Outcomes<(T, PanicOnDrop)>,
        > + CanTrySpawnHandlerTasks<
            ReadPair,
            Child<(T, PanicOnDrop)>,
            Inputs,
            Outcomes = Outcomes<(T, PanicOnDrop)>,
        >,
{
    let (gates, inputs) = PairGates::inputs();
    let (original, released) = PanicOnDrop::observed();
    let tasks = parent.spawn(
        PhantomData,
        Child {
            value: (value.clone(), original),
        },
        inputs,
    );
    assert!(released.is_closed());
    gates.join(tasks, local).await;
    let (gates, inputs) = PairGates::inputs();
    let (original, released) = PanicOnDrop::observed();
    let child = Child {
        value: (value, original),
    };
    let token = CancellationToken::new();
    token.request_cancel();
    assert!(matches!(
        parent
            .try_spawn(&token, PhantomData, child.clone(), inputs.clone())
            .await,
        Err(SpawnError::Cancelled)
    ));
    assert!(gates.started.is_empty());
    let tasks = parent
        .try_spawn(&CancellationToken::new(), PhantomData, child, inputs)
        .await
        .unwrap();
    assert!(
        released.is_closed(),
        "admission retained the original context"
    );
    drop(parent);
    gates.join(tasks, local).await.0
}

// Submit to the real adapter. Refuse at the configured index and gate
// observation of each native handle's termination independently.
struct RefuseAt<'a, R> {
    runtime: &'a R,
    refuse_at: usize,
    attempts: Cell<usize>,
    first_finished: Option<Sender<()>>,
    gates: [Receiver<()>; 2],
    observed: Arc<Observed>,
}

struct Observed {
    aborts: AtomicUsize,
    drained: Sender<usize>,
}

impl<R> RefuseAt<'_, R> {
    fn attempt(&self, token: &CancellationToken) -> usize {
        let index = self.attempts.get();
        self.attempts.set(index + 1);
        if index == self.refuse_at {
            if let Some(first) = &self.first_finished {
                // Workers progress on another thread. A has passed its gate and produced a value.
                future::block_on(first.closed());
            }
            token.request_cancel();
        }
        assert!(index <= self.refuse_at, "admission continued after refusal");
        index
    }

    fn track<H>(&self, index: usize, inner: H) -> Tracked<H> {
        Tracked {
            inner,
            bit: 1 << index,
            gate: Box::pin(self.gates[index].clone()),
            observed: self.observed.clone(),
        }
    }
}

struct Tracked<H> {
    inner: H,
    bit: usize,
    gate: Pin<Box<Receiver<()>>>,
    observed: Arc<Observed>,
}

impl<T, H: TaskHandle<T>> TaskHandle<T> for Tracked<H> {
    fn control(&self) -> &TaskControl {
        self.inner.control()
    }

    fn abort(&self) {
        self.observed.aborts.fetch_or(self.bit, Ordering::SeqCst);
        self.inner.abort();
    }
}

impl<H: Future + Unpin> Future for Tracked<H> {
    type Output = H::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(
            self.observed.aborts.load(Ordering::SeqCst),
            0b11,
            "drain before both aborts"
        );
        if self.gate.as_mut().poll_next(cx).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll(cx).map(|outcome| {
            self.observed.drained.try_send(self.bit).unwrap();
            outcome
        })
    }
}

impl<R: CanSpawnLocalTask> CanSpawnLocalTask for RefuseAt<'_, R> {
    type LocalJoinHandle<T: 'static> = Tracked<R::LocalJoinHandle<T>>;

    fn spawn_local_task<F: Future<Output = T> + 'static, T: 'static>(
        &self,
        _: F,
    ) -> Self::LocalJoinHandle<T> {
        panic!("expected fallible local admission")
    }

    fn try_spawn_local_task<F, T>(
        &self,
        token: &CancellationToken,
        work: F,
    ) -> Result<Self::LocalJoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        let index = self.attempt(token);
        if index == self.refuse_at {
            return Err(SpawnError::Cancelled);
        }
        Ok(self.track(index, self.runtime.try_spawn_local_task(token, work)?))
    }
}

impl<R: CanSpawnTask> CanSpawnTask for RefuseAt<'_, R> {
    type JoinHandle<T: Send + 'static> = Tracked<R::JoinHandle<T>>;

    fn spawn_task<F: Future<Output = T> + Send + 'static, T: Send + 'static>(
        &self,
        _: F,
    ) -> Self::JoinHandle<T> {
        panic!("expected fallible worker admission")
    }

    fn try_spawn_task<F, T>(
        &self,
        token: &CancellationToken,
        work: F,
    ) -> Result<Self::JoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static,
    {
        let index = self.attempt(token);
        if index == self.refuse_at {
            return Err(SpawnError::Cancelled);
        }
        Ok(self.track(index, self.runtime.try_spawn_task(token, work)?))
    }
}

struct PanicOnDrop(Option<Sender<()>>);

impl PanicOnDrop {
    fn observed() -> (Self, Receiver<()>) {
        let (owner, released) = bounded(1);
        (Self(Some(owner)), released)
    }
}

impl Clone for PanicOnDrop {
    fn clone(&self) -> Self {
        // For the child context, only destruction of the original should panic.
        Self(None)
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        if self.0.is_some() {
            panic!("discarded application value");
        }
    }
}

#[cgp_impl(new ReturnAfterGate)]
impl<T> Handler<ReadPair, (Gate, T)> for Child<PanicOnDrop> {
    type Output = T;

    async fn handle(
        &self,
        _: PhantomData<ReadPair>,
        (gate, value): (Gate, T),
    ) -> Result<T, Rejected> {
        gate.wait().await;
        Ok(value)
    }
}

impl<T: Send> SendHandler<Child<PanicOnDrop>, ReadPair, (Gate, T)> for ReturnAfterGate {
    fn handle_send(
        child: &Child<PanicOnDrop>,
        code: PhantomData<ReadPair>,
        input: (Gate, T),
    ) -> impl Future<Output = Result<T, Rejected>> + Send {
        Self::handle(child, code, input)
    }
}

type Jobs = Product![
    ReturnAfterGate,
    ReturnAfterGate,
    ReturnAfterGate,
    ReturnAfterGate,
    ReturnAfterGate
];
type GatedInputs = Product![
    (Gate, PanicOnDrop),
    (Gate, ()),
    (Gate, ()),
    (Gate, PanicOnDrop),
    (Gate, PanicOnDrop)
];

async fn partial<R, Spawner>(runtime: &R, local: bool, interrupt: bool)
where
    for<'a> Spawner: TryHandlerTaskSpawner<
            Parent<RefuseAt<'a, R>, Spawner>,
            ReadPair,
            Child<PanicOnDrop>,
            GatedInputs,
        >,
{
    let (first, first_gate) = bounded(1);
    first.try_send(()).unwrap();
    let (second, second_gate) = bounded(1);
    let (allow_first, drain_first) = bounded(1);
    let (allow_second, drain_second) = bounded(1);
    let (start, started) = bounded(4);
    let gate = |release| Gate {
        start: start.clone(),
        release,
    };
    let (output, output_released) = PanicOnDrop::observed();
    let (unused_d, released_d) = PanicOnDrop::observed();
    let (unused_e, released_e) = PanicOnDrop::observed();
    let (child, child_released) = PanicOnDrop::observed();
    let inputs = product![
        (gate(first_gate), output),
        (gate(second_gate.clone()), ()),
        (gate(second_gate.clone()), ()),
        (gate(second_gate.clone()), unused_d),
        (gate(second_gate), unused_e),
    ];
    let (signal, drained) = bounded(2);
    let parent = Parent {
        runtime: RefuseAt {
            runtime,
            refuse_at: 2,
            attempts: Cell::new(0),
            first_finished: (!local).then(|| first.clone()),
            gates: [drain_first, drain_second],
            observed: Arc::new(Observed {
                aborts: AtomicUsize::new(0),
                drained: signal,
            }),
        },
        branches: PhantomData::<Spawner>,
    };
    let token = CancellationToken::new();
    let mut submission = Box::pin(Spawner::try_spawn(
        &parent,
        &token,
        PhantomData,
        Child { value: child },
        inputs,
    ));
    assert_eq!(parent.runtime.attempts.get(), 0);
    assert!(future::poll_once(&mut submission).await.is_none());
    assert_eq!(parent.runtime.attempts.get(), 3);
    assert!(token.is_cancelled());
    assert_eq!(parent.runtime.observed.aborts.load(Ordering::SeqCst), 0b11);
    assert_eq!(started.is_empty(), local);
    assert!(child_released.is_closed() && released_d.is_closed() && released_e.is_closed());
    if !local {
        assert!(
            !output_released.is_closed(),
            "A's output is owned by its gated handle"
        );
    }

    // Drive the submission until A actually drains. B's drain gate stays closed.
    drop(allow_first);
    let first_drained = future::or(
        async {
            let _ = submission.as_mut().await;
            panic!("returned before B drained")
        },
        drained.recv(),
    )
    .await
    .unwrap();
    assert_eq!(first_drained, 0b01);
    assert!(output_released.is_closed(), "A's output survived its drain");
    if interrupt {
        drop(submission);
        drop(parent); // Release the fixture's copy of the drain gate.
        assert!(
            allow_second.is_closed(),
            "B's handle survived the dropped wait"
        );
        second.closed().await;
    } else {
        drop(allow_second);
        assert!(matches!(submission.await, Err(SpawnError::Cancelled)));
        assert_eq!(drained.try_recv().unwrap(), 0b10);
        assert!(second.is_closed(), "B survived the admission error");
    }
}

// Every context or input records whether another destructor is already unwinding.
#[derive(Clone)]
struct Capture(Sender<bool>);

impl Drop for Capture {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        self.0.try_send(unwinding).unwrap();
        if !unwinding {
            panic!("rejected capture");
        }
    }
}

impl SendHandler<Child<Capture>, ReadPair, Capture> for cgp_handler::ReturnInput {
    fn handle_send(
        child: &Child<Capture>,
        code: PhantomData<ReadPair>,
        input: Capture,
    ) -> impl Future<Output = Result<Capture, Rejected>> + Send {
        Self::handle(child, code, input)
    }
}

type CaptureJobs = Product![cgp_handler::ReturnInput, cgp_handler::ReturnInput];

async fn unused_captures<R, S>(runtime: &R)
where
    for<'a> S: TryHandlerTaskSpawner<
            Parent<RefuseAt<'a, R>, S>,
            ReadPair,
            Child<Capture>,
            Product![Capture, Capture],
        >,
{
    enum Submission {
        Unpolled,
        Cancelled,
        Refused,
    }
    for mode in [
        Submission::Unpolled,
        Submission::Cancelled,
        Submission::Refused,
    ] {
        let prior = matches!(mode, Submission::Cancelled);
        let (signal, observed) = bounded(4);
        let token = CancellationToken::new();
        if prior {
            token.request_cancel();
        }
        let parent = Parent {
            runtime: RefuseAt {
                runtime,
                // Prior requests reach the native adapter. Otherwise the fixture refuses the first child.
                refuse_at: if prior { 2 } else { 0 },
                attempts: Cell::new(0),
                first_finished: None,
                gates: std::array::from_fn(|_| bounded(1).1),
                observed: Arc::new(Observed {
                    aborts: AtomicUsize::new(0),
                    drained: bounded(1).0,
                }),
            },
            branches: PhantomData::<S>,
        };
        let child = Child {
            value: Capture(signal.clone()),
        };
        let submission = S::try_spawn(
            &parent,
            &token,
            PhantomData,
            child,
            product![Capture(signal.clone()), Capture(signal)],
        );
        let expected_drops = if matches!(mode, Submission::Unpolled) {
            drop(submission);
            assert_eq!(parent.runtime.attempts.get(), 0);
            3 // Original context and two inputs. No context was cloned.
        } else {
            assert!(matches!(submission.await, Err(SpawnError::Cancelled)));
            assert_eq!(parent.runtime.attempts.get(), 1);
            4 // The refused branch also owns a context clone.
        };
        for _ in 0..expected_drops {
            assert_eq!(
                observed.try_recv(),
                Ok(false),
                "each capture must drop outside the other's unwind"
            );
        }
    }
}

async fn local(runtime: impl CanSpawnLocalTask) {
    unused_captures::<_, SpawnLocalTasks<CaptureJobs>>(&runtime).await;
    for interrupt in [false, true] {
        partial::<_, SpawnLocalTasks<Jobs>>(&runtime, true, interrupt).await;
    }
    let value = std::rc::Rc::new(42);
    let parent = Parent {
        runtime,
        branches: PhantomData::<Local>,
    };
    let token = CancellationToken::new();
    token.request_cancel();
    let (unused, released) = PanicOnDrop::observed();
    let empty =
        SpawnLocalTasks::<Product![]>::spawn(&parent, PhantomData::<ReadPair>, unused, product![]);
    assert!(released.is_closed());
    assert_eq!(empty.await, product![]);
    let (unused, released) = PanicOnDrop::observed();
    let empty = SpawnLocalTasks::<Product![]>::try_spawn(
        &parent,
        &token,
        PhantomData::<ReadPair>,
        unused,
        product![],
    )
    .await
    .unwrap();
    assert!(released.is_closed());
    assert_eq!(empty.await, product![]);
    let result = accepted(parent, value.clone(), true).await;
    assert!(std::rc::Rc::ptr_eq(&value, &result));
}

async fn worker(runtime: impl CanSpawnTask) {
    unused_captures::<_, SpawnWorkerTasks<CaptureJobs>>(&runtime).await;
    for interrupt in [false, true] {
        partial::<_, SpawnWorkerTasks<Jobs>>(&runtime, false, interrupt).await;
    }
    let parent = Parent {
        runtime,
        branches: PhantomData::<Workers>,
    };
    assert_eq!(accepted(parent, Cell::new(42), false).await.get(), 42);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_local() {
    executors::tokio_local(async |runtime| local(runtime).await);
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_worker() {
    executors::tokio_worker(async |runtime| worker(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_local() {
    executors::smol_local(async |runtime| local(runtime).await);
}

#[cfg(feature = "smol")]
#[test]
fn smol_worker() {
    executors::smol_worker(async |runtime| worker(runtime).await);
}
