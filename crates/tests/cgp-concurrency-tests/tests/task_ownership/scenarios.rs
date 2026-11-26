use std::cell::Cell;
use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};

use async_channel::{Receiver, Sender, bounded};
use cgp_concurrency::prelude::*;
use futures_lite::future;

use crate::app::{App, check_errors};
use crate::join_notification::{join_notification, wake_channel};

pub(super) struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("cleanup panic");
    }
}

// The caller supplies the future itself, so its poll and its destructor can fail independently.
fn cleanup_failure(panic_in_poll: bool) -> impl Future<Output = Result<PanicOnDrop, Cancelled>> {
    let cleanup = PanicOnDrop;
    future::poll_fn(move |_| {
        let _guard = &cleanup;
        if panic_in_poll {
            panic!("poll panic");
        }
        Poll::Ready(Ok(PanicOnDrop))
    })
}

async fn assert_task_panic<T>(mut owner: impl TaskHandle<T>, expected: &str) {
    assert!(
        matches!(owner.drain().await, TaskOutcome::Panicked(Some(message)) if message == expected)
    );
}

struct DropWitness(Sender<()>);

impl Drop for DropWitness {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

// Discarding this output also exercises disposal of a panic payload with its own destructor.
struct PanickingOutput(Sender<()>);

impl Drop for PanickingOutput {
    fn drop(&mut self) {
        self.0.try_send(()).unwrap();
        std::panic::panic_any(PanicOnDrop);
    }
}

pub async fn signal_then_wait(start: Sender<()>) {
    start.try_send(()).unwrap();
    future::pending::<()>().await;
}

// The caller deliberately leaves the native executor idle while polling this handle.
pub fn assert_passive_drain(owner: &mut impl TaskHandle<()>, started: &Receiver<()>) {
    assert!(future::block_on(future::poll_once(owner.drain())).is_none());
    assert!(started.is_empty(), "polling a join ran queued work");
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Case {
    Refused,
    Complete,
    ApplicationError,
    Panic,
    PanicOnAbort,
    PanickingPayload,
    CancelEarly,
    CancelRegistered,
    IgnoreRequest,
    AbortPending,
    AbortCompleted,
    DropPending,
}

struct Gates {
    started: Receiver<ThreadId>,
    register: Sender<()>,
    registered: Receiver<()>,
    finish: Sender<()>,
    destroyed: Receiver<()>,
}

type ApplicationResult<T> = Result<T, &'static str>;

fn fixture<T>(
    value: T,
    token: CancellationToken,
    case: Case,
) -> (
    Gates,
    impl Future<Output = Result<ApplicationResult<T>, Cancelled>>,
) {
    let (start, started) = bounded(1);
    let (register, allow_registration) = bounded(1);
    let (ready, registered) = bounded(1);
    let (finish, release) = bounded(1);
    let (dropped, destroyed) = bounded(1);
    let witness = DropWitness(dropped);
    let cleanup = if matches!(case, Case::PanicOnAbort | Case::Refused) {
        Some(PanicOnDrop)
    } else {
        None
    };
    let task = async move {
        let _witness = witness;
        let _cleanup = cleanup;
        start.try_send(thread::current().id()).unwrap();
        allow_registration.recv().await.unwrap();
        let mut work = pin!(future::or(
            async move {
                release.recv().await.unwrap();
                match case {
                    Case::ApplicationError => Ok(Err("application error")),
                    Case::Panic => panic!("task panic"),
                    Case::PanickingPayload => std::panic::panic_any(PanicOnDrop),
                    _ => Ok(Ok(value)),
                }
            },
            async move {
                if matches!(case, Case::IgnoreRequest | Case::AbortPending) {
                    future::pending::<()>().await;
                }
                Err(token.cancelled().await)
            },
        ));
        let first = future::poll_once(&mut work).await;
        ready.try_send(()).unwrap();
        match first {
            Some(result) => result,
            None => work.await,
        }
    };
    (
        Gates {
            started,
            register,
            registered,
            finish,
            destroyed,
        },
        task,
    )
}

pub async fn local(runtime: impl CanSpawnLocalTask) {
    let runtime = App { runtime };
    assert_task_panic(
        runtime.spawn_local_task(cleanup_failure(true)),
        "poll panic",
    )
    .await;
    let owner = runtime
        .try_spawn_local_task(&CancellationToken::new(), cleanup_failure(false))
        .unwrap();
    assert_task_panic(owner, "cleanup panic").await;
    for panic_in_task in [false, true] {
        let (release, gate) = bounded(1);
        let (dropped, destroyed) = bounded(1);
        let mut owner = runtime.spawn_local_task(async move {
            gate.recv().await.unwrap();
            let output = PanickingOutput(dropped);
            if panic_in_task {
                std::panic::panic_any(output);
            }
            output
        });
        let published = join_notification(&mut owner);
        release.try_send(()).unwrap();
        published.recv().await.unwrap();
        drop(owner);
        destroyed.recv().await.unwrap();
    }

    run(Rc::new(42), true, |value, token, case| {
        let (gates, task) = fixture(value, token.clone(), case);
        (gates, runtime.try_spawn_local_task(token, task))
    })
    .await;
}

pub async fn cross_thread(runtime: impl CanSpawnTask) {
    let runtime = App { runtime };
    check_errors(&runtime).await;
    assert_task_panic(runtime.spawn_task(cleanup_failure(true)), "poll panic").await;
    let owner = runtime
        .try_spawn_task(&CancellationToken::new(), cleanup_failure(false))
        .unwrap();
    assert_task_panic(owner, "cleanup panic").await;
    let (start, started) = bounded(1);
    let (release, gate) = std::sync::mpsc::channel();
    let (dropped, destroyed) = bounded(1);
    let owner = runtime.spawn_task(async move {
        start.try_send(()).unwrap();
        // Keep the final poll on the worker until the caller drops the owner.
        gate.recv().unwrap();
        PanickingOutput(dropped)
    });
    started.recv().await.unwrap();
    drop(owner);
    release.send(()).unwrap();
    destroyed.recv().await.unwrap();

    // A successful drain hands the output, including its destructor, to its new owner.
    let (dropped, destroyed) = bounded(1);
    let mut owner = runtime.spawn_task(async move { DropWitness(dropped) });
    let outcome = thread::spawn(move || future::block_on(owner.drain()))
        .join()
        .unwrap();
    let TaskOutcome::Completed(value) = outcome else {
        panic!("expected the transferred owner to drain a completed task");
    };
    assert!(destroyed.is_empty());
    drop(value);
    destroyed.try_recv().unwrap();

    // The future and its output can move to a worker without being Sync.
    run(Cell::new(42_u32), false, |value, token, case| {
        let (gates, task) = fixture(value, token.clone(), case);
        (gates, runtime.try_spawn_task(token, task))
    })
    .await;
}

#[test]
fn one_request_wakes_two_registered_waiters() {
    let token = CancellationToken::new();
    let mut first = pin!(token.cancelled());
    let mut second = pin!(token.cancelled());
    let (first_waker, wake_first) = wake_channel();
    let (second_waker, wake_second) = wake_channel();
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    assert!(first.as_mut().poll(&mut first_cx).is_pending());
    assert!(second.as_mut().poll(&mut second_cx).is_pending());

    token.request_cancel();
    wake_first.try_recv().expect("first waiter was not woken");
    wake_second.try_recv().expect("second waiter was not woken");
    assert!(first.poll(&mut first_cx).is_ready());
    assert!(second.poll(&mut second_cx).is_ready());
}

async fn run<T, H>(
    value: T,
    local: bool,
    spawn: impl Fn(T, &CancellationToken, Case) -> (Gates, Result<H, SpawnError>),
) where
    T: Clone + Debug + PartialEq,
    H: TaskHandle<ApplicationResult<T>>,
{
    for case in [
        Case::Refused,
        Case::Complete,
        Case::ApplicationError,
        Case::Panic,
        Case::PanicOnAbort,
        Case::PanickingPayload,
        Case::CancelEarly,
        Case::CancelRegistered,
        Case::IgnoreRequest,
        Case::AbortPending,
        Case::AbortCompleted,
        Case::DropPending,
    ] {
        let token = CancellationToken::new();
        if case == Case::Refused {
            token.request_cancel();
        }
        let (gates, admission) = spawn(value.clone(), &token, case);
        if case == Case::Refused {
            assert!(matches!(admission, Err(SpawnError::Cancelled)));
            assert!(gates.started.is_empty());
            gates.destroyed.try_recv().unwrap();
            continue;
        }
        let mut owner = admission.unwrap();
        let worker = gates.started.recv().await.unwrap();
        assert_eq!(worker == thread::current().id(), local, "{case:?}");
        assert!(future::poll_once(owner.drain()).await.is_none());
        if case == Case::CancelEarly {
            owner.request_cancel();
        }
        gates.register.try_send(()).unwrap();
        gates.registered.recv().await.unwrap();
        match case {
            Case::Complete | Case::IgnoreRequest => {
                if case == Case::IgnoreRequest {
                    owner.request_cancel();
                }
                gates.finish.try_send(()).unwrap();
                assert_eq!(
                    owner.drain().await,
                    TaskOutcome::Completed(Ok(value.clone()))
                );
            }
            Case::ApplicationError => {
                gates.finish.try_send(()).unwrap();
                assert_eq!(
                    owner.drain().await,
                    TaskOutcome::Completed(Err("application error"))
                );
            }
            Case::Panic => {
                gates.finish.try_send(()).unwrap();
                assert_eq!(
                    owner.drain().await,
                    TaskOutcome::Panicked(Some("task panic".into()))
                );
            }
            Case::PanicOnAbort => {
                owner.abort();
                assert_eq!(
                    owner.drain().await,
                    TaskOutcome::Panicked(Some("cleanup panic".into()))
                );
            }
            Case::PanickingPayload => {
                gates.finish.try_send(()).unwrap();
                assert_eq!(owner.drain().await, TaskOutcome::Panicked(None));
            }
            Case::CancelEarly | Case::CancelRegistered => {
                if case == Case::CancelRegistered {
                    owner.request_cancel();
                }
                assert_eq!(owner.drain().await, TaskOutcome::Cancelled);
            }
            Case::AbortPending => {
                owner.request_cancel();
                owner.abort();
                // Abort takes effect while the drain is unpolled.
                gates.destroyed.recv().await.unwrap();
                assert_eq!(owner.drain().await, TaskOutcome::Cancelled);
                continue;
            }
            Case::AbortCompleted => {
                // Join notification establishes publication. A destructor can run before it.
                let published = join_notification(&mut owner);
                gates.finish.try_send(()).unwrap();
                published.recv().await.unwrap();
                owner.abort();
                assert_eq!(
                    owner.drain().await,
                    TaskOutcome::Completed(Ok(value.clone()))
                );
            }
            Case::DropPending => {
                drop(owner);
                gates.destroyed.recv().await.unwrap();
                continue;
            }
            Case::Refused => unreachable!(),
        }
        gates
            .destroyed
            .try_recv()
            .expect("drain returned before destruction");
    }

    for abort_first in [false, true] {
        let token = CancellationToken::new();
        let (first, first_owner) = spawn(value.clone(), &token, Case::CancelRegistered);
        let (second, second_owner) = spawn(value.clone(), &token, Case::CancelRegistered);
        let mut first_owner = first_owner.unwrap();
        let mut second_owner = second_owner.unwrap();
        for gates in [&first, &second] {
            gates.register.try_send(()).unwrap();
            gates.registered.recv().await.unwrap();
        }
        if abort_first {
            first_owner.abort();
            assert_eq!(first_owner.drain().await, TaskOutcome::Cancelled);
            assert!(!token.is_cancelled());
            assert!(future::poll_once(second_owner.drain()).await.is_none());
        }
        token.request_cancel();
        if !abort_first {
            assert_eq!(first_owner.drain().await, TaskOutcome::Cancelled);
        }
        assert_eq!(second_owner.drain().await, TaskOutcome::Cancelled);
    }
}
