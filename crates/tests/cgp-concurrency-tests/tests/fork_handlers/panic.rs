use core::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use async_channel::{Sender, bounded};
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, HandlerComponent};

use super::{Child, ReadPair, Rejected};

pub(super) struct ContextDrop {
    arm_next_clone: Arc<AtomicBool>,
    armed: bool,
    observed: Sender<bool>,
}

impl Clone for ContextDrop {
    fn clone(&self) -> Self {
        Self {
            arm_next_clone: self.arm_next_clone.clone(),
            armed: self.arm_next_clone.swap(false, Ordering::Relaxed),
            observed: self.observed.clone(),
        }
    }
}

impl Drop for ContextDrop {
    fn drop(&mut self) {
        if self.armed {
            record_drop(&self.observed, "context cleanup");
        }
    }
}

fn record_drop(observed: &Sender<bool>, message: &'static str) {
    let unwinding = thread::panicking();
    observed.try_send(unwinding).unwrap();
    // Report an overlapping unwind without aborting the test runner.
    if !unwinding {
        panic!("{message}");
    }
}

pub(super) struct Failure {
    panic_in_poll: bool,
    cleanup: Option<Sender<bool>>,
}

impl Drop for Failure {
    fn drop(&mut self) {
        if let Some(observed) = &self.cleanup {
            record_drop(observed, "handler cleanup");
        }
    }
}

#[cgp_impl(new FailOnDrop)]
impl Handler<ReadPair, Failure> for Child<ContextDrop> {
    type Output = ();

    fn handle(
        &self,
        _: PhantomData<ReadPair>,
        failure: Failure,
    ) -> impl Future<Output = Result<(), Rejected>> {
        poll_fn(move |_| {
            let failure = &failure;
            assert!(!failure.panic_in_poll, "handler poll");
            core::task::Poll::Ready(Ok(()))
        })
    }
}

impl SendHandler<Child<ContextDrop>, ReadPair, Failure> for FailOnDrop {
    fn handle_send(
        child: &Child<ContextDrop>,
        code: PhantomData<ReadPair>,
        input: Failure,
    ) -> impl Future<Output = Result<(), Rejected>> + Send {
        Self::handle(child, code, input)
    }
}

pub(super) type Jobs = Product![FailOnDrop, FailOnDrop];
type Inputs = Product![Failure, Failure];
type Outcome = TaskOutcome<Result<(), Rejected>>;
type Outcomes = Product![Outcome, Outcome];

pub(super) async fn check<P, S>(parent: &P)
where
    S: HandlerTaskSpawner<P, ReadPair, Child<ContextDrop>, Inputs, Outcomes = Outcomes>,
{
    for (panic_in_poll, panic_in_cleanup, expected) in [
        (true, true, "handler poll"),
        (false, true, "handler cleanup"),
        (false, false, "context cleanup"),
    ] {
        let (signal, observed) = bounded(2);
        let child = Child {
            value: ContextDrop {
                arm_next_clone: Arc::new(AtomicBool::new(true)),
                armed: false,
                observed: signal.clone(),
            },
        };
        let inputs = product![
            Failure {
                panic_in_poll,
                cleanup: panic_in_cleanup.then_some(signal)
            },
            Failure {
                panic_in_poll: false,
                cleanup: None
            }
        ];
        let product![failed, sibling] = S::spawn(parent, PhantomData, child, inputs).await;
        assert_eq!(failed, TaskOutcome::Panicked(Some(expected.into())));
        assert_eq!(sibling, TaskOutcome::Completed(Ok(())));
        for _ in 0..1 + usize::from(panic_in_cleanup) {
            assert_eq!(
                observed.try_recv(),
                Ok(false),
                "destructor ran during unwind"
            );
        }
    }
}
