use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use crate::{TaskOutcome, discard, panic_message};

/// Collects panics from task destruction before a native join publishes termination.
pub struct TaskCompletion {
    cleanup_panic: Arc<OnceLock<Option<String>>>,
}

impl TaskCompletion {
    /// Wraps the future at submission, including cleanup before its first poll.
    pub fn track<F, T>(future: F) -> (Self, impl Future<Output = UnclaimedOutput<T>> + use<F, T>)
    where
        F: Future<Output = TaskOutcome<T>>,
    {
        let cleanup_panic = Arc::default();
        let work = Work {
            future: Some(Box::pin(future)),
            cleanup_panic: Arc::clone(&cleanup_panic),
        };
        (Self { cleanup_panic }, work)
    }

    /// Consumes the native join result after the executor has destroyed its future.
    pub fn finish<T>(&self, mut output: Option<UnclaimedOutput<T>>) -> TaskOutcome<T> {
        let outcome = output.as_mut().and_then(|output| output.0.take());
        match (outcome, self.cleanup_panic.get()) {
            (Some(panic @ TaskOutcome::Panicked(_)), _) => panic,
            (superseded, Some(message)) => {
                discard(superseded);
                TaskOutcome::Panicked(message.clone())
            }
            (Some(outcome), None) => outcome,
            (None, None) => TaskOutcome::RuntimeFailure,
        }
    }
}

/// A native join's output. An abandoned application value stays guarded until it is destroyed.
pub struct UnclaimedOutput<T>(Option<TaskOutcome<T>>);

impl<T> Drop for UnclaimedOutput<T> {
    fn drop(&mut self) {
        if let Some(outcome) = self.0.take() {
            discard(outcome);
        }
    }
}

struct Work<F> {
    future: Option<Pin<Box<F>>>,
    cleanup_panic: Arc<OnceLock<Option<String>>>,
}

impl<F, T> Future for Work<F>
where
    F: Future<Output = TaskOutcome<T>>,
{
    type Output = UnclaimedOutput<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.future.as_mut().expect("work is present until drop");
        let outcome = match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll,
            Err(payload) => Poll::Ready(TaskOutcome::from_panic(payload)),
        };
        outcome.map(|value| UnclaimedOutput(Some(value)))
    }
}

impl<F> Drop for Work<F> {
    fn drop(&mut self) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(self.future.take()))) {
            // The native join becomes ready after destruction, including at shutdown.
            let _ = self.cleanup_panic.set(panic_message(payload));
        }
    }
}
