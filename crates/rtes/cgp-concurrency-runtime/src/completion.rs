use alloc::string::String;
use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use pin_project_lite::pin_project;

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
            future: Some(future),
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

pin_project! {
    struct Work<F> {
        #[pin]
        future: Option<F>,
        cleanup_panic: Arc<OnceLock<Option<String>>>,
    }

    impl<F> PinnedDrop for Work<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            // Drop in place before the native join publishes termination, even at shutdown.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| this.future.set(None))) {
                let _ = this.cleanup_panic.set(panic_message(payload));
            }
        }
    }
}

impl<F, T> Future for Work<F>
where
    F: Future<Output = TaskOutcome<T>>,
{
    type Output = UnclaimedOutput<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self
            .project()
            .future
            .as_pin_mut()
            .expect("work is present until drop");
        let outcome = match catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(poll) => poll,
            Err(payload) => Poll::Ready(TaskOutcome::from_panic(payload)),
        };
        outcome.map(|value| UnclaimedOutput(Some(value)))
    }
}
