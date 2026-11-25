use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use event_listener::Event;
use pin_project_lite::pin_project;

use crate::TaskOutcome;

/// A sticky cancellation request. Clones observe the same request.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<Signal>);

#[derive(Debug, Default)]
struct Signal {
    requested: AtomicBool,
    event: Event,
}

impl CancellationToken {
    /// Creates a token with no outstanding request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes all waiters. Repeated requests have the same effect as one.
    pub fn request_cancel(&self) {
        if !self.0.requested.swap(true, Ordering::AcqRel) {
            self.0.event.notify(usize::MAX);
        }
    }

    /// Returns whether anyone has requested cancellation.
    pub fn is_cancelled(&self) -> bool {
        self.0.requested.load(Ordering::Acquire)
    }

    /// Waits for a request and returns the marker a task can use to acknowledge it.
    pub async fn cancelled(&self) -> Cancelled {
        if !self.is_cancelled() {
            let listener = self.0.event.listen();
            // A request between the initial check and registration must also be observed.
            if !self.is_cancelled() {
                listener.await;
            }
        }
        Cancelled
    }
}

/// A task returned from its cancellation path after performing any cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cancelled;

/// Failure to admit work. No task owner was created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SpawnError {
    /// Cancellation had already been requested at admission.
    Cancelled,
}

impl core::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Cancelled => f.write_str("cancelled before task admission"),
        }
    }
}

impl core::error::Error for SpawnError {}

/// The signals shared by a provider's task owner and its scheduled future.
pub struct TaskControl {
    cooperative: CancellationToken,
    abort: CancellationToken,
}

pin_project! {
    struct MapOutput<F, M> {
        #[pin]
        work: F,
        map: M,
    }
}

impl<F, M, T> Future for MapOutput<F, M>
where
    F: Future,
    M: Fn(F::Output) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.project();
        this.work.poll(cx).map(this.map)
    }
}

impl TaskControl {
    /// Adapts ordinary work to the cancellation-aware spawn signature. Every output
    /// becomes `Ok(value)`, including application errors.
    pub fn returning_value<F: Future>(
        work: F,
    ) -> impl Future<Output = Result<F::Output, Cancelled>> {
        MapOutput { work, map: Ok }
    }

    /// Checks admission and wraps work so an abort wakes it even when it ignores cancellation.
    pub fn prepare<F, T>(
        token: &CancellationToken,
        work: F,
    ) -> Result<(Self, impl Future<Output = TaskOutcome<T>> + use<F, T>), SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>>,
    {
        if token.is_cancelled() {
            // No owner will receive an outcome for rejected work. Preserve the refusal
            // even if destroying the submitted future unwinds on a native adapter.
            crate::discard(work);
            return Err(SpawnError::Cancelled);
        }
        let abort = CancellationToken::new();
        let control = Self {
            cooperative: token.clone(),
            abort: abort.clone(),
        };
        let run = futures_lite::future::or(
            async move {
                abort.cancelled().await;
                TaskOutcome::Cancelled
            },
            // Keep the submitted future alive until the executor destroys its wrapper.
            // Dropping an owned await temporary while poll is unwinding can abort the process.
            MapOutput {
                work,
                map: |result| match result {
                    Ok(value) => TaskOutcome::Completed(value),
                    Err(Cancelled) => TaskOutcome::Cancelled,
                },
            },
        );
        Ok((control, run))
    }
}

/// An owned task. A pending drain borrows the owner and may be interrupted and retried.
/// After one terminal outcome has been consumed, the owner must not be polled again.
pub trait TaskHandle<T>: Future<Output = TaskOutcome<T>> + Unpin {
    /// Provides the signals associated with this task.
    fn control(&self) -> &TaskControl;

    /// Requests cooperative cancellation. Work may acknowledge it or finish normally.
    fn request_cancel(&self) {
        self.control().cooperative.request_cancel();
    }

    /// Wakes the task and forces pending work to stop at a poll boundary.
    /// A result already produced by the executor wins over a later abort.
    fn abort(&self) {
        self.control().abort.request_cancel();
    }

    /// Waits for the terminal outcome and the destruction of the task future.
    fn drain(&mut self) -> &mut Self {
        self
    }
}
