// Local spawn signature adapted from contextgeneric/cgp (Apache-2.0).
#[cfg(feature = "std")]
use alloc::boxed::Box;
use alloc::string::String;
#[cfg(feature = "std")]
use core::any::Any;
use core::future::Future;

use cgp::prelude::*;

use crate::{CancellationToken, Cancelled, SpawnError, TaskHandle};

/// The terminal outcome observed by the owner of a spawned task.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskOutcome<T> {
    /// The task returned its output, including any application-level error.
    Completed(T),
    /// Work acknowledged cancellation or its owner aborted it.
    Cancelled,
    /// The task unwound. String panic payloads retain their message.
    Panicked(Option<String>),
    /// The executor discarded accepted work before it produced an outcome.
    RuntimeFailure,
}

#[cfg(feature = "std")]
type PanicPayload = Box<dyn Any + Send>;
#[cfg(not(feature = "std"))]
type PanicPayload = core::convert::Infallible;

/// A task operation's result, held until its separately owned resources have been released.
pub struct TaskResult<T>(Result<T, PanicPayload>);

impl<T> TaskResult<T> {
    /// Runs an operation, retaining its panic when unwinding is supported.
    pub fn capture(run: impl FnOnce() -> T) -> Self {
        #[cfg(feature = "std")]
        return Self(std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)));
        #[cfg(not(feature = "std"))]
        Self(Ok(run()))
    }

    fn after_cleanup(self, cleanup: impl FnOnce()) -> Self {
        match (self.0, TaskResult::capture(cleanup).0) {
            (Ok(value), Ok(())) => Self(Ok(value)),
            (Err(panic), secondary) => {
                discard(secondary);
                Self(Err(panic))
            }
            (Ok(value), Err(panic)) => {
                discard(value);
                Self(Err(panic))
            }
        }
    }

    /// Releases the remaining task state, then returns its value or resumes the first panic.
    pub fn finish(self, cleanup: impl FnOnce()) -> T {
        match self.after_cleanup(cleanup).0 {
            Ok(value) => value,
            #[cfg(feature = "std")]
            Err(panic) => std::panic::resume_unwind(panic),
            #[cfg(not(feature = "std"))]
            Err(never) => match never {},
        }
    }
}

impl<F: Future> TaskResult<F> {
    /// Drives a captured future. Construction, polling, and destruction unwind independently.
    pub async fn run(self) -> TaskResult<F::Output> {
        use core::future::poll_fn;
        use core::pin::pin;
        use core::task::Poll;

        let future = match self.0 {
            Ok(work) => work,
            Err(panic) => return TaskResult(Err(panic)),
        };
        let mut work = pin!(Some(future));
        let outcome = poll_fn(|cx| {
            let future = work
                .as_mut()
                .as_pin_mut()
                .expect("work is present until cleanup");
            match TaskResult::capture(|| future.poll(cx)).0 {
                Ok(poll) => poll.map(|value| TaskResult(Ok(value))),
                Err(panic) => Poll::Ready(TaskResult(Err(panic))),
            }
        })
        .await;
        outcome.after_cleanup(|| work.set(None))
    }
}

/// Drives a nested future and releases it before returning its output. With unwinding
/// enabled, a poll panic takes precedence over a panic during destruction.
pub async fn await_with_cleanup<F: Future>(work: F) -> F::Output {
    TaskResult(Ok(work)).run().await.finish(|| {})
}

impl<T> TaskOutcome<T> {
    /// Releases an unwanted outcome during cleanup. With `std` and unwinding enabled,
    /// contains a panic from destroying its value so other tasks can still be drained.
    pub fn discard(self) {
        discard(self);
    }
}

/// Drops one unused value during cleanup. With `std` and unwinding enabled, a panic
/// from its destructor is contained. Discard independent values separately.
pub fn discard<T>(value: T) {
    #[cfg(feature = "std")]
    {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            panic_message(payload);
        }
    }
    #[cfg(not(feature = "std"))]
    drop(value);
}

#[cfg(feature = "std")]
impl<T> TaskOutcome<T> {
    /// Converts an observed unwind into a terminal outcome. Both native adapters enable `std`.
    pub fn from_panic(payload: Box<dyn Any + Send>) -> Self {
        Self::Panicked(panic_message(payload))
    }
}

/// Extracts a string message and contains unwinds from an opaque panic payload's destructor.
#[cfg(feature = "std")]
pub fn panic_message(payload: Box<dyn Any + Send>) -> Option<String> {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let payload = match payload.downcast::<String>() {
        Ok(message) => return Some(*message),
        Err(payload) => payload,
    };
    if let Some(message) = payload.downcast_ref::<&str>() {
        return Some(String::from(*message));
    }
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // A destructor can panic with another destructor-bearing payload, indefinitely.
        // String payloads are safe to drop. Retain only an opaque secondary payload.
        if !secondary.is::<String>() && !secondary.is::<&str>() {
            core::mem::forget(secondary);
        }
    }
    None
}

/// Submits owned futures to a local executor. The future, output, and handle may be non-`Send`.
#[cgp_component(LocalTaskSpawner)]
pub trait CanSpawnLocalTask {
    /// Owns the submitted task and yields its terminal outcome once.
    ///
    /// Wait through a mutable borrow to retain ownership if the wait is interrupted.
    /// Dropping the owner while work is pending requests abortion. Reclamation may
    /// require further executor progress.
    type LocalJoinHandle<T: 'static>: TaskHandle<T> + 'static;

    /// Schedules a task that can progress whenever the local executor is driven,
    /// including before the caller polls the returned handle.
    fn spawn_local_task<F, T>(&self, future: F) -> Self::LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static;

    /// Attempts admission with a cooperative token. Return `Err(Cancelled)` from work
    /// to acknowledge a request. Wrap application results in `Ok` to preserve them as output.
    fn try_spawn_local_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<Self::LocalJoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static;
}

/// Submits owned work to an executor that may move the task between threads.
#[cgp_component(TaskSpawner)]
pub trait CanSpawnTask {
    /// Owns the task and can be moved to another thread to await its outcome.
    /// Dropping a pending owner requests abortion. The executor must run to reclaim the task.
    type JoinHandle<T: Send + 'static>: TaskHandle<T> + Send + 'static;

    /// Schedules the future immediately. Await a mutable borrow to retain ownership across waits.
    fn spawn_task<F, T>(&self, future: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Attempts admission with a cooperative token. Return `Err(Cancelled)` from work
    /// to acknowledge a request. Wrap application results in `Ok` to preserve them as output.
    fn try_spawn_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<Self::JoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static;
}
