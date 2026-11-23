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
