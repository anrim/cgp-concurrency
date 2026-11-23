// Local spawn signature adapted from contextgeneric/cgp (Apache-2.0).
use core::future::Future;

use cgp::prelude::*;

/// The terminal outcome observed by the owner of a spawned task.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskOutcome<T> {
    /// The task returned its output, including any application-level error.
    Completed(T),
}

/// Submits owned futures to a local executor. The future, output, and handle may be non-`Send`.
#[cgp_component(LocalTaskSpawner)]
pub trait CanSpawnLocalTask {
    /// Owns the submitted task and yields its terminal outcome once.
    ///
    /// Wait through a mutable borrow to retain ownership if the wait is interrupted.
    /// Dropping the owner while work is pending requests abortion. Reclamation may
    /// require further executor progress.
    type LocalJoinHandle<T: 'static>: Future<Output = TaskOutcome<T>> + Unpin + 'static;

    /// Schedules a task that can progress whenever the local executor is driven,
    /// including before the caller polls the returned handle.
    fn spawn_local_task<F, T>(&self, future: F) -> Self::LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static;
}
