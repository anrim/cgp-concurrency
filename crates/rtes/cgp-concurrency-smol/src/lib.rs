#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub use cgp_concurrency_runtime as runtime;
use runtime::{CanSpawnLocalTask, TaskOutcome};
use smol::{LocalExecutor, Task};

/// Submits owned tasks to the caller's Smol local executor.
pub struct SmolLocalSpawner<'a, 'tasks>(pub &'a LocalExecutor<'tasks>);

impl CanSpawnLocalTask for SmolLocalSpawner<'_, '_> {
    type LocalJoinHandle<T: 'static> = SmolLocalTask<T>;

    fn spawn_local_task<F, T>(&self, future: F) -> SmolLocalTask<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        SmolLocalTask(self.0.spawn(future))
    }
}

/// Owns a local Smol task. Native task drop requests cancellation.
pub struct SmolLocalTask<T>(Task<T>);

impl<T> Future for SmolLocalTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(TaskOutcome::Completed)
    }
}
