#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub use cgp_concurrency_runtime as runtime;
use runtime::{CanSpawnLocalTask, CanSpawnTask, TaskOutcome};
use smol::{Executor, LocalExecutor, Task};

/// Submits owned tasks to the caller's Smol local executor.
pub struct SmolLocalSpawner<'a, 'tasks>(pub &'a LocalExecutor<'tasks>);

impl CanSpawnLocalTask for SmolLocalSpawner<'_, '_> {
    type LocalJoinHandle<T: 'static> = SmolLocalTask<T>;

    fn spawn_local_task<F, T>(&self, future: F) -> SmolLocalTask<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        SmolTask(self.0.spawn(future))
    }
}

/// Submits owned `Send` tasks to the caller's Smol executor.
pub struct SmolSpawner<'a, 'tasks>(pub &'a Executor<'tasks>);

impl CanSpawnTask for SmolSpawner<'_, '_> {
    type JoinHandle<T: Send + 'static> = SmolTask<T>;

    fn spawn_task<F, T>(&self, future: F) -> SmolTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        SmolTask(self.0.spawn(future))
    }
}

/// Owns a Smol task. Native task drop requests cancellation.
pub struct SmolTask<T>(Task<T>);

/// The owner returned by [`SmolLocalSpawner`].
pub type SmolLocalTask<T> = SmolTask<T>;

impl<T> Future for SmolTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(TaskOutcome::Completed)
    }
}
