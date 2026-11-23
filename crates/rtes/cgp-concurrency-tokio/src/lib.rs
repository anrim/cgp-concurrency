#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub use cgp_concurrency_runtime as runtime;
use runtime::{CanSpawnLocalTask, CanSpawnTask, TaskOutcome};
use tokio::runtime::Handle;
use tokio::task::{JoinHandle, LocalSet};

/// Submits owned tasks to the caller's Tokio local set.
pub struct TokioLocalSpawner<'a>(pub &'a LocalSet);

impl CanSpawnLocalTask for TokioLocalSpawner<'_> {
    type LocalJoinHandle<T: 'static> = TokioLocalTask<T>;

    fn spawn_local_task<F, T>(&self, future: F) -> TokioLocalTask<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        TokioTask(self.0.spawn_local(future))
    }
}

/// Submits owned `Send` tasks to the Tokio runtime selected by the caller.
pub struct TokioSpawner<'a>(pub &'a Handle);

impl CanSpawnTask for TokioSpawner<'_> {
    type JoinHandle<T: Send + 'static> = TokioTask<T>;

    fn spawn_task<F, T>(&self, future: F) -> TokioTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioTask(self.0.spawn(future))
    }
}

/// Owns a Tokio task and requests abortion on drop.
pub struct TokioTask<T>(JoinHandle<T>);

/// The owner returned by [`TokioLocalSpawner`].
pub type TokioLocalTask<T> = TokioTask<T>;

impl<T> Drop for TokioTask<T> {
    fn drop(&mut self) {
        // Tokio's native handle detaches on drop.
        self.0.abort();
    }
}

impl<T> Future for TokioTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| match result {
            Ok(value) => TaskOutcome::Completed(value),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("task terminated before returning: {error}"),
        })
    }
}
