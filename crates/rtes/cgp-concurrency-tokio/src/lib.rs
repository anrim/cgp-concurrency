#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub use cgp_concurrency_runtime as runtime;
use runtime::{CanSpawnLocalTask, TaskOutcome};
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
        TokioLocalTask(self.0.spawn_local(future))
    }
}

/// Owns a local Tokio task and requests abortion on drop.
pub struct TokioLocalTask<T>(JoinHandle<T>);

impl<T> Drop for TokioLocalTask<T> {
    fn drop(&mut self) {
        // Tokio's native handle detaches on drop.
        self.0.abort();
    }
}

impl<T> Future for TokioLocalTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| match result {
            Ok(value) => TaskOutcome::Completed(value),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("local task terminated before returning: {error}"),
        })
    }
}
