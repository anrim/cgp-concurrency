#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub use cgp_concurrency_runtime as runtime;
use runtime::{
    CanSpawnLocalTask, CanSpawnTask, CancellationToken, Cancelled, SpawnError, TaskCompletion,
    TaskControl, TaskHandle, TaskOutcome, UnclaimedOutput,
};
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
        self.try_spawn_local_task(
            &CancellationToken::new(),
            TaskControl::returning_value(future),
        )
        .expect("fresh token permits admission")
    }

    fn try_spawn_local_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<TokioLocalTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        let (control, work) = TaskControl::prepare(token, future)?;
        let (completion, work) = TaskCompletion::track(work);
        Ok(TokioTask {
            control,
            completion,
            join: self.0.spawn_local(work),
        })
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
        self.try_spawn_task(
            &CancellationToken::new(),
            TaskControl::returning_value(future),
        )
        .expect("fresh token permits admission")
    }

    fn try_spawn_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<TokioTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static,
    {
        let (control, work) = TaskControl::prepare(token, future)?;
        let (completion, work) = TaskCompletion::track(work);
        Ok(TokioTask {
            control,
            completion,
            join: self.0.spawn(work),
        })
    }
}

/// Owns a Tokio task and requests abortion on drop.
pub struct TokioTask<T> {
    control: TaskControl,
    completion: TaskCompletion,
    join: JoinHandle<UnclaimedOutput<T>>,
}

/// The owner returned by [`TokioLocalSpawner`].
pub type TokioLocalTask<T> = TokioTask<T>;

impl<T> Drop for TokioTask<T> {
    fn drop(&mut self) {
        // Tokio's native handle detaches on drop.
        self.join.abort();
    }
}

impl<T> TaskHandle<T> for TokioTask<T> {
    fn control(&self) -> &TaskControl {
        &self.control
    }
}

impl<T> Future for TokioTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.join)
            .poll(cx)
            .map(|result| match result {
                Ok(output) => self.completion.finish(Some(output)),
                Err(error) if error.is_panic() => TaskOutcome::from_panic(error.into_panic()),
                Err(_) => self.completion.finish(None),
            })
    }
}
