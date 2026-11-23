#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use async_executor::FallibleTask;
pub use cgp_concurrency_runtime as runtime;
use runtime::{
    CanSpawnLocalTask, CanSpawnTask, CancellationToken, Cancelled, SpawnError, TaskCompletion,
    TaskControl, TaskHandle, TaskOutcome, UnclaimedOutput,
};
use smol::{Executor, LocalExecutor};

/// Submits owned tasks to the caller's Smol local executor.
pub struct SmolLocalSpawner<'a, 'tasks>(pub &'a LocalExecutor<'tasks>);

impl CanSpawnLocalTask for SmolLocalSpawner<'_, '_> {
    type LocalJoinHandle<T: 'static> = SmolLocalTask<T>;

    fn spawn_local_task<F, T>(&self, future: F) -> SmolLocalTask<T>
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
    ) -> Result<SmolLocalTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        let (control, work) = TaskControl::prepare(token, future)?;
        let (completion, work) = TaskCompletion::track(work);
        Ok(SmolTask {
            control,
            completion,
            join: self.0.spawn(work).fallible(),
        })
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
    ) -> Result<SmolTask<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static,
    {
        let (control, work) = TaskControl::prepare(token, future)?;
        let (completion, work) = TaskCompletion::track(work);
        Ok(SmolTask {
            control,
            completion,
            join: self.0.spawn(work).fallible(),
        })
    }
}

/// Owns a Smol task. Native task drop requests cancellation.
pub struct SmolTask<T> {
    control: TaskControl,
    join: FallibleTask<UnclaimedOutput<T>>,
    completion: TaskCompletion,
}

/// The owner returned by [`SmolLocalSpawner`].
pub type SmolLocalTask<T> = SmolTask<T>;

impl<T> TaskHandle<T> for SmolTask<T> {
    fn control(&self) -> &TaskControl {
        &self.control
    }
}

impl<T> Future for SmolTask<T> {
    type Output = TaskOutcome<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.join)
            .poll(cx)
            .map(|result| self.completion.finish(result))
    }
}
