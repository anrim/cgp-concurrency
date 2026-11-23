use core::future::Future;

use cgp::component::UseFields;
use cgp::error::ErrorTypeProviderComponent;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;

// The two spawn components are wired independently so each adapter needs only its own bounds.
#[derive(HasField)]
pub(super) struct App<R> {
    pub runtime: R,
}

#[cgp_impl(new SpawnOnRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl LocalTaskSpawner
where
    Self: HasRuntime,
    Runtime: CanSpawnLocalTask,
{
    type LocalJoinHandle<T: 'static> = <Runtime as CanSpawnLocalTask>::LocalJoinHandle<T>;

    fn spawn_local_task<F, T>(&self, future: F) -> Self::LocalJoinHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        self.runtime().spawn_local_task(future)
    }

    fn try_spawn_local_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<Self::LocalJoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + 'static,
        T: 'static,
    {
        self.runtime().try_spawn_local_task(token, future)
    }
}

#[cgp_impl(SpawnOnRuntime)]
#[use_type(HasRuntimeType.Runtime)]
impl TaskSpawner
where
    Self: HasRuntime,
    Runtime: CanSpawnTask,
{
    type JoinHandle<T: Send + 'static> = <Runtime as CanSpawnTask>::JoinHandle<T>;

    fn spawn_task<F, T>(&self, future: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime().spawn_task(future)
    }

    fn try_spawn_task<F, T>(
        &self,
        token: &CancellationToken,
        future: F,
    ) -> Result<Self::JoinHandle<T>, SpawnError>
    where
        F: Future<Output = Result<T, Cancelled>> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime().try_spawn_task(token, future)
    }
}

delegate_and_check_components! {
    <R> App<R> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
        ErrorTypeProviderComponent: UseType<AppError>,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckLocalSpawn)]
    <R: CanSpawnLocalTask> App<R> {
        LocalTaskSpawnerComponent: SpawnOnRuntime,
    }
}

delegate_and_check_components! {
    #[check_trait(CheckWorkerSpawn)]
    <R: CanSpawnTask> App<R> {
        TaskSpawnerComponent: SpawnOnRuntime,
    }
}

#[derive(Debug, PartialEq)]
pub(super) enum AppError {
    Refused(SpawnError),
    InvalidRecord(u32),
}

impl<R> CanRaiseError<SpawnError> for App<R> {
    fn raise_error(error: SpawnError) -> AppError {
        AppError::Refused(error)
    }
}

// A downstream caller translates admission failures and keeps the application's own error in T.
async fn run_job<C>(
    app: &C,
    token: &CancellationToken,
    result: Result<(), C::Error>,
) -> Result<TaskOutcome<Result<(), C::Error>>, C::Error>
where
    C: CanSpawnTask + CanRaiseError<SpawnError>,
    C::Error: Send + 'static,
{
    let mut owner = app
        .try_spawn_task(token, async move { Ok(result) })
        .map_err(C::raise_error)?;
    Ok(owner.drain().await)
}

pub(super) async fn check_errors<R: CanSpawnTask>(app: &App<R>) {
    let token = CancellationToken::new();
    assert_eq!(
        run_job(app, &token, Err(AppError::InvalidRecord(17))).await,
        Ok(TaskOutcome::Completed(Err(AppError::InvalidRecord(17))))
    );
    token.request_cancel();
    assert_eq!(
        run_job(app, &token, Ok(())).await,
        Err(AppError::Refused(SpawnError::Cancelled))
    );
}
