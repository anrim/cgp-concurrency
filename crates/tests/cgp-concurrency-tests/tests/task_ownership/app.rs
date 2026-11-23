use core::future::Future;

use cgp::component::UseFields;
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
}

delegate_and_check_components! {
    <R> App<R> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
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
