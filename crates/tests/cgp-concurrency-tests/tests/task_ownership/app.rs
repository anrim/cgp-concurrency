use core::future::Future;

use cgp::component::UseFields;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;

// Exercise the local spawn component through a downstream application.
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
