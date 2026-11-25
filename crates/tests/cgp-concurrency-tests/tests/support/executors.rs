#[cfg(feature = "smol")]
use cgp_concurrency::smol::{SmolLocalSpawner, SmolSpawner};
#[cfg(feature = "tokio")]
use cgp_concurrency::tokio::{TokioLocalSpawner, TokioSpawner};

#[cfg(feature = "tokio")]
pub(super) fn tokio_local<T>(run: impl AsyncFnOnce(TokioLocalSpawner<'_>) -> T) -> T {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let tasks = tokio::task::LocalSet::new();
    tasks.block_on(&runtime, run(TokioLocalSpawner(&tasks)))
}

#[cfg(feature = "tokio")]
pub(super) fn tokio_worker<T>(run: impl AsyncFnOnce(TokioSpawner<'_>) -> T) -> T {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    futures_lite::future::block_on(run(TokioSpawner(runtime.handle())))
}

#[cfg(feature = "smol")]
pub(super) fn smol_local<T>(run: impl AsyncFnOnce(SmolLocalSpawner<'_, '_>) -> T) -> T {
    let executor = smol::LocalExecutor::new();
    smol::block_on(executor.run(run(SmolLocalSpawner(&executor))))
}

#[cfg(feature = "smol")]
pub(super) fn smol_worker<T>(run: impl AsyncFnOnce(SmolSpawner<'_, '_>) -> T) -> T {
    let executor = smol::Executor::new();
    std::thread::scope(|scope| {
        let (shutdown, stopped) = async_channel::bounded::<()>(1);
        let executor = &executor;
        scope.spawn(move || smol::block_on(executor.run(stopped.recv())));
        let result = smol::block_on(run(SmolSpawner(executor)));
        drop(shutdown);
        result
    })
}
