use cgp_concurrency::prelude::*;
use cgp_concurrency::tokio::{TokioLocalSpawner, TokioSpawner};
use futures_lite::future;
use tokio::runtime::Builder;
use tokio::task::LocalSet;

use crate::scenarios;

#[test]
fn local_set() {
    let runtime = Builder::new_current_thread().build().unwrap();
    let local = LocalSet::new();
    local.block_on(&runtime, scenarios::local(TokioLocalSpawner(&local)));
}

#[test]
fn multithread() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    future::block_on(scenarios::cross_thread(TokioSpawner(runtime.handle())));
}

#[test]
fn executor_loss() {
    let runtime = Builder::new_current_thread().build().unwrap();
    let local = LocalSet::new();
    let local_spawner = TokioLocalSpawner(&local);
    let spawner = TokioSpawner(runtime.handle());
    let (start, started) = async_channel::bounded(4);
    let mut a = local_spawner.spawn_local_task(scenarios::signal_then_wait(start.clone()));
    let _local_peer = local_spawner.spawn_local_task(scenarios::signal_then_wait(start.clone()));
    let mut b = spawner.spawn_task(scenarios::signal_then_wait(start.clone()));
    let _peer = spawner.spawn_task(scenarios::signal_then_wait(start));
    scenarios::assert_passive_drain(&mut a, &started);
    scenarios::assert_passive_drain(&mut b, &started);
    local.block_on(&runtime, async {
        for _ in 0..4 {
            started.recv().await.unwrap();
        }
    });
    drop(local);
    drop(runtime);
    assert_eq!(future::block_on(a.drain()), TaskOutcome::RuntimeFailure);
    assert_eq!(future::block_on(b.drain()), TaskOutcome::RuntimeFailure);
}
