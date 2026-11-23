// Task ownership design adapted from hyperflow-smol (MIT OR Apache-2.0).
use std::thread;

use cgp_concurrency::prelude::*;
use cgp_concurrency::smol::{SmolLocalSpawner, SmolSpawner};
use smol::{Executor, LocalExecutor};

use crate::scenarios;

#[test]
fn local_executor() {
    let executor = LocalExecutor::new();
    smol::block_on(executor.run(scenarios::local(SmolLocalSpawner(&executor))));
}

#[test]
fn multithread() {
    let executor = Executor::new();
    thread::scope(|scope| {
        let (shutdown, stopped) = smol::channel::bounded::<()>(1);
        for _ in 0..2 {
            let stopped = stopped.clone();
            let executor = &executor;
            scope.spawn(move || smol::block_on(executor.run(stopped.recv())));
        }
        smol::block_on(scenarios::cross_thread(SmolSpawner(&executor)));
        drop(shutdown);
    });
}

#[test]
fn executor_loss() {
    let local = LocalExecutor::new();
    let executor = Executor::new();
    let local_spawner = SmolLocalSpawner(&local);
    let spawner = SmolSpawner(&executor);
    let (start, started) = async_channel::bounded(4);
    let mut a = local_spawner.spawn_local_task(scenarios::signal_then_wait(start.clone()));
    let _local_peer = local_spawner.spawn_local_task(scenarios::signal_then_wait(start.clone()));
    let mut b = spawner.spawn_task(scenarios::signal_then_wait(start.clone()));
    let _peer = spawner.spawn_task(scenarios::signal_then_wait(start));
    scenarios::assert_passive_drain(&mut a, &started);
    scenarios::assert_passive_drain(&mut b, &started);
    while local.try_tick() {}
    while executor.try_tick() {}
    assert_eq!(started.len(), 4);
    drop(local);
    drop(executor);
    assert_eq!(smol::block_on(a.drain()), TaskOutcome::RuntimeFailure);
    assert_eq!(smol::block_on(b.drain()), TaskOutcome::RuntimeFailure);
}

#[test]
fn shutdown_reports_cleanup_panic_before_first_poll() {
    let executor = LocalExecutor::new();
    let guard = scenarios::PanicOnDrop;
    let mut owner = SmolLocalSpawner(&executor).spawn_local_task(async move {
        let _guard = guard;
        core::future::pending::<()>().await;
    });
    drop(executor);
    assert_eq!(
        smol::block_on(owner.drain()),
        TaskOutcome::Panicked(Some("cleanup panic".into()))
    );
}
