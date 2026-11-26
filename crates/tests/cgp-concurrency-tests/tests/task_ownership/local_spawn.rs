use std::rc::Rc;

use async_channel::{TryRecvError, bounded};
use cgp_concurrency::prelude::{CanSpawnLocalTask, TaskHandle, TaskOutcome};
use futures_lite::future;

use crate::app::App;

async fn recovers_rc_after_suspension(runtime: &impl CanSpawnLocalTask) {
    let expected = Rc::<str>::from("local output");
    let value = Rc::clone(&expected);
    let (start, started) = bounded(1);
    let (release, gate) = bounded(1);
    let mut owner = runtime.spawn_local_task(async move {
        start.try_send(()).unwrap();
        gate.recv().await.unwrap();
        value
    });

    started.recv().await.unwrap();
    assert!(future::poll_once(&mut owner).await.is_none());
    release.try_send(()).unwrap();
    let outcome = (&mut owner).await;
    assert!(matches!(outcome, TaskOutcome::Completed(found) if Rc::ptr_eq(&found, &expected)));
}

async fn reclaims_pending_task(runtime: &impl CanSpawnLocalTask) {
    let (start, started) = bounded(1);
    let (release, gate) = bounded::<()>(1);
    let owner = runtime.spawn_local_task(async move {
        start.try_send(()).unwrap();
        gate.recv().await.unwrap();
    });
    started.recv().await.unwrap();
    drop(owner);
    // Only destroying the pending task can drop the sole receiver of this unopened gate.
    release.closed().await;
}

async fn abort_before_first_poll(runtime: &impl CanSpawnLocalTask) {
    let (entered, observed) = bounded(1);
    let owner = runtime.spawn_local_task(async move { entered.try_send(()).unwrap() });
    // The local executor cannot poll the child before we yield.
    owner.abort();
    assert_eq!(owner.await, TaskOutcome::Cancelled);
    assert_eq!(observed.try_recv(), Err(TryRecvError::Closed));
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_local_spawn() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let app = App {
        runtime: cgp_concurrency::tokio::TokioLocalSpawner(&local),
    };
    local.block_on(&runtime, async {
        recovers_rc_after_suspension(&app).await;
        reclaims_pending_task(&app).await;
        abort_before_first_poll(&app).await;
    });
}

#[cfg(feature = "smol")]
#[test]
fn smol_local_spawn() {
    let local = smol::LocalExecutor::new();
    let app = App {
        runtime: cgp_concurrency::smol::SmolLocalSpawner(&local),
    };
    smol::block_on(local.run(async {
        recovers_rc_after_suspension(&app).await;
        reclaims_pending_task(&app).await;
        abort_before_first_poll(&app).await;
    }));
}
