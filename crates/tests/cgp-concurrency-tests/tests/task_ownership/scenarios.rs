use std::cell::Cell;
use std::thread;

use async_channel::{Sender, bounded};
use cgp_concurrency::prelude::*;
use futures_lite::future;

use crate::app::App;

struct DropWitness(Sender<()>);

impl Drop for DropWitness {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

pub async fn cross_thread(runtime: impl CanSpawnTask) {
    let app = App { runtime };
    for complete in [true, false] {
        let (start, started) = bounded(1);
        let (release, gate) = bounded(1);
        let (dropped, destroyed) = bounded(1);
        let mut owner = app.spawn_task(async move {
            let _witness = DropWitness(dropped);
            let value = Cell::new(42_u32);
            start.try_send(thread::current().id()).unwrap();
            gate.recv().await.unwrap();
            value
        });
        assert_ne!(thread::current().id(), started.recv().await.unwrap());
        assert!(future::poll_once(&mut owner).await.is_none());
        if complete {
            release.try_send(()).unwrap();
            assert_eq!((&mut owner).await, TaskOutcome::Completed(Cell::new(42)));
        } else {
            drop(owner);
        }
        destroyed.recv().await.unwrap();
    }
}
