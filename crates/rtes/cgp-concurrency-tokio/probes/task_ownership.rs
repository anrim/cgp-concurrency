use cgp_concurrency::tokio::TokioSpawner;
use futures_lite::future;
use tokio::runtime::Builder;

use crate::scenarios;

#[test]
fn multithread() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    future::block_on(scenarios::cross_thread(TokioSpawner(runtime.handle())));
}
