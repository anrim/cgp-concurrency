// Task ownership design adapted from hyperflow-smol (MIT OR Apache-2.0).
use std::thread;

use cgp_concurrency::smol::SmolSpawner;
use smol::Executor;

use crate::scenarios;

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
