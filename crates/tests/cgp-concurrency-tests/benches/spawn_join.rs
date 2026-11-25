extern crate cgp_core as cgp;

#[path = "../tests/support/executors.rs"]
mod executors;
#[path = "support/parent.rs"]
mod parent;

use core::convert::Infallible;
use core::fmt::Debug;
use std::hint::black_box;
use std::time::{Duration, Instant};

use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, ReturnInput};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parent::Parent;

#[derive(Clone)]
struct Child;

impl HasErrorType for Child {
    type Error = Infallible;
}

impl SendHandler<Child, (), u64> for ReturnInput {
    fn handle_send(
        child: &Child,
        code: PhantomData<()>,
        input: u64,
    ) -> impl Future<Output = Result<u64, Infallible>> + Send {
        Self::handle(child, code, input)
    }
}

async fn time_groups<F, O>(iterations: u64, spawn_and_join: impl Fn() -> F, expected: O) -> Duration
where
    F: Future<Output = O>,
    O: Debug + PartialEq,
{
    // Bring the executor up and check that every child returned before starting the clock.
    assert_eq!(spawn_and_join().await, expected);
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(spawn_and_join().await);
    }
    start.elapsed()
}

fn spawn_join(c: &mut Criterion) {
    type One = Product![ReturnInput];
    type Eight = Product![
        ReturnInput,
        ReturnInput,
        ReturnInput,
        ReturnInput,
        ReturnInput,
        ReturnInput,
        ReturnInput,
        ReturnInput
    ];
    let mut group = c.benchmark_group("spawn_join");

    macro_rules! bench_case {
        ($executor:ident, $provider:ty, $($value:literal),+) => {{
            let width = [$($value),+].len() as u64;
            group.throughput(Throughput::Elements(width));
            group.bench_function(BenchmarkId::new(stringify!($executor), width), |b| {
                b.iter_custom(|iterations| {
                    executors::$executor(async |runtime| {
                        let parent = Parent { runtime, provider: PhantomData::<$provider> };
                        let spawn_and_join = || {
                            let inputs = black_box(product![$($value),+]);
                            parent.spawn(PhantomData::<()>, Child, inputs).join()
                        };
                        let expected = product![$(TaskOutcome::Completed(Ok::<u64, Infallible>($value))),+];
                        time_groups(iterations, spawn_and_join, expected).await
                    })
                });
            });
        }};
    }

    bench_case!(tokio_local, SpawnLocalTasks<One>, 1);
    bench_case!(tokio_local, SpawnLocalTasks<Eight>, 1, 2, 3, 4, 5, 6, 7, 8);
    bench_case!(tokio_worker, SpawnWorkerTasks<One>, 1);
    bench_case!(
        tokio_worker,
        SpawnWorkerTasks<Eight>,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
    );
    bench_case!(smol_local, SpawnLocalTasks<One>, 1);
    bench_case!(smol_local, SpawnLocalTasks<Eight>, 1, 2, 3, 4, 5, 6, 7, 8);
    bench_case!(smol_worker, SpawnWorkerTasks<One>, 1);
    bench_case!(smol_worker, SpawnWorkerTasks<Eight>, 1, 2, 3, 4, 5, 6, 7, 8);
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50);
    targets = spawn_join
}
criterion_main!(benches);
