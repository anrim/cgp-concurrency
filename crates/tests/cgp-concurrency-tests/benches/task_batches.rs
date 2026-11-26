extern crate cgp_core as cgp;

#[path = "../tests/support/executors.rs"]
mod executors;
#[path = "support/parent.rs"]
mod parent;

use core::convert::Infallible;
use core::pin::pin;
use std::time::Duration;

use async_channel::{Sender, bounded};
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::{Handler, HandlerComponent};
use cpu_time::ProcessTime;
use futures_lite::future;
use parent::Parent;

#[cfg(feature = "heap-profile")]
#[global_allocator]
static HEAP: mod_alloc::ModAlloc = mod_alloc::ModAlloc::new();

const WIDTH: usize = 8;
macro_rules! eight {
    ($t:ty) => {
        Product![$t, $t, $t, $t, $t, $t, $t, $t]
    };
}

type Jobs = eight!(WaitForGate);
type Inputs = eight!(Input);
type ResultValue = Result<usize, Infallible>;
type Outcomes = eight!(TaskOutcome<ResultValue>);

#[derive(Clone)]
struct Child(CancellationToken);

impl HasErrorType for Child {
    type Error = Infallible;
}

struct Input {
    index: usize,
    registered: Sender<()>,
    alive: Sender<()>,
}

#[cgp_impl(new WaitForGate)]
impl Handler<(), Input> for Child {
    type Output = usize;

    async fn handle(&self, _: PhantomData<()>, input: Input) -> ResultValue {
        let mut gate = pin!(self.0.cancelled());
        assert!(future::poll_once(&mut gate).await.is_none());
        drop(input.registered);
        gate.await;
        drop(input.alive);
        Ok(input.index)
    }
}

impl_send_handler!((Child, (), Input) for WaitForGate);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Finish {
    Join,
    Cancel,
    Drop,
}

struct Batch {
    count: usize,
    gate: CancellationToken,
    registered: Sender<()>,
    alive: Sender<()>,
}

impl Batch {
    fn groups(&self) -> impl ExactSizeIterator<Item = [Input; WIDTH]> {
        (0..self.count / WIDTH).map(|group| {
            std::array::from_fn(|offset| Input {
                index: group * WIDTH + offset,
                registered: self.registered.clone(),
                alive: self.alive.clone(),
            })
        })
    }
}

struct Sample {
    submit: Duration,
    park: Duration,
    finish: Duration,
    #[cfg(feature = "heap-profile")]
    heap: mod_alloc::AllocStats,
}

async fn measure<Owners>(
    count: usize,
    finish: Finish,
    submit: impl FnOnce(&Batch) -> Owners,
    drain: impl AsyncFnOnce(Owners),
) -> Sample {
    #[cfg(feature = "heap-profile")]
    let idle = {
        HEAP.reset();
        HEAP.snapshot().current_bytes
    };
    let (registered, parked) = bounded(1);
    let (alive, reclaimed) = bounded(1);
    let gate = CancellationToken::new();
    let batch = Batch {
        count,
        gate: gate.clone(),
        registered,
        alive,
    };
    let start = ProcessTime::now();
    let owners = submit(&batch);
    let submitted = ProcessTime::now();
    drop(batch);
    assert!(parked.recv().await.is_err());
    assert_eq!(reclaimed.sender_count(), count);

    let pending = ProcessTime::now();
    if finish == Finish::Drop {
        drop(owners);
    } else {
        if finish == Finish::Join {
            gate.request_cancel();
        }
        drain(owners).await;
    }
    assert!(reclaimed.recv().await.is_err());
    let finished = ProcessTime::now();
    drop((gate, parked, reclaimed));
    Sample {
        submit: submitted.duration_since(start),
        park: pending.duration_since(submitted),
        finish: finished.duration_since(pending),
        #[cfg(feature = "heap-profile")]
        heap: {
            let mut stats = HEAP.snapshot();
            assert!(stats.alloc_count >= count as u64);
            stats.peak_bytes = stats.peak_bytes.saturating_sub(idle);
            stats.current_bytes = stats.current_bytes.saturating_sub(idle);
            stats
        },
    }
}

async fn check_groups(groups: Vec<impl Future<Output = Outcomes>>) {
    for (group, join) in groups.into_iter().enumerate() {
        let product![a, b, c, d, e, f, g, h] = join.await;
        for (offset, outcome) in [a, b, c, d, e, f, g, h].into_iter().enumerate() {
            assert_eq!(outcome, TaskOutcome::Completed(Ok(group * WIDTH + offset)));
        }
    }
}

fn inputs([a, b, c, d, e, f, g, h]: [Input; WIDTH]) -> Inputs {
    product![a, b, c, d, e, f, g, h]
}

async fn grouped(
    parent: &impl CanSpawnHandlerTasks<(), Child, Inputs, Outcomes = Outcomes>,
    count: usize,
    finish: Finish,
) -> Sample {
    if finish == Finish::Cancel {
        measure(
            count,
            finish,
            |batch| {
                batch
                    .groups()
                    .map(|group| parent.spawn_cancellable(PhantomData, Child, inputs(group)))
                    .collect::<Vec<_>>()
            },
            async |groups| {
                // Signal every group before waiting on the first one.
                check_groups(groups.into_iter().map(HandlerTasks::cancel).collect()).await;
            },
        )
        .await
    } else {
        measure(
            count,
            finish,
            |batch| {
                batch
                    .groups()
                    .map(|group| {
                        parent.spawn(PhantomData, Child(batch.gate.clone()), inputs(group))
                    })
                    .collect()
            },
            check_groups,
        )
        .await
    }
}

async fn direct<H: TaskHandle<ResultValue>>(
    spawn: impl Fn(Child, Input) -> H,
    count: usize,
    finish: Finish,
) -> Sample {
    measure(
        count,
        finish,
        |batch| {
            batch
                .groups()
                .map(|group| {
                    let cancel = (finish == Finish::Cancel).then(CancellationToken::new);
                    let gate = cancel.as_ref().unwrap_or(&batch.gate);
                    let handles = group.map(|input| spawn(Child(gate.clone()), input));
                    (cancel, handles)
                })
                .collect::<Vec<_>>()
        },
        async |groups| {
            for (token, _) in &groups {
                if let Some(token) = token {
                    token.request_cancel();
                }
            }
            for (group, (_, handles)) in groups.into_iter().enumerate() {
                for (offset, handle) in handles.into_iter().enumerate() {
                    assert_eq!(
                        handle.await,
                        TaskOutcome::Completed(Ok(group * WIDTH + offset))
                    );
                }
            }
        },
    )
    .await
}

fn run_cases(count: usize, finish: Finish, api: &str) -> [(&'static str, Sample); 4] {
    macro_rules! measure_on {
        ($executor:ident, $provider:ident, $spawn:ident) => {{
            let sample = executors::$executor(async |runtime| {
                let parent = Parent {
                    runtime,
                    provider: PhantomData::<$provider<Jobs>>,
                };
                if api == "group" {
                    grouped(&parent, count, finish).await
                } else {
                    direct(
                        |child, input| {
                            parent.runtime.$spawn(async move {
                                WaitForGate::handle(&child, PhantomData, input).await
                            })
                        },
                        count,
                        finish,
                    )
                    .await
                }
            });
            (stringify!($executor), sample)
        }};
    }

    [
        measure_on!(tokio_local, SpawnLocalTasks, spawn_local_task),
        measure_on!(tokio_worker, SpawnWorkerTasks, spawn_task),
        measure_on!(smol_local, SpawnLocalTasks, spawn_local_task),
        measure_on!(smol_worker, SpawnWorkerTasks, spawn_task),
    ]
}

fn main() {
    print!(
        "sample,executor,api,children,finish,submit_cpu_ns_per_child,park_cpu_ns_per_child,finish_cpu_ns_per_child"
    );
    #[cfg(feature = "heap-profile")]
    print!(",allocs_per_child,peak_bytes_per_child,live_bytes_after");
    println!();
    for sample in 1..=5 {
        for count in [1_000, 10_000] {
            let per_child = |d: Duration| d.as_nanos() as f64 / count as f64;
            for finish in [Finish::Join, Finish::Cancel, Finish::Drop] {
                for api in ["group", "direct"] {
                    for (executor, result) in run_cases(count, finish, api) {
                        print!(
                            "{sample},{executor},{api},{count},{finish:?},{:.1},{:.1},{:.1}",
                            per_child(result.submit),
                            per_child(result.park),
                            per_child(result.finish)
                        );
                        #[cfg(feature = "heap-profile")]
                        print!(
                            ",{:.3},{:.1},{}",
                            result.heap.alloc_count as f64 / count as f64,
                            result.heap.peak_bytes as f64 / count as f64,
                            result.heap.current_bytes
                        );
                        println!();
                    }
                }
            }
        }
    }
}
