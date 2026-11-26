#![forbid(unsafe_code)]
#![cfg(any(feature = "tokio", feature = "smol"))]

extern crate cgp_core as cgp;

mod app;
#[path = "../support/join_notification.rs"]
mod join_notification;
mod local_spawn;
mod scenarios;

// Each provider runs the same ownership scenarios against its native executor.
#[cfg(feature = "smol")]
#[path = "../../../../rtes/cgp-concurrency-smol/probes/task_ownership.rs"]
mod smol_provider;
#[cfg(feature = "tokio")]
#[path = "../../../../rtes/cgp-concurrency-tokio/probes/task_ownership.rs"]
mod tokio_provider;
