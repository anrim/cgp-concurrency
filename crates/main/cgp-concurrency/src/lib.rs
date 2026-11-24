#![no_std]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

extern crate cgp_core as cgp;

mod join;

pub use cgp_concurrency_runtime as runtime;
#[cfg(feature = "smol")]
pub use cgp_concurrency_smol as smol;
#[cfg(feature = "tokio")]
pub use cgp_concurrency_tokio as tokio;
pub use join::JoinHandlers;

pub mod prelude {
    pub use crate::JoinHandlers;
    pub use crate::runtime::{
        CanSpawnLocalTask, CanSpawnTask, CancellationToken, Cancelled, HasRuntime, HasRuntimeType,
        LocalTaskSpawner, LocalTaskSpawnerComponent, RuntimeGetter, RuntimeGetterComponent,
        RuntimeOf, RuntimeTypeProvider, RuntimeTypeProviderComponent, SpawnError, TaskHandle,
        TaskOutcome, TaskSpawner, TaskSpawnerComponent,
    };
}
