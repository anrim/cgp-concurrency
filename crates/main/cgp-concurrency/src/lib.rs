#![no_std]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub use cgp_concurrency_runtime as runtime;
#[cfg(feature = "smol")]
pub use cgp_concurrency_smol as smol;
#[cfg(feature = "tokio")]
pub use cgp_concurrency_tokio as tokio;

pub mod prelude {
    pub use crate::runtime::{
        CanSpawnLocalTask, CanSpawnTask, HasRuntime, HasRuntimeType, LocalTaskSpawner,
        LocalTaskSpawnerComponent, RuntimeGetter, RuntimeGetterComponent, RuntimeOf,
        RuntimeTypeProvider, RuntimeTypeProviderComponent, TaskOutcome, TaskSpawner,
        TaskSpawnerComponent,
    };
}
