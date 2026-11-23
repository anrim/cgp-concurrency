#![no_std]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

extern crate cgp_core as cgp;

mod task;

pub use cgp_runtime::{
    HasRuntime, HasRuntimeType, RuntimeGetter, RuntimeGetterComponent, RuntimeOf,
    RuntimeTypeProvider, RuntimeTypeProviderComponent,
};
pub use task::{CanSpawnLocalTask, LocalTaskSpawner, LocalTaskSpawnerComponent, TaskOutcome};
