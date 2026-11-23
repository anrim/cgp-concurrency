#![no_std]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

extern crate alloc;
extern crate cgp_core as cgp;
#[cfg(feature = "std")]
extern crate std;

mod cancellation;
#[cfg(feature = "std")]
mod completion;
mod task;

pub use cancellation::{CancellationToken, Cancelled, SpawnError, TaskControl, TaskHandle};
pub use cgp_runtime::{
    HasRuntime, HasRuntimeType, RuntimeGetter, RuntimeGetterComponent, RuntimeOf,
    RuntimeTypeProvider, RuntimeTypeProviderComponent,
};
#[cfg(feature = "std")]
pub use completion::{TaskCompletion, UnclaimedOutput};
#[cfg(feature = "std")]
pub use task::panic_message;
pub use task::{
    CanSpawnLocalTask, CanSpawnTask, LocalTaskSpawner, LocalTaskSpawnerComponent, TaskOutcome,
    TaskSpawner, TaskSpawnerComponent,
};
