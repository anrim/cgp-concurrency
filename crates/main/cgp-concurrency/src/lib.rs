#![no_std]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

extern crate alloc;
extern crate cgp_core as cgp;

mod fork;
mod join;
mod map;

pub use cgp_concurrency_runtime as runtime;
#[cfg(feature = "smol")]
pub use cgp_concurrency_smol as smol;
#[cfg(feature = "tokio")]
pub use cgp_concurrency_tokio as tokio;
pub use fork::{
    CanSpawnCancellableHandlerTasks, CanSpawnHandlerTasks, CanTrySpawnHandlerTasks,
    HandlerTaskSpawner, HandlerTaskSpawnerComponent, HandlerTasks, SendHandler, SpawnLocalTasks,
    SpawnWorkerTasks, TryHandlerTaskSpawner, TryHandlerTaskSpawnerComponent,
};
pub use join::JoinHandlers;
pub use map::BoundedParallel;

#[doc(hidden)]
pub mod __private {
    pub use cgp::prelude::HasErrorType;
    pub use cgp_handler::Handler;
}

pub mod prelude {
    pub use crate::runtime::{
        CanSpawnLocalTask, CanSpawnTask, CancellationToken, Cancelled, HasRuntime, HasRuntimeType,
        LocalTaskSpawner, LocalTaskSpawnerComponent, RuntimeGetter, RuntimeGetterComponent,
        RuntimeOf, RuntimeTypeProvider, RuntimeTypeProviderComponent, SpawnError, TaskHandle,
        TaskOutcome, TaskSpawner, TaskSpawnerComponent,
    };
    pub use crate::{
        BoundedParallel, CanSpawnCancellableHandlerTasks, CanSpawnHandlerTasks,
        CanTrySpawnHandlerTasks, HandlerTaskSpawner, HandlerTaskSpawnerComponent, HandlerTasks,
        JoinHandlers, SendHandler, SpawnLocalTasks, SpawnWorkerTasks, TryHandlerTaskSpawner,
        TryHandlerTaskSpawnerComponent, impl_send_handler,
    };
}
