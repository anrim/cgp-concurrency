#![forbid(unsafe_code)]
#![cfg(any(feature = "tokio", feature = "smol"))]

extern crate cgp_core as cgp;

mod app;
mod local_spawn;
