extern crate cgp_core as cgp;

use core::convert::Infallible;

use cgp::component::UseFields;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;
use cgp_handler::ReturnInput;

#[derive(Clone)]
struct Child;
impl HasErrorType for Child {
    type Error = Infallible;
}

type Workers = SpawnWorkerTasks<Product![ReturnInput, ReturnInput]>;

#[derive(HasField)]
struct Parent<R> {
    runtime: R,
}

delegate_and_check_components! {
    <R: CanSpawnTask> Parent<R> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
        #[check_params(((), Child, Product![String, u16]))]
        HandlerTaskSpawnerComponent: Workers,
    }
}

fn main() {
    let executor = Default::default();
    let parent = Parent {
        runtime: cgp_concurrency::smol::SmolSpawner(&executor),
    };
    let tasks = parent.spawn(
        PhantomData::<()>,
        Child,
        product![String::from("server"), 443_u16],
    );
    let product![label, port] = futures_lite::future::block_on(executor.run(tasks.join()));
    assert_eq!(label, TaskOutcome::Completed(Ok(String::from("server"))));
    assert_eq!(port, TaskOutcome::Completed(Ok(443)));
    println!("label = {label:?}, port = {port:?}");
}
