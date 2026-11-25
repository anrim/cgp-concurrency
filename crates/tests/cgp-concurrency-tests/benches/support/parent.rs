use cgp::component::UseFields;
use cgp::prelude::*;
use cgp_concurrency::prelude::*;

#[derive(HasField)]
pub(super) struct Parent<R, Provider> {
    pub(super) runtime: R,
    pub(super) provider: PhantomData<Provider>,
}

delegate_components! {
    <R, Provider> Parent<R, Provider> {
        RuntimeTypeProviderComponent: UseType<R>,
        RuntimeGetterComponent: UseFields,
        HandlerTaskSpawnerComponent: Provider,
    }
}
