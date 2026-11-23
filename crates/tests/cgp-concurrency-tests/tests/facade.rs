use cgp_concurrency::runtime::{HasRuntimeType, RuntimeOf};

struct TestContext;

impl HasRuntimeType for TestContext {
    type Runtime = ();
}

#[test]
fn facade_reexports_the_cgp_runtime_foundation() {
    assert_eq!(core::any::type_name::<RuntimeOf<TestContext>>(), "()");
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_feature_reexports_the_provider() {
    assert_eq!(
        core::any::type_name::<cgp_concurrency::tokio::runtime::RuntimeOf<TestContext>>(),
        "()"
    );
}

#[cfg(feature = "smol")]
#[test]
fn smol_feature_reexports_the_provider() {
    assert_eq!(
        core::any::type_name::<cgp_concurrency::smol::runtime::RuntimeOf<TestContext>>(),
        "()"
    );
}
