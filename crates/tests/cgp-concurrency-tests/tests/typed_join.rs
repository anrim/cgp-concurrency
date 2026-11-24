extern crate cgp_core as cgp;

use core::cell::Cell;
use core::pin::pin;
use std::rc::Rc;

use async_channel::{Receiver, bounded};
use cgp::error::ErrorTypeProviderComponent;
use cgp::prelude::*;
use cgp_concurrency::prelude::JoinHandlers;
use cgp_handler::{
    AsyncComputer, AsyncComputerComponent, CanComputeAsync, Handler, HandlerComponent, Promote,
    ReturnInput,
};
use futures_lite::future;

#[derive(Default, HasField)]
struct App {
    starts: Cell<usize>,
}

struct ReadPair;

#[derive(Debug, PartialEq)]
struct InvalidRecord(u32);

type LabelInput<'a> = (Receiver<()>, Rc<&'a str>);
type RecordInput = (Receiver<()>, u32);

#[cgp_impl(new ReadLabel)]
impl<'a> AsyncComputer<ReadPair, LabelInput<'a>> {
    type Output = Rc<&'a str>;

    async fn compute_async(
        &self,
        _: PhantomData<ReadPair>,
        input: LabelInput<'a>,
        #[implicit] starts: &Cell<usize>,
    ) -> Self::Output {
        let (gate, label) = input;
        starts.set(starts.get() + 1);
        gate.recv().await.expect("label gate closed");
        label
    }
}

#[cgp_impl(new RejectRecord)]
#[use_type(HasErrorType.Error = InvalidRecord)]
impl Handler<ReadPair, RecordInput> {
    type Output = usize;

    async fn handle(
        &self,
        _: PhantomData<ReadPair>,
        input: RecordInput,
        #[implicit] starts: &Cell<usize>,
    ) -> Result<usize, Error> {
        let (gate, record) = input;
        starts.set(starts.get() + 1);
        gate.recv().await.expect("record gate closed");
        Err(InvalidRecord(record))
    }
}

delegate_and_check_components! {
    App {
        ErrorTypeProviderComponent: UseType<InvalidRecord>,
        #[check_params((ReadPair, Product![LabelInput<'static>, RecordInput]))]
        AsyncComputerComponent: JoinHandlers<Product![Promote<ReadLabel>, RejectRecord]>,
    }
}

#[test]
fn joins_empty_singleton_and_longer_products() {
    future::block_on(async {
        type Empty = JoinHandlers<Product![]>;
        type One = JoinHandlers<Product![ReturnInput]>;
        type Three = JoinHandlers<Product![ReturnInput, RejectRecord, ReturnInput]>;
        let app = App::default();
        let empty =
            <Empty as AsyncComputer<_, ReadPair, _>>::compute_async(&app, PhantomData, product![])
                .await;
        assert_eq!(empty, product![]);
        let one =
            <One as AsyncComputer<_, ReadPair, _>>::compute_async(&app, PhantomData, product![42])
                .await;
        assert_eq!(one, product![Ok(42)]);

        let (release, gate) = bounded(1);
        release.try_send(()).unwrap();
        let inputs = product!["label", (gate, 17), true];
        let three =
            <Three as AsyncComputer<_, ReadPair, _>>::compute_async(&app, PhantomData, inputs)
                .await;
        assert_eq!(
            three,
            product![Ok("label"), Err(InvalidRecord(17)), Ok(true)]
        );
    });
}

#[test]
fn dropping_join_releases_pending_branch_and_retained_result() {
    let app = App::default();
    let local = String::from("borrowed label");
    let label = Rc::new(local.as_str());
    let (release_label, label_gate) = bounded(1);
    let (release_record, record_gate) = bounded(1);
    {
        let inputs = product![(label_gate, Rc::clone(&label)), (record_gate, 17)];
        let mut join = pin!(app.compute_async(PhantomData::<ReadPair>, inputs));
        assert_eq!(app.starts.get(), 0);
        release_label.try_send(()).unwrap();
        assert!(future::block_on(future::poll_once(&mut join)).is_none());
        assert_eq!(app.starts.get(), 2);
        assert!(release_label.is_closed());
        assert!(!release_record.is_closed());
        assert_eq!(Rc::strong_count(&label), 2);
    }
    assert!(release_record.is_closed());
    assert_eq!(Rc::strong_count(&label), 1);
}

#[test]
#[should_panic(expected = "record gate closed")]
fn branch_panic_reaches_caller() {
    let (_release_label, label_gate) = bounded(1);
    let (release_record, record_gate) = bounded(1);
    drop(release_record);
    let inputs = product![(label_gate, Rc::new("label")), (record_gate, 17)];
    let app = App::default();
    let join = app.compute_async(PhantomData::<ReadPair>, inputs);
    let _ = future::block_on(future::poll_once(join));
}
