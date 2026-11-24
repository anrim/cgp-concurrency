extern crate cgp_core as cgp;

#[path = "typed_join/interleaved.rs"]
mod interleaved;

use core::cell::Cell;
use core::future::{Future, poll_fn};
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
    label_started: Cell<bool>,
    record_started: Cell<bool>,
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
        #[implicit] label_started: &Cell<bool>,
    ) -> Self::Output {
        let (gate, label) = input;
        label_started.set(true);
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
        #[implicit] record_started: &Cell<bool>,
    ) -> Result<usize, Error> {
        let (gate, record) = input;
        record_started.set(true);
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

struct EmptyApp;

delegate_and_check_components! {
    EmptyApp {
        #[check_params((ReadPair, Product![]))]
        AsyncComputerComponent: JoinHandlers<Product![]>,
    }
}

#[test]
fn empty_join_is_ready_on_first_poll_without_an_error_type() {
    let join = EmptyApp.compute_async(PhantomData::<ReadPair>, product![]);
    assert_eq!(future::block_on(future::poll_once(join)), Some(product![]));
}

#[test]
fn joins_singleton_product() {
    type One = JoinHandlers<Product![ReturnInput]>;
    let app = App::default();
    let join = One::compute_async(&app, PhantomData::<ReadPair>, product![42]);
    assert_eq!(future::block_on(join), product![Ok(42)]);
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
        assert!(!app.label_started.get() && !app.record_started.get());
        release_label.try_send(()).unwrap();
        assert!(future::block_on(future::poll_once(&mut join)).is_none());
        assert!(app.label_started.get() && app.record_started.get());
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

async fn reverse_completion() {
    let app = App::default();
    let local = String::from("borrowed label");
    let label = Rc::new(local.as_str());
    let (release_label, label_gate) = bounded(1);
    let (release_record, record_gate) = bounded(1);
    let inputs = product![(label_gate, Rc::clone(&label)), (record_gate, 17)];
    let mut join = pin!(app.compute_async(PhantomData::<ReadPair>, inputs));
    let mut opened_record = false;
    let mut opened_label = false;

    // Open each gate after the join yields Pending. The gate must wake the executor
    // to poll the join again, including when the ready branch is in the tail.
    let results = poll_fn(|cx| {
        let state = join.as_mut().poll(cx);
        if state.is_pending() {
            if !opened_record {
                assert!(app.label_started.get() && app.record_started.get());
                release_record.try_send(()).unwrap();
                opened_record = true;
            } else if !opened_label && release_record.is_closed() {
                // Only the second handler owns its receiver. It has returned its error.
                release_label.try_send(()).unwrap();
                opened_label = true;
            }
        }
        state
    })
    .await;

    assert!(opened_label);
    let results: Product![Result<Rc<&str>, InvalidRecord>, Result<usize, InvalidRecord>] = results;
    assert_eq!(results, product![Ok(label), Err(InvalidRecord(17))]);
}

async fn caller_scenarios() {
    reverse_completion().await;
    interleaved::middle_last_first().await;
}

#[test]
fn caller_future() {
    future::block_on(caller_scenarios());
}

#[cfg(feature = "tokio")]
#[test]
fn tokio_caller_future() {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(caller_scenarios());
}

#[cfg(feature = "smol")]
#[test]
fn smol_caller_future() {
    smol::block_on(caller_scenarios());
}
