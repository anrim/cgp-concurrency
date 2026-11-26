extern crate cgp_core as cgp;

#[path = "support/wake_channel.rs"]
mod wake_channel;

use core::cell::Cell;
use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::rc::Rc;

use async_channel::{Receiver, RecvError, Sender, TryRecvError, bounded};
use cgp::error::ErrorTypeProviderComponent;
use cgp::prelude::*;
use cgp_concurrency::prelude::BoundedParallel;
use cgp_handler::{AsyncComputerComponent, CanComputeAsync, Handler, HandlerComponent};
use wake_channel::wake_channel;

#[derive(Default)]
struct App<const LIMIT: usize> {
    pulled: Cell<usize>,
    started: Cell<usize>,
    active: Cell<usize>,
}

impl<const LIMIT: usize> App<LIMIT> {
    fn track_pulls<I: IntoIterator>(&self, inputs: I) -> impl Iterator<Item = I::Item> {
        inputs
            .into_iter()
            .inspect(|_| self.pulled.set(self.pulled.get() + 1))
    }

    fn start_handler(&self) -> ActiveHandler<'_> {
        let active = self.active.get() + 1;
        assert!(
            active <= LIMIT,
            "{active} handlers active with a limit of {LIMIT}"
        );
        self.started.set(self.started.get() + 1);
        self.active.set(active);
        ActiveHandler(&self.active)
    }
}

struct ActiveHandler<'a>(&'a Cell<usize>);

impl Drop for ActiveHandler<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

struct ReadLabels;
type Inbox<'a> = Receiver<Rc<&'a str>>;

fn prefilled_inbox<T>(label: T) -> (Sender<T>, Receiver<T>) {
    let (sender, inbox) = bounded(1);
    sender.try_send(label).unwrap();
    (sender, inbox)
}

#[cgp_impl(new ReceiveLabel)]
#[use_type(HasErrorType.Error = RecvError)]
impl<'a, const LIMIT: usize> Handler<ReadLabels, Inbox<'a>> for App<LIMIT> {
    type Output = Rc<&'a str>;

    async fn handle(
        &self,
        _: PhantomData<ReadLabels>,
        inbox: Inbox<'a>,
    ) -> Result<Self::Output, Error> {
        let _active = self.start_handler();
        inbox.recv().await
    }
}

delegate_and_check_components! {
    <const LIMIT: usize> App<LIMIT> {
        ErrorTypeProviderComponent: UseType<RecvError>,
        #[check_params((ReadLabels, [Inbox<'static>; 4]))]
        AsyncComputerComponent: BoundedParallel<ReceiveLabel, LIMIT>,
    }
}

// All gates run on this thread. Follow self-wakes until the map finishes or needs a gate.
fn try_finish_map<F: Future>(mut map: Pin<&mut F>) -> Result<F::Output, Receiver<()>> {
    loop {
        let (waker, wake) = wake_channel();
        if let Poll::Ready(output) = map.as_mut().poll(&mut Context::from_waker(&waker)) {
            return Ok(output);
        }
        drop(waker);
        match wake.try_recv() {
            Ok(()) => continue,
            Err(TryRecvError::Empty) => return Err(wake),
            Err(TryRecvError::Closed) => panic!("pending map dropped its waker"),
        }
    }
}

#[test]
fn empty_input_starts_no_handlers() {
    let app = App::<2>::default();
    let inputs: [Inbox<'_>; 0] = [];
    let map = pin!(app.compute_async(PhantomData::<ReadLabels>, inputs));
    let results = try_finish_map(map).expect("empty map stalled");
    assert!(results.is_empty());
    assert_eq!(app.started.get(), 0);
}

#[test]
fn second_item_error_admits_third_while_first_is_pending() {
    let app = App::<2>::default();
    let (first, first_inbox) = bounded(1);
    let (second, second_inbox) = bounded(1);
    let (third, third_inbox) = prefilled_inbox(Rc::new("third"));

    let inputs = app.track_pulls([first_inbox, second_inbox, third_inbox]);
    let mut map = pin!(app.compute_async(PhantomData::<ReadLabels>, inputs));
    let wake = try_finish_map(map.as_mut()).expect_err("first two inboxes are still pending");
    assert_eq!(app.pulled.get(), 2);
    assert_eq!(app.started.get(), 2);
    assert_eq!(app.active.get(), 2);

    drop(second); // Return RecvError and free a slot.
    wake.try_recv()
        .expect("closing the second inbox wakes the map");
    let wake = try_finish_map(map.as_mut()).expect_err("first inbox is still pending");
    assert_eq!(app.started.get(), 3);
    assert!(third.is_closed(), "third handler still owns its inbox");

    first.try_send(Rc::new("first")).unwrap();
    wake.try_recv()
        .expect("sending the first label wakes the map");
    let results = try_finish_map(map).expect("map stalled with every input ready");
    assert_eq!(
        results,
        [Ok(Rc::new("first")), Err(RecvError), Ok(Rc::new("third"))]
    );
}

#[test]
fn serial_map_admits_a_borrowed_label_after_an_error() {
    let app = App::<1>::default();
    let local = String::from("borrowed");
    let (first, first_inbox) = bounded(1);
    let (_, borrowed_inbox) = prefilled_inbox(Rc::new(local.as_str()));
    let inputs = app.track_pulls([first_inbox, borrowed_inbox]);
    let mut map = pin!(app.compute_async(PhantomData::<ReadLabels>, inputs));

    let wake = try_finish_map(map.as_mut()).expect_err("first inbox is still pending");
    assert_eq!(app.pulled.get(), 1, "serial map read ahead");
    assert_eq!(app.started.get(), 1);
    assert_eq!(app.active.get(), 1);
    drop(first);
    wake.try_recv()
        .expect("closing the first inbox wakes the serial map");

    let results: Vec<Result<Rc<&str>, RecvError>> =
        try_finish_map(map).expect("serial map stalled with every input ready");
    assert_eq!(results, [Err(RecvError), Ok(Rc::new(local.as_str()))]);
    assert_eq!(app.started.get(), 2);
}

#[test]
fn dropping_map_releases_retained_results_and_unfinished_inputs() {
    let app = App::<2>::default();
    let label = Rc::new("ready");
    let retained_label = Rc::downgrade(&label);
    let (ready, ready_inbox) = prefilled_inbox(label);
    let (pending, pending_inbox) = bounded(1);
    let (unread, unread_inbox) = bounded(1);
    let inputs = app.track_pulls([
        ready_inbox,
        pending_inbox.clone(),
        pending_inbox,
        unread_inbox,
    ]);
    let mut map = Box::pin(app.compute_async(PhantomData::<ReadLabels>, inputs));
    assert_eq!(app.started.get(), 0);
    try_finish_map(map.as_mut()).expect_err("two inboxes are still pending");
    assert!(ready.is_closed());
    assert!(retained_label.upgrade().is_some());
    assert_eq!(app.started.get(), 3);
    assert_eq!(app.active.get(), 2);
    assert!(!unread.is_closed());

    drop(map);
    assert!(retained_label.upgrade().is_none());
    assert!(pending.is_closed());
    assert!(unread.is_closed());
    assert_eq!(app.pulled.get(), 3);
    assert_eq!(app.started.get(), 3);
    assert_eq!(app.active.get(), 0);
}
