extern crate cgp_core as cgp;

#[path = "support/join_notification.rs"]
mod join_notification;

use core::cell::Cell;
use core::future::Future;
use core::pin::pin;
use std::rc::Rc;

use async_channel::{Receiver, RecvError, bounded};
use cgp::error::ErrorTypeProviderComponent;
use cgp::prelude::*;
use cgp_concurrency::prelude::BoundedParallel;
use cgp_handler::{AsyncComputerComponent, CanComputeAsync, Handler, HandlerComponent};
use futures_lite::future;
use join_notification::join_notification;

#[derive(Default)]
struct App<const LIMIT: usize> {
    started: Cell<usize>,
    finished: Cell<usize>,
    peak: Cell<usize>,
}

struct ReadLabels;
type Inbox<'a> = Receiver<Rc<&'a str>>;

#[cgp_impl(new ReceiveLabel)]
#[use_type(HasErrorType.Error = RecvError)]
impl<'a, const LIMIT: usize> Handler<ReadLabels, Inbox<'a>> for App<LIMIT> {
    type Output = Rc<&'a str>;

    async fn handle(
        &self,
        _: PhantomData<ReadLabels>,
        inbox: Inbox<'a>,
    ) -> Result<Self::Output, Error> {
        self.started.set(self.started.get() + 1);
        let active = self.started.get() - self.finished.get();
        self.peak.set(self.peak.get().max(active));
        // Suspend even when the inbox already holds a label.
        future::yield_now().await;
        let result = inbox.recv().await;
        self.finished.set(self.finished.get() + 1);
        result
    }
}

delegate_and_check_components! {
    <const LIMIT: usize> App<LIMIT> {
        ErrorTypeProviderComponent: UseType<RecvError>,
        #[check_params((ReadLabels, [Inbox<'static>; 4]))]
        AsyncComputerComponent: BoundedParallel<ReceiveLabel, LIMIT>,
    }
}

// The test owns every gate. Consume self-wakes until the map needs an external wake,
// then return the notification for that wake.
fn poll_until_stalled(map: &mut (impl Future + Unpin)) -> Receiver<()> {
    loop {
        let wake = join_notification(map);
        if wake.try_recv().is_err() {
            return wake;
        }
    }
}

#[test]
fn maps_borrowed_labels_one_at_a_time_and_keeps_reading_after_an_error() {
    let app = App::<1>::default();
    let local = String::from("borrowed");
    let inputs = [Some("first"), None, Some(local.as_str()), Some("last")].map(|label| {
        let (sender, inbox) = bounded(1);
        if let Some(label) = label {
            sender.try_send(Rc::new(label)).unwrap();
        }
        inbox
    });

    let results: Vec<Result<Rc<&str>, RecvError>> =
        future::block_on(app.compute_async(PhantomData::<ReadLabels>, inputs));
    assert_eq!(
        results,
        [
            Ok(Rc::new("first")),
            Err(RecvError),
            Ok(Rc::new(local.as_str())),
            Ok(Rc::new("last"))
        ]
    );
    assert_eq!(app.started.get(), 4);
    assert_eq!(app.peak.get(), 1);
}

#[test]
fn dropping_map_releases_retained_results_and_unfinished_inputs() {
    let app = App::<2>::default();
    let label = Rc::new("ready");
    let (ready, first) = bounded(1);
    ready.try_send(Rc::clone(&label)).unwrap();
    let (sender, waiting) = bounded(1);
    {
        let inputs = [first, waiting.clone(), waiting.clone(), waiting];
        let mut map = pin!(app.compute_async(PhantomData::<ReadLabels>, inputs));
        assert_eq!(app.started.get(), 0);
        poll_until_stalled(&mut map);
        assert_eq!(app.finished.get(), 1);
        assert!(ready.is_closed());
        assert_eq!(Rc::strong_count(&label), 2);
        assert_eq!(app.started.get(), 3);
    }
    assert!(sender.is_closed());
    assert_eq!(Rc::strong_count(&label), 1);
    assert_eq!(app.started.get(), 3);
}
