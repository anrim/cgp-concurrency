use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use async_channel::{Receiver, Sender, bounded};

struct JoinWaker(Sender<()>);

impl Wake for JoinWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.0.try_send(());
    }
}

pub(crate) fn wake_channel() -> (Waker, Receiver<()>) {
    let (signal, wake) = bounded(1);
    (Waker::from(Arc::new(JoinWaker(signal))), wake)
}

pub(crate) fn join_notification(join: &mut (impl Future + Unpin)) -> Receiver<()> {
    let (waker, published) = wake_channel();
    assert!(
        Pin::new(join)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    published
}
