use std::sync::Arc;
use std::task::{Wake, Waker};

use async_channel::{Receiver, Sender, bounded};

struct ChannelWaker(Sender<()>);

impl Wake for ChannelWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.0.try_send(());
    }
}

pub(crate) fn wake_channel() -> (Waker, Receiver<()>) {
    let (signal, wake) = bounded(1);
    (Waker::from(Arc::new(ChannelWaker(signal))), wake)
}
