#[path = "wake_channel.rs"]
mod wake_channel;

use std::future::Future;
use std::pin::Pin;
use std::task::Context;

use async_channel::Receiver;
pub(crate) use wake_channel::wake_channel;

pub(crate) fn join_notification(join: &mut (impl Future + Unpin)) -> Receiver<()> {
    let (waker, published) = wake_channel();
    assert!(
        Pin::new(join)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    published
}
