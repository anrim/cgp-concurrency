use core::cell::Cell;
use core::future::{Future, poll_fn};
use core::pin::pin;

use async_channel::{Receiver, bounded};
use cgp::prelude::*;
use cgp_concurrency::JoinHandlers;
use cgp_handler::{AsyncComputerComponent, CanComputeAsync, Handler, HandlerComponent};

struct Lookup;

#[derive(Debug, PartialEq)]
enum LookupError {
    UnknownId,
    InvalidPort,
}

impl HasErrorType for Lookup {
    type Error = LookupError;
}

type Reply<'a, T> = (&'a Cell<bool>, Receiver<Result<T, LookupError>>);

#[cgp_impl(new AwaitReply)]
#[use_type(HasErrorType.Error = LookupError)]
impl<'a, T> Handler<(), Reply<'a, T>> {
    type Output = T;

    async fn handle(&self, _: PhantomData<()>, reply: Reply<'a, T>) -> Result<T, Error> {
        let (started, receiver) = reply;
        started.set(true);
        receiver.recv().await.expect("reply channel closed")
    }
}

type Inputs<'a> = Product![Reply<'a, u64>, Reply<'a, u16>, Reply<'a, &'a str>];

delegate_and_check_components! {
    Lookup {
        #[check_params(((), Inputs<'static>))]
        AsyncComputerComponent: JoinHandlers<Product![AwaitReply, AwaitReply, AwaitReply]>,
    }
}

pub(super) async fn middle_last_first() {
    let started = [const { Cell::new(false) }; 3];
    let label = String::from("server");
    let (first, id_reply) = bounded(1);
    let (middle, port_reply) = bounded(1);
    let (last, label_reply) = bounded(1);
    let inputs: Inputs<'_> = product![
        (&started[0], id_reply),
        (&started[1], port_reply),
        (&started[2], label_reply)
    ];
    let mut join = pin!(Lookup.compute_async(PhantomData, inputs));

    // Each handler owns its receiver. Its sender closes when that handler returns.
    let mut released = 0;
    let results = poll_fn(|cx| {
        let state = join.as_mut().poll(cx);
        if state.is_pending() {
            match released {
                0 => {
                    assert!(started.iter().all(Cell::get));
                    middle.try_send(Err(LookupError::InvalidPort)).unwrap();
                    released = 1;
                }
                1 if middle.is_closed() => {
                    assert!(!first.is_closed() && !last.is_closed());
                    last.try_send(Ok(label.as_str())).unwrap();
                    released = 2;
                }
                2 if last.is_closed() => {
                    assert!(!first.is_closed());
                    first.try_send(Err(LookupError::UnknownId)).unwrap();
                    released = 3;
                }
                _ => {}
            }
        }
        state
    })
    .await;

    assert_eq!(released, 3);
    assert_eq!(
        results,
        product![
            Err(LookupError::UnknownId),
            Err(LookupError::InvalidPort),
            Ok(label.as_str())
        ]
    );
}
