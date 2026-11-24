extern crate cgp_core as cgp;

use core::mem::size_of_val;

use cgp::base::types::{Cons, Nil};
use cgp::prelude::*;
use cgp_concurrency::JoinHandlers;
use cgp_handler::{AsyncComputer, Handler, HandlerComponent};

struct App;

impl HasErrorType for App {
    type Error = ();
}

#[cgp_impl(new Block)]
#[use_type(HasErrorType.Error)]
impl Handler<(), ()> {
    type Output = [u8; 1024];

    async fn handle(&self, _: PhantomData<()>, _: ()) -> Result<Self::Output, Error> {
        Ok([0; 1024])
    }
}

type Four<T, Rest = Nil> = Cons<T, Cons<T, Cons<T, Cons<T, Rest>>>>;
type Eight<T, Rest = Nil> = Four<T, Four<T, Rest>>;
type Sixteen<T> = Eight<T, Eight<T>>;

fn future_bytes<Handlers, Inputs: Default>() -> usize
where
    JoinHandlers<Handlers>: AsyncComputer<App, (), Inputs>,
{
    size_of_val(&JoinHandlers::<Handlers>::compute_async(
        &App,
        PhantomData,
        Inputs::default(),
    ))
}

#[test]
fn doubling_branches_keeps_storage_near_linear() {
    let four = future_bytes::<Four<Block>, Four<()>>();
    let eight = future_bytes::<Eight<Block>, Eight<()>>();
    let sixteen = future_bytes::<Sixteen<Block>, Sixteen<()>>();
    eprintln!("join future bytes for 4, 8, 16 branches: {four}, {eight}, {sixteen}");
    // Allow for alignment and compiler layout differences while catching repeated tail buffers.
    assert!(eight <= four * 5 / 2 && sixteen <= eight * 5 / 2);
}
