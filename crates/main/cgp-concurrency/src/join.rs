use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::task::{Context, Poll};

use cgp::base::types::{Cons, Nil};
use cgp::prelude::*;
use cgp_handler::{AsyncComputer, AsyncComputerComponent, Handler};
use futures_util::future::{MaybeDone, maybe_done};
use pin_project_lite::pin_project;

/// Joins a fixed `Product!` of CGP [`Handler`] providers in the caller's future.
///
/// Wire this provider to [`AsyncComputerComponent`] and pass a product with one input per
/// handler. Each handler receives the same context and code tag. Inputs and outputs may borrow
/// caller-local data, and a handler may hold non-`Send` values across suspension.
///
/// The list length is the concurrency limit. On the first poll, every branch can progress
/// while its siblings are pending. The join retains each completed branch's `Result` until
/// all branches finish, then returns them in declaration order. An application error occupies
/// its branch's slot. An empty product completes immediately.
/// Each branch stores its pending future or its completed result inline. The join assembles
/// the output product once, when all branches are ready.
///
/// Dropping the join drops pending branches and any retained results. A branch panic unwinds
/// through the caller when unwinding is enabled.
pub struct JoinHandlers<Handlers>(PhantomData<Handlers>);

#[cgp_impl(JoinHandlers<Handlers>)]
impl<Handlers, Code, Inputs, Outputs> AsyncComputer<Code, Inputs>
where
    Handlers: HandlerList<Self, Code, Inputs, Output = Outputs>,
{
    type Output = Outputs;

    async fn compute_async(&self, code: PhantomData<Code>, inputs: Inputs) -> Outputs {
        let mut branches = pin!(Handlers::branches(self, code, inputs));
        poll_fn(|cx| branches.as_mut().poll_joined(cx)).await
    }
}

trait HandlerList<App, Code, Inputs> {
    type Output;

    fn branches(
        app: &App,
        code: PhantomData<Code>,
        inputs: Inputs,
    ) -> impl BranchList<Output = Self::Output>;
}

impl<App, Code> HandlerList<App, Code, Nil> for Nil {
    type Output = Nil;

    fn branches(_: &App, _: PhantomData<Code>, _: Nil) -> impl BranchList<Output = Nil> {
        Nil
    }
}

impl<App, Code, Head, Tail, Input, Inputs> HandlerList<App, Code, Cons<Input, Inputs>>
    for Cons<Head, Tail>
where
    App: HasErrorType,
    Head: Handler<App, Code, Input>,
    Tail: HandlerList<App, Code, Inputs>,
{
    type Output = Cons<Result<Head::Output, App::Error>, Tail::Output>;

    fn branches(
        app: &App,
        code: PhantomData<Code>,
        inputs: Cons<Input, Inputs>,
    ) -> impl BranchList<Output = Self::Output> {
        Branches {
            // Invoke the handler on its first poll, even if constructing its future does work.
            head: maybe_done(async move { Head::handle(app, code, inputs.0).await }),
            tail: Tail::branches(app, code, inputs.1),
        }
    }
}

// The tail contains only its branch slots. Assemble the product once, after all are ready.
trait BranchList {
    type Output;

    fn poll_joined(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.as_mut().poll_all(cx) {
            Poll::Ready(self.take_outputs())
        } else {
            Poll::Pending
        }
    }

    fn poll_all(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool;
    fn take_outputs(self: Pin<&mut Self>) -> Self::Output;
}

impl BranchList for Nil {
    type Output = Nil;

    fn poll_all(self: Pin<&mut Self>, _: &mut Context<'_>) -> bool {
        true
    }

    fn take_outputs(self: Pin<&mut Self>) -> Nil {
        Nil
    }
}

pin_project! {
    struct Branches<Head: Future, Tail> {
        #[pin]
        head: MaybeDone<Head>,
        #[pin]
        tail: Tail,
    }
}

impl<Head: Future, Tail: BranchList> BranchList for Branches<Head, Tail> {
    type Output = Cons<Head::Output, Tail::Output>;

    fn poll_all(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool {
        let branches = self.project();
        let head_ready = branches.head.poll(cx).is_ready();
        let tail_ready = branches.tail.poll_all(cx);
        head_ready && tail_ready
    }

    fn take_outputs(self: Pin<&mut Self>) -> Self::Output {
        let branches = self.project();
        let head = branches.head.take_output().expect("all branches are ready");
        Cons(head, branches.tail.take_outputs())
    }
}
