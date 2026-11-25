use core::future::Future;
use core::pin::Pin;
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
///
/// ## Compose a join with a reporting handler
///
/// The reporting handler keeps the borrowed label even when parsing the port fails.
///
/// ```rust
/// extern crate cgp_core as cgp;
///
/// use core::num::ParseIntError;
/// use cgp::prelude::*;
/// use cgp_concurrency::prelude::JoinHandlers;
/// use cgp_handler::{CanHandle, Handler, HandlerComponent, PipeHandlers, Promote, ReturnInput};
///
/// struct App;
/// impl HasErrorType for App {
///     type Error = ParseIntError;
/// }
///
/// #[cgp_impl(new ParsePort)]
/// #[use_type(HasErrorType.Error = ParseIntError)]
/// impl<'a> Handler<(), &'a str> {
///     type Output = u16;
///
///     async fn handle(&self, _: PhantomData<()>, port: &'a str) -> Result<u16, Error> {
///         port.parse()
///     }
/// }
///
/// type Joined<'a> = Product![Result<&'a str, ParseIntError>, Result<u16, ParseIntError>];
///
/// #[cgp_impl(new MakeReport)]
/// #[use_type(HasErrorType.Error = ParseIntError)]
/// impl<'a> Handler<(), Joined<'a>> {
///     type Output = (&'a str, Option<u16>);
///
///     async fn handle(&self, _: PhantomData<()>, results: Joined<'a>) -> Result<Self::Output, Error> {
///         let product![label, port] = results;
///         Ok((label?, port.ok()))
///     }
/// }
///
/// type ReadEndpoint = JoinHandlers<Product![ReturnInput, ParsePort]>;
///
/// delegate_and_check_components! {
///     App {
///         #[check_params(((), Product![&'static str, &'static str]))]
///         HandlerComponent: PipeHandlers<Product![Promote<ReadEndpoint>, MakeReport]>,
///     }
/// }
///
/// futures_lite::future::block_on(async {
///     let label = String::from("server");
///     for (port, expected) in [("443", Some(443)), ("invalid", None)] {
///         let inputs = product![label.as_str(), port];
///         let report = App.handle(PhantomData::<()>, inputs).await.unwrap();
///         assert_eq!(report, (label.as_str(), expected));
///     }
/// });
/// ```
///
/// ## Input arity
///
/// A join with one handler rejects an input product with two elements.
///
/// ```compile_fail
/// extern crate cgp_core as cgp;
///
/// use cgp::prelude::*;
/// use cgp_concurrency::JoinHandlers;
/// use cgp_handler::{AsyncComputer, ReturnInput};
///
/// async fn surplus_input(app: &impl HasErrorType) {
///     type One = JoinHandlers<Product![ReturnInput]>;
///     One::compute_async(app, PhantomData::<()>, product![42_u8, true]).await;
/// }
/// ```
pub struct JoinHandlers<Handlers>(PhantomData<Handlers>);

#[cgp_impl(JoinHandlers<Handlers>)]
impl<Handlers, Code, Inputs, Outputs> AsyncComputer<Code, Inputs>
where
    Handlers: HandlerList<Self, Code, Inputs, Output = Outputs>,
{
    type Output = Outputs;

    async fn compute_async(&self, code: PhantomData<Code>, inputs: Inputs) -> Outputs {
        join_branches(Handlers::branches(self, code, inputs)).await
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
        // Invoke the handler on its first poll, even if constructing its future does work.
        let head = async move { Head::handle(app, code, inputs.0).await };
        Branches::new(head, Tail::branches(app, code, inputs.1))
    }
}

// The tail contains only its branch slots. Assemble the product once, after all are ready.
pub(crate) trait BranchList {
    type Output;

    fn poll_all(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool;
    fn take_outputs(self: Pin<&mut Self>) -> Self::Output;
}

pin_project! {
    pub struct JoinBranches<B> {
        #[pin]
        branches: B,
    }
}

pub(crate) fn join_branches<B: BranchList>(branches: B) -> JoinBranches<B> {
    JoinBranches { branches }
}

impl<B: BranchList<Output = Outputs>, Outputs> Future for JoinBranches<B> {
    type Output = Outputs;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut branches = self.project().branches;
        if branches.as_mut().poll_all(cx) {
            Poll::Ready(branches.take_outputs())
        } else {
            Poll::Pending
        }
    }
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
    pub struct Branches<Head: Future, Tail> {
        #[pin]
        head: MaybeDone<Head>,
        #[pin]
        tail: Tail,
    }
}

impl<Head: Future, Tail> Branches<Head, Tail> {
    pub(crate) fn new(head: Head, tail: Tail) -> Self {
        Self {
            head: maybe_done(head),
            tail,
        }
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

// A fork owns a completed task outcome until the entire product is handed to the caller.
pin_project! {
    pub struct TaskBranches<Head: Future, Tail> {
        #[pin]
        inner: Branches<Head, Tail>,
    }

    impl<Head: Future, Tail> PinnedDrop for TaskBranches<Head, Tail> {
        fn drop(this: Pin<&mut Self>) {
            if let Some(outcome) = this.project().inner.project().head.take_output() {
                crate::runtime::discard(outcome);
            }
        }
    }
}

impl<Head: Future, Tail> TaskBranches<Head, Tail> {
    pub(crate) fn new(head: Head, tail: Tail) -> Self {
        Self {
            inner: Branches::new(head, tail),
        }
    }
}

impl<Head: Future, Tail: BranchList> BranchList for TaskBranches<Head, Tail> {
    type Output = Cons<Head::Output, Tail::Output>;

    fn poll_all(self: Pin<&mut Self>, cx: &mut Context<'_>) -> bool {
        self.project().inner.poll_all(cx)
    }

    fn take_outputs(self: Pin<&mut Self>) -> Self::Output {
        self.project().inner.take_outputs()
    }
}
