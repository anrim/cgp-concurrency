use alloc::vec::Vec;

use cgp::prelude::*;
use cgp_handler::{AsyncComputer, AsyncComputerComponent, Handler};
use futures_util::stream::{FuturesUnordered, StreamExt};

/// Maps one [`Handler`] over a finite iterable in the caller's task.
///
/// Wire [`AsyncComputerComponent`] to `BoundedParallel<Branch, LIMIT>` with a positive
/// compile-time limit. Each admitted item uses one slot until its handler finishes.
/// A completion frees the slot immediately, even if earlier items are still pending.
/// Each handler receives the caller's context and code tag and may borrow local data.
///
/// The returned vector contains every item result in input order. Errors occupy their
/// item's position and admission continues. Retained results use space proportional to
/// the number of items and do not count toward the in-flight limit.
///
/// Dropping the map drops the iterator, pending futures, and retained results. A handler
/// panic unwinds through the caller when unwinding is enabled.
///
/// Building a map with a zero limit fails at compile time.
///
/// ```compile_fail,E0080
/// use core::marker::PhantomData;
/// use cgp_core::error::ErrorOnly;
/// use cgp_concurrency::BoundedParallel;
/// use cgp_handler::{AsyncComputer, ReturnInput};
///
/// let app = ErrorOnly::<()>::default();
/// let map = BoundedParallel::<ReturnInput, 0>::compute_async(&app, PhantomData::<()>, [42]);
/// futures_lite::future::block_on(map);
/// ```
pub struct BoundedParallel<Branch, const LIMIT: usize>(PhantomData<Branch>);

#[cgp_impl(BoundedParallel<Branch, LIMIT>)]
#[use_type(HasErrorType.Error)]
#[use_provider(Branch: Handler<Code, Inputs::Item>)]
impl<Branch, const LIMIT: usize, Code, Inputs: IntoIterator> AsyncComputer<Code, Inputs> {
    type Output = Vec<Result<Branch::Output, Error>>;

    async fn compute_async(&self, code: PhantomData<Code>, inputs: Inputs) -> Self::Output {
        const { assert!(LIMIT > 0, "BoundedParallel requires a positive limit") };

        let mut inputs = inputs.into_iter().fuse();
        let mut in_flight = FuturesUnordered::new();
        let mut results = Vec::new();

        loop {
            for input in inputs.by_ref().take(LIMIT - in_flight.len()) {
                let index = results.len();
                results.push(None);
                in_flight.push(async move { (index, Branch::handle(self, code, input).await) });
            }

            let Some((index, result)) = in_flight.next().await else {
                break;
            };
            results[index] = Some(result);
        }

        results
            .into_iter()
            .map(|slot| slot.expect("every admitted handler has finished"))
            .collect()
    }
}
