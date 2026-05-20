//! CommandExecutor: atomic commit point for component-local commands.

use crate::lowering::{BatchEffects, BatchPlan};
use signal_frame::BatchErrorClassification;
use std::future::Future;

pub trait CommandExecutor {
    type Command;
    type ComponentEffect;
    type Error: BatchErrorClassification;

    /// Execute the whole batch as one atomic component-local commit.
    ///
    /// Returning `Ok` means every command in every operation plan
    /// committed and produced effects. Returning `Err` means no command
    /// committed, or the executor cannot prove commit state and reports
    /// that uncertainty through `BatchErrorClassification`. There is no
    /// partial-success surface in this trait.
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl Future<
        Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error>,
    > + Send
    + '_;
}
