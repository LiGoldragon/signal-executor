//! CommandExecutor: atomic commit point for component-local commands.

use crate::lowering::{BatchEffects, BatchPlan};
use signal_frame::BatchErrorClassification;
use std::future::Future;

pub trait CommandExecutor {
    type Command;
    type ComponentEffect;
    type Error: BatchErrorClassification;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> impl Future<
        Output = Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error>,
    > + Send
    + '_;
}
