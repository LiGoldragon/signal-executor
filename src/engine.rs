//! CommandExecutor: atomic commit point for component-local commands.

use crate::lowering::{BatchEffects, BatchPlan};
use signal_frame::BatchErrorClassification;

pub trait CommandExecutor {
    type Command;
    type ComponentEffect;
    type Error: BatchErrorClassification;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error>;
}
