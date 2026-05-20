//! CommandExecutor: atomic commit point for component-local commands.

use crate::lowering::{BatchEffects, BatchPlan};

pub trait CommandExecutor {
    type Command;
    type Effect;
    type Error;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Effect>, Self::Error>;
}
