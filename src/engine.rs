//! CommandExecutor: atomic commit point for component-local commands.

use crate::lowering::{BatchEffects, BatchPlan};
use signal_frame::{BatchFailureReason, CommitStatus, RetryClassification};

pub trait BatchErrorClassification {
    fn batch_failure_reason(&self) -> BatchFailureReason;
    fn retry_classification(&self) -> RetryClassification;
    fn commit_status(&self) -> CommitStatus;
}

pub trait CommandExecutor {
    type Command;
    type ComponentEffect;
    type Error: BatchErrorClassification;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error>;
}
