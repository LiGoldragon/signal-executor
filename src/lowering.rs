//! [`Lowering`]: contract operation to executable command bridge.

use signal_frame::RequestPayload;

use crate::effect::SemaEffect;

/// Daemon-side bridge between a public contract operation enum and
/// the daemon's executable command enum.
pub trait Lowering {
    type Operation: RequestPayload;
    type Reply;
    type Command;

    /// Translate one contract operation into zero-or-more executable
    /// commands. Returning `Err(reply)` aborts the whole request and
    /// carries `reply` as the failed operation's typed detail.
    fn lower(&self, operation: &Self::Operation) -> Result<Vec<Self::Command>, Self::Reply>;

    /// Build the per-operation reply variant from the Sema effects
    /// emitted for this operation's commands.
    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[SemaEffect],
    ) -> Self::Reply;
}
