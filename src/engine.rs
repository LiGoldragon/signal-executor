//! [`SemaEngine`]: atomic commit point for executable commands.

use crate::effect::SemaEffect;

/// Atomic commit point for daemon-specific executable commands.
pub trait SemaEngine {
    /// Command accepted by the daemon's concrete state engine.
    type Command;

    /// Typed engine error kept daemon-side.
    type Error;

    /// Commit `commands` atomically. Returns one effect per command in
    /// request order on success, or a typed engine error on failure.
    fn execute_atomic(
        &mut self,
        commands: Vec<Self::Command>,
    ) -> Result<Vec<SemaEffect>, Self::Error>;
}
