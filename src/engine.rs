//! [`SemaEngine`]: the abstraction the executor calls into to commit
//! Sema operations atomically.
//!
//! The crate does not depend on `sema-engine` (the real engine).
//! Daemons that wire a real engine implement this trait against
//! their own backend. The executor speaks only to the trait,
//! preserving the atomicity contract through whatever the daemon's
//! engine provides.

use crate::effect::SemaEffect;
use signal_sema::SemaOperation;

/// Atomic commit point for a sequence of [`SemaOperation`]s.
///
/// Implementors guarantee:
///
/// * either every operation in `ops` commits, producing one
///   [`SemaEffect`] per operation in request order, or
/// * no operation commits, and the call returns
///   [`Err`].
///
/// There is no partial-commit return shape. Daemons that need
/// partial-commit semantics split the work across separate
/// [`Executor`](crate::Executor) calls -- atomicity is structural
/// per call.
///
/// The error type is associated so daemons can surface their
/// engine's typed errors through
/// [`ExecutorOutcome::EngineRejected`](crate::ExecutorOutcome::EngineRejected)
/// without losing the typed shape.
pub trait SemaEngine {
    /// Daemon-engine typed error. Surfaced verbatim through
    /// [`ExecutorOutcome::EngineRejected`](crate::ExecutorOutcome::EngineRejected)
    /// when atomic commit fails.
    type Error;

    /// Commit `ops` atomically. Returns one effect per operation
    /// in request order on success, or the typed error on failure.
    fn execute_atomic(&mut self, ops: Vec<SemaOperation>) -> Result<Vec<SemaEffect>, Self::Error>;
}
