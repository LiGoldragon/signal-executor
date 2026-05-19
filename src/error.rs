//! Crate-boundary errors.
//!
//! The executor itself produces no errors at the boundary -- it
//! returns an [`ExecutorOutcome`](crate::ExecutorOutcome) that
//! discriminates accepted, lowering-rejected, and engine-rejected
//! paths typed-summandwise. This [`enum@Error`] enum covers structural
//! preconditions on inputs that fail before the executor's
//! orchestration starts.

use thiserror::Error;

/// Structural pre-execution errors at the executor boundary.
///
/// Daemons rarely encounter these in production because
/// [`Request<L::Operation>`](signal_frame::Request) is itself
/// non-empty by construction. The variant set exists so that
/// future wrapping layers (e.g. an async dispatcher that
/// pre-validates request shape) have a typed surface to fail
/// against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum Error {
    /// A request payload sequence was empty. Cannot happen for a
    /// well-formed [`signal_frame::Request`] -- the kernel type
    /// enforces non-emptiness. The variant is reserved for
    /// future input-shape checks before the executor runs.
    #[error("empty request payload sequence")]
    EmptyRequest,
}
