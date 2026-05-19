//! [`Lowering`]: the per-daemon translation from contract operations
//! to Sema operations and back from Sema effects to contract replies.
//!
//! A daemon's [`Lowering`] impl is the contract-to-Sema bridge. The
//! `lower` method translates one inbound contract operation into a
//! sequence of [`SemaOperation`]s; the `reply_from_effects` method
//! builds the per-operation reply variant from the slice of effects
//! the engine emitted.
//!
//! The trait is the only place a daemon's domain knowledge sits.
//! The [`Executor`](crate::Executor) machinery is fully generic
//! over the trait's associated types: the operation enum, the
//! reply enum, and the typed rejection reason.

use signal_frame::RequestPayload;
use signal_sema::SemaOperation;

use crate::effect::SemaEffect;

/// Daemon-side bridge between contract operations and Sema operations.
///
/// Implemented by each triad daemon for its public channel. The
/// trait owns three associated types:
///
/// * `Operation` -- the channel's operation enum (e.g.
///   `SpiritOperation`, emitted by `signal_channel!`); must satisfy
///   [`RequestPayload`] because the executor consumes a
///   [`Request<Operation>`](signal_frame::Request) directly.
/// * `Reply` -- the channel's reply payload enum (e.g.
///   `SpiritReply`); each operation's reply variant is selected by
///   [`Self::reply_from_effects`].
/// * `RejectionReason` -- the daemon-internal typed reason for a
///   pre-execution lowering failure. Surfaced to the daemon for
///   observability and logging; the on-wire reply on lowering
///   failure is the kernel `Reply::Rejected { reason:
///   RequestRejectionReason::Internal }` per signal-frame's typed
///   shape.
pub trait Lowering {
    /// The channel's operation enum.
    type Operation: RequestPayload;

    /// The channel's reply payload enum. Per-operation reply
    /// variants are selected from this enum by
    /// [`Self::reply_from_effects`].
    type Reply;

    /// Daemon-internal typed reason for a pre-execution lowering
    /// failure. The executor surfaces a kernel `Reply::Rejected
    /// { reason: RequestRejectionReason::Internal }` on the wire
    /// and carries this typed reason on the daemon side via
    /// [`ExecutorOutcome::LoweringRejected`](crate::ExecutorOutcome::LoweringRejected).
    type RejectionReason;

    /// Translate a single contract operation into zero-or-more Sema
    /// operations. An empty `Vec` means "no state effect" and is
    /// legitimate for validation-only or no-op operations.
    ///
    /// Returning `Err` rejects the entire request before any Sema
    /// operation runs. The atomicity contract guarantees no Sema
    /// effect is visible to the caller when this method returns
    /// `Err`.
    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<Vec<SemaOperation>, Self::RejectionReason>;

    /// Build the per-operation reply variant from the Sema effects
    /// the engine emitted for the request.
    ///
    /// Called once per operation, in request order, with the full
    /// effects slice. The impl is responsible for selecting the
    /// effects that correspond to the operation -- typically by
    /// counting forward through the slice in the order
    /// [`Self::lower`] produced operations.
    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[SemaEffect],
    ) -> Self::Reply;
}
