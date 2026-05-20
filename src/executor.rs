//! [`Executor`]: orchestrates contract-to-Sema execution.
//!
//! The executor is the single binding point for:
//!
//! 1. Publishing every inbound contract operation to observers
//!    before any lowering happens.
//! 2. Lowering each operation to zero-or-more Sema operations via
//!    [`Lowering::lower`]; on `Err`, the request aborts before any
//!    Sema call and the contract-local rejection reply is carried in
//!    the failed operation's `SubReply`.
//! 3. Atomic commit of every Sema operation through
//!    [`SemaEngine::execute_atomic`]; on `Err`, the request is
//!    rejected with the engine's typed error and no Sema effect
//!    is visible to the caller.
//! 4. Publishing every emitted [`SemaEffect`] to observers after
//!    atomic commit succeeds.
//! 5. Building per-operation replies via
//!    [`Lowering::reply_from_effects`], in request order, with the
//!    full effects slice.
//!
//! The atomicity contract and the full failure-mode taxonomy are
//! documented in `ARCHITECTURE.md`. Observer publication ordering
//! is enforced by the executor and witnessed by the recording-channel
//! tests in `tests/`.

use signal_frame::{
    AcceptedOutcome, NonEmpty, OperationFailureReason, Reply, Request, RequestRejectionReason,
    SubReply,
};

use crate::effect::SemaEffect;
use crate::engine::SemaEngine;
use crate::lowering::Lowering;
use crate::observer::ObserverSet;

/// Orchestrator that lowers a [`Request`] through a [`Lowering`]
/// impl and a [`SemaEngine`] impl into a typed [`Reply`].
///
/// The executor owns three composable surfaces: the daemon's
/// [`Lowering`] (contract bridge), the daemon's [`SemaEngine`]
/// (atomic commit point), and an [`ObserverSet`] (publish surface).
/// Each daemon binds these once and reuses the executor across
/// requests.
pub struct Executor<LoweringImpl, SemaEngineImpl>
where
    LoweringImpl: Lowering,
    SemaEngineImpl: SemaEngine,
{
    lowering: LoweringImpl,
    sema_engine: SemaEngineImpl,
    observers: ObserverSet<LoweringImpl::Operation>,
}

impl<LoweringImpl, SemaEngineImpl> Executor<LoweringImpl, SemaEngineImpl>
where
    LoweringImpl: Lowering,
    SemaEngineImpl: SemaEngine,
{
    /// Construct an executor over a daemon's lowering, engine, and
    /// observer surfaces.
    pub fn new(
        lowering: LoweringImpl,
        sema_engine: SemaEngineImpl,
        observers: ObserverSet<LoweringImpl::Operation>,
    ) -> Self {
        Self {
            lowering,
            sema_engine,
            observers,
        }
    }

    /// Borrow the underlying lowering implementation.
    pub fn lowering(&self) -> &LoweringImpl {
        &self.lowering
    }

    /// Borrow the underlying engine implementation.
    pub fn sema_engine(&self) -> &SemaEngineImpl {
        &self.sema_engine
    }

    /// Borrow the observer set.
    pub fn observers(&self) -> &ObserverSet<LoweringImpl::Operation> {
        &self.observers
    }

    /// Execute a request through the lowering / engine / observer
    /// pipeline. Returns a typed [`ExecutorOutcome`] discriminating
    /// the three terminal paths: accepted, lowering-rejected, and
    /// engine-rejected.
    ///
    /// The orchestration ordering is fixed and witnessed by the
    /// observer tests:
    ///
    /// 1. Publish `OperationReceived` for every operation in the
    ///    request, in request order.
    /// 2. Lower every operation. On the first `Err`, return
    ///    [`ExecutorOutcome::LoweringRejected`] without calling
    ///    the engine.
    /// 3. Call [`SemaEngine::execute_atomic`] with the full Sema
    ///    operation sequence. On `Err`, return
    ///    [`ExecutorOutcome::EngineRejected`]; the atomicity
    ///    guarantee says no Sema effect is visible.
    /// 4. Publish `SemaEffectEmitted` for every effect, in commit
    ///    order.
    /// 5. Build per-operation replies via
    ///    [`Lowering::reply_from_effects`], in request order, with
    ///    the full effects slice.
    /// 6. Return [`ExecutorOutcome::Accepted`].
    pub fn execute(
        &mut self,
        request: Request<LoweringImpl::Operation>,
    ) -> ExecutorOutcome<LoweringImpl, SemaEngineImpl> {
        let payloads = request.payloads;

        // Step 1: publish every received operation before lowering.
        // This keeps observation tied to the received request shape,
        // including operations that may later become skipped because
        // an earlier lowering failed.
        for operation in &payloads {
            self.observers.publish_operation_received(operation);
        }

        // Step 2: lower every operation until the first domain
        // rejection. On failure, no Sema operation has run and the
        // wire reply remains frame-accepted but operation-aborted.
        let mut sema_operations: Vec<signal_sema::SemaOperation> = Vec::new();
        for (operation_index, operation) in payloads.iter().enumerate() {
            match self.lowering.lower(operation) {
                Ok(lowered) => sema_operations.extend(lowered),
                Err(contract_reply) => {
                    return ExecutorOutcome::LoweringRejected {
                        reply: Self::lowering_rejection_reply(
                            payloads.len(),
                            operation_index,
                            contract_reply,
                        ),
                    };
                }
            }
        }

        // Step 3: atomic commit. The atomicity contract guarantees
        // all-or-nothing -- on `Err`, no effect is visible to the
        // caller.
        let effects = match self.sema_engine.execute_atomic(sema_operations) {
            Ok(effects) => effects,
            Err(error) => {
                return ExecutorOutcome::EngineRejected {
                    reply: Reply::Rejected {
                        reason: RequestRejectionReason::Internal,
                    },
                    error,
                };
            }
        };

        // Step 4: publish committed effects in commit order.
        for effect in &effects {
            self.observers.publish_sema_effect_emitted(effect);
        }

        // Step 5: build per-operation replies. We map over the
        // non-empty payload sequence preserving non-emptiness, so
        // the wire reply's `per_operation: NonEmpty<SubReply<_>>`
        // is constructible without re-checking shape.
        let (head_operation, tail_operations) = payloads.into_head_and_tail();
        let head_reply = self.lowering.reply_from_effects(&head_operation, &effects);
        let tail_replies: Vec<SubReply<LoweringImpl::Reply>> = tail_operations
            .iter()
            .map(|operation| SubReply::Ok {
                payload: self.lowering.reply_from_effects(operation, &effects),
            })
            .collect();
        let per_operation = NonEmpty::from_head_and_tail(
            SubReply::Ok {
                payload: head_reply,
            },
            tail_replies,
        );

        ExecutorOutcome::Accepted {
            reply: Reply::Accepted {
                outcome: AcceptedOutcome::Completed,
                per_operation,
            },
            effects,
        }
    }

    fn lowering_rejection_reply(
        operation_count: usize,
        failed_at: usize,
        contract_reply: LoweringImpl::Reply,
    ) -> Reply<LoweringImpl::Reply> {
        let mut contract_reply = Some(contract_reply);
        let mut sub_replies = Vec::with_capacity(operation_count);

        for operation_index in 0..operation_count {
            let sub_reply = if operation_index < failed_at {
                SubReply::Invalidated
            } else if operation_index == failed_at {
                SubReply::Failed {
                    reason: OperationFailureReason::DomainRejection,
                    detail: contract_reply.take(),
                }
            } else {
                SubReply::Skipped
            };
            sub_replies.push(sub_reply);
        }

        let head = sub_replies.remove(0);
        Reply::aborted(
            failed_at,
            OperationFailureReason::DomainRejection,
            NonEmpty::from_head_and_tail(head, sub_replies),
        )
    }
}

/// Terminal outcome of [`Executor::execute`]. Discriminates the
/// three paths typed-summandwise so the daemon can react to each
/// without re-pattern-matching on [`Reply`].
///
/// Every variant carries the [`Reply`] to send back on the wire
/// alongside any per-path daemon-side material (engine error or
/// committed effects). Domain rejection details stay inside the
/// contract-local failed sub-reply.
pub enum ExecutorOutcome<LoweringImpl, SemaEngineImpl>
where
    LoweringImpl: Lowering,
    SemaEngineImpl: SemaEngine,
{
    /// Every operation lowered, atomic commit succeeded, replies
    /// built. The `effects` field carries the committed effects
    /// so daemons can drive post-execution side effects (logs,
    /// metrics, derived events) without re-computing them.
    Accepted {
        reply: Reply<LoweringImpl::Reply>,
        effects: Vec<SemaEffect>,
    },
    /// At least one operation failed at the lowering stage. The
    /// `reply` is a frame-accepted, operation-aborted reply whose
    /// failed operation carries the contract-local rejection detail.
    LoweringRejected { reply: Reply<LoweringImpl::Reply> },
    /// Atomic commit failed at the engine. The `reply` is
    /// `Reply::Rejected { reason:
    /// RequestRejectionReason::Internal }`; the typed `error`
    /// carries the engine's typed failure cause.
    EngineRejected {
        reply: Reply<LoweringImpl::Reply>,
        error: SemaEngineImpl::Error,
    },
}

impl<LoweringImpl, SemaEngineImpl> ExecutorOutcome<LoweringImpl, SemaEngineImpl>
where
    LoweringImpl: Lowering,
    SemaEngineImpl: SemaEngine,
{
    /// Borrow the [`Reply`] this outcome carries. Convenient when
    /// the daemon only needs the wire shape and not the per-path
    /// material.
    pub fn reply(&self) -> &Reply<LoweringImpl::Reply> {
        match self {
            Self::Accepted { reply, .. } => reply,
            Self::LoweringRejected { reply, .. } => reply,
            Self::EngineRejected { reply, .. } => reply,
        }
    }

    /// True when the outcome is [`Self::Accepted`].
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// True when the outcome is [`Self::LoweringRejected`] or
    /// [`Self::EngineRejected`].
    pub fn is_rejected(&self) -> bool {
        !self.is_accepted()
    }
}
