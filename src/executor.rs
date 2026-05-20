//! Executor: orchestrates contract-to-Sema execution per /246.

use signal_frame::{
    AcceptedOutcome, NonEmpty, OperationFailureReason, Reply, Request, RequestRejectionReason,
    SubReply,
};
use signal_sema::SemaOperation;

use crate::engine::SemaEngine;
use crate::lowering::Lowering;
use crate::observer::ObserverSet;

pub struct Executor<L, S>
where
    L: Lowering,
    S: SemaEngine,
{
    lowering: L,
    sema_engine: S,
    observers: ObserverSet<L::Operation>,
    last_engine_error: Option<S::Error>,
}

impl<L, S> Executor<L, S>
where
    L: Lowering,
    S: SemaEngine,
{
    pub fn new(lowering: L, sema_engine: S, observers: ObserverSet<L::Operation>) -> Self {
        Self {
            lowering,
            sema_engine,
            observers,
            last_engine_error: None,
        }
    }

    pub fn lowering(&self) -> &L { &self.lowering }
    pub fn sema_engine(&self) -> &S { &self.sema_engine }
    pub fn observers(&self) -> &ObserverSet<L::Operation> { &self.observers }

    pub fn take_last_engine_error(&mut self) -> Option<S::Error> {
        self.last_engine_error.take()
    }

    pub fn execute(&mut self, request: Request<L::Operation>) -> Reply<L::Reply> {
        let total_operations = request.payloads().len();
        let mut sema_ops: Vec<SemaOperation> = Vec::new();
        let mut operation_spans: Vec<(usize, usize)> = Vec::with_capacity(total_operations);

        for (operation_index, operation) in request.payloads().iter().enumerate() {
            self.observers.publish_operation_received(operation);
            match self.lowering.lower(operation) {
                Ok(lowered) => {
                    let span_start = sema_ops.len();
                    sema_ops.extend(lowered);
                    let span_end = sema_ops.len();
                    operation_spans.push((span_start, span_end));
                }
                Err(reply_detail) => {
                    return aborted_reply(total_operations, operation_index, reply_detail);
                }
            }
            debug_assert_eq!(operation_spans.len(), operation_index + 1);
        }

        let effects = match self.sema_engine.execute_atomic(sema_ops) {
            Ok(effects) => effects,
            Err(error) => {
                self.last_engine_error = Some(error);
                return Reply::Rejected {
                    reason: RequestRejectionReason::Internal,
                };
            }
        };

        for effect in &effects {
            self.observers.publish_sema_effect_emitted(effect);
        }

        let (head_op, tail_ops) = request.payloads.into_head_and_tail();
        let (head_start, head_end) = operation_spans[0];
        let head_reply = self
            .lowering
            .reply_from_effects(&head_op, &effects[head_start..head_end]);

        let tail_replies: Vec<SubReply<L::Reply>> = tail_ops
            .iter()
            .enumerate()
            .map(|(tail_index, op)| {
                let operation_index = tail_index + 1;
                let (start, end) = operation_spans[operation_index];
                SubReply::Ok(self.lowering.reply_from_effects(op, &effects[start..end]))
            })
            .collect();
        let per_operation = NonEmpty::from_head_and_tail(SubReply::Ok(head_reply), tail_replies);

        Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        }
    }
}

fn aborted_reply<P>(total_operations: usize, failed_at: usize, reply_detail: P) -> Reply<P> {
    debug_assert!(total_operations > 0);
    debug_assert!(failed_at < total_operations);

    let mut detail = Some(reply_detail);
    let mut sub_replies: Vec<SubReply<P>> = Vec::with_capacity(total_operations);
    for index in 0..total_operations {
        let sub_reply = if index < failed_at {
            SubReply::Invalidated
        } else if index == failed_at {
            SubReply::Failed {
                reason: OperationFailureReason::DomainRejection,
                detail: detail.take(),
            }
        } else {
            SubReply::Skipped
        };
        sub_replies.push(sub_reply);
    }

    let per_operation =
        NonEmpty::try_from_vec(sub_replies).expect("requests are statically non-empty");
    Reply::Accepted {
        outcome: AcceptedOutcome::Aborted {
            failed_at,
            reason: OperationFailureReason::DomainRejection,
        },
        per_operation,
    }
}
