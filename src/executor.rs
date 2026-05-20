//! Executor: orchestrates contract operation to component command execution.

use signal_frame::{
    AcceptedOutcome, BatchFailureReason, NonEmpty, OperationFailureReason, Reply, Request, SubReply,
};

use crate::engine::CommandExecutor;
use crate::lowering::Lowering;
use crate::observer::ObserverSet;

pub struct Executor<L, S>
where
    L: Lowering,
    S: CommandExecutor<Command = L::Command, Effect = L::Effect>,
{
    lowering: L,
    command_executor: S,
    observers: ObserverSet<L::Operation, L::Effect>,
    last_engine_error: Option<S::Error>,
}

impl<L, S> Executor<L, S>
where
    L: Lowering,
    S: CommandExecutor<Command = L::Command, Effect = L::Effect>,
{
    pub fn new(
        lowering: L,
        command_executor: S,
        observers: ObserverSet<L::Operation, L::Effect>,
    ) -> Self {
        Self {
            lowering,
            command_executor,
            observers,
            last_engine_error: None,
        }
    }

    pub fn lowering(&self) -> &L {
        &self.lowering
    }
    pub fn command_executor(&self) -> &S {
        &self.command_executor
    }
    pub fn observers(&self) -> &ObserverSet<L::Operation, L::Effect> {
        &self.observers
    }
    pub fn take_last_engine_error(&mut self) -> Option<S::Error> {
        self.last_engine_error.take()
    }

    pub fn execute(&mut self, request: Request<L::Operation>) -> Reply<L::Reply> {
        let total_operations = request.payloads().len();
        let mut commands: Vec<L::Command> = Vec::new();
        let mut operation_spans: Vec<(usize, usize)> = Vec::with_capacity(total_operations);

        for (operation_index, operation) in request.payloads().iter().enumerate() {
            self.observers.publish_operation_received(operation);
            match self.lowering.lower(operation) {
                Ok(operation_plan) => {
                    let span_start = commands.len();
                    commands.extend(operation_plan.into_commands());
                    let span_end = commands.len();
                    operation_spans.push((span_start, span_end));
                }
                Err(reply_detail) => {
                    return operation_aborted_reply(
                        total_operations,
                        operation_index,
                        reply_detail,
                    );
                }
            }
        }

        let effects = match self.command_executor.execute_atomic(commands) {
            Ok(effects) => effects,
            Err(error) => {
                self.last_engine_error = Some(error);
                return batch_aborted_reply(total_operations, BatchFailureReason::EngineRejected);
            }
        };

        for effect in &effects {
            self.observers.publish_effect_emitted(effect);
        }

        let (head_operation, tail_operations) = request.payloads.into_head_and_tail();
        let (head_start, head_end) = operation_spans[0];
        let head_reply = self
            .lowering
            .reply_from_effects(&head_operation, &effects[head_start..head_end]);

        let tail_replies: Vec<SubReply<L::Reply>> = tail_operations
            .iter()
            .enumerate()
            .map(|(tail_index, operation)| {
                let operation_index = tail_index + 1;
                let (start, end) = operation_spans[operation_index];
                SubReply::Ok(
                    self.lowering
                        .reply_from_effects(operation, &effects[start..end]),
                )
            })
            .collect();
        let per_operation = NonEmpty::from_head_and_tail(SubReply::Ok(head_reply), tail_replies);

        Reply::Accepted {
            outcome: AcceptedOutcome::Committed,
            per_operation,
        }
    }
}

fn operation_aborted_reply<P>(
    total_operations: usize,
    failed_at: usize,
    reply_detail: P,
) -> Reply<P> {
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
        outcome: AcceptedOutcome::OperationAborted {
            failed_at,
            reason: OperationFailureReason::DomainRejection,
        },
        per_operation,
    }
}

fn batch_aborted_reply<P>(total_operations: usize, reason: BatchFailureReason) -> Reply<P> {
    let per_operation = NonEmpty::try_from_vec(
        (0..total_operations)
            .map(|_| SubReply::Invalidated)
            .collect(),
    )
    .expect("requests are statically non-empty");
    Reply::Accepted {
        outcome: AcceptedOutcome::BatchAborted { reason },
        per_operation,
    }
}
