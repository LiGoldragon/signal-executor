//! Mock Counter daemon used across executor tests.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_executor::{
    BatchEffects, BatchPlan, CommandEffect, CommandExecutor, Lowering, OperationEffects,
    OperationPlan,
};
use signal_frame::RequestPayload;
use signal_sema::{SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};
use thiserror::Error;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum CounterOperation {
    Increment(u32),
    Decrement(u32),
    Query,
    ResetTracking,
}
impl RequestPayload for CounterOperation {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterCommand {
    Increment { magnitude: u32 },
    Decrement { magnitude: u32 },
    Query,
    ResetTracking,
}

impl ToSemaOperation for CounterCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::Increment { .. } => SemaOperation::Assert,
            Self::Decrement { .. } => SemaOperation::Retract,
            Self::Query => SemaOperation::Match,
            Self::ResetTracking => SemaOperation::Validate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterEffectOutcome {
    IncrementApplied { rows_written: u64 },
    DecrementApplied { rows_matched: u64 },
    ReadCompleted { rows_read: u64 },
    TrackingValidated { predicate_held: bool },
}

impl ToSemaOutcome for CounterEffectOutcome {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::IncrementApplied { .. } => SemaOutcome::Asserted,
            Self::DecrementApplied { .. } => SemaOutcome::Retracted,
            Self::ReadCompleted { .. } => SemaOutcome::Matched,
            Self::TrackingValidated { .. } => SemaOutcome::Validated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterReply {
    Incremented { rows_written: u64 },
    Decremented { rows_matched: u64 },
    Queried { rows_read: u64 },
    TrackingReset,
    MagnitudeRejected { reason: MagnitudeRejectionReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MagnitudeRejectionReason {
    #[error("zero-magnitude operation is not permitted")]
    ZeroMagnitude,
}

pub struct CounterLowering;
impl CounterLowering {
    pub fn new() -> Self {
        Self
    }
}
impl Default for CounterLowering {
    fn default() -> Self {
        Self::new()
    }
}

impl Lowering for CounterLowering {
    type Operation = CounterOperation;
    type Reply = CounterReply;
    type Command = CounterCommand;
    type ComponentEffect = CounterEffectOutcome;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<OperationPlan<Self::Command>, Self::Reply> {
        match operation {
            CounterOperation::Increment(magnitude) | CounterOperation::Decrement(magnitude)
                if *magnitude == 0 =>
            {
                Err(CounterReply::MagnitudeRejected {
                    reason: MagnitudeRejectionReason::ZeroMagnitude,
                })
            }
            CounterOperation::Increment(magnitude) => {
                Ok(OperationPlan::single(CounterCommand::Increment {
                    magnitude: *magnitude,
                }))
            }
            CounterOperation::Decrement(magnitude) => {
                Ok(OperationPlan::single(CounterCommand::Decrement {
                    magnitude: *magnitude,
                }))
            }
            CounterOperation::Query => Ok(OperationPlan::single(CounterCommand::Query)),
            CounterOperation::ResetTracking => {
                Ok(OperationPlan::single(CounterCommand::ResetTracking))
            }
        }
    }

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        match operation {
            CounterOperation::Increment(_) => CounterReply::Incremented {
                rows_written: first_increment_rows(effects),
            },
            CounterOperation::Decrement(_) => CounterReply::Decremented {
                rows_matched: first_decrement_rows(effects),
            },
            CounterOperation::Query => CounterReply::Queried {
                rows_read: first_read_rows(effects),
            },
            CounterOperation::ResetTracking => CounterReply::TrackingReset,
        }
    }
}

fn first_increment_rows(effects: &OperationEffects<CounterCommand, CounterEffectOutcome>) -> u64 {
    effects
        .component_effects()
        .find_map(|effect| match effect {
            CounterEffectOutcome::IncrementApplied { rows_written } => Some(*rows_written),
            _ => None,
        })
        .unwrap_or(0)
}

fn first_decrement_rows(effects: &OperationEffects<CounterCommand, CounterEffectOutcome>) -> u64 {
    effects
        .component_effects()
        .find_map(|effect| match effect {
            CounterEffectOutcome::DecrementApplied { rows_matched } => Some(*rows_matched),
            _ => None,
        })
        .unwrap_or(0)
}

fn first_read_rows(effects: &OperationEffects<CounterCommand, CounterEffectOutcome>) -> u64 {
    effects
        .component_effects()
        .find_map(|effect| match effect {
            CounterEffectOutcome::ReadCompleted { rows_read } => Some(*rows_read),
            _ => None,
        })
        .unwrap_or(0)
}

pub struct CounterEngine {
    committed: u64,
    poisoned: bool,
}
#[allow(dead_code)]
impl CounterEngine {
    pub fn new() -> Self {
        Self {
            committed: 0,
            poisoned: false,
        }
    }
    pub fn with_poison() -> Self {
        Self {
            committed: 0,
            poisoned: true,
        }
    }
    pub fn committed_operation_count(&self) -> u64 {
        self.committed
    }
}
impl Default for CounterEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandExecutor for CounterEngine {
    type Command = CounterCommand;
    type ComponentEffect = CounterEffectOutcome;
    type Error = PoisonError;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error> {
        if self.poisoned {
            return Err(PoisonError);
        }
        let (head_plan, tail_plans) = plan.into_operations().into_head_and_tail();
        let head_effects = self.execute_operation_plan(head_plan);
        let tail_effects = tail_plans
            .into_iter()
            .map(|operation_plan| self.execute_operation_plan(operation_plan))
            .collect();
        Ok(BatchEffects::from_head_and_tail(head_effects, tail_effects))
    }
}

impl CounterEngine {
    fn execute_operation_plan(
        &mut self,
        operation_plan: OperationPlan<CounterCommand>,
    ) -> OperationEffects<CounterCommand, CounterEffectOutcome> {
        let command_effects: Vec<CommandEffect<CounterCommand, CounterEffectOutcome>> =
            operation_plan
                .into_commands()
                .into_iter()
                .map(|command| self.execute_command(command))
                .collect();
        self.committed += command_effects.len() as u64;
        OperationEffects::new(
            signal_frame::NonEmpty::try_from_vec(command_effects)
                .expect("operation plan is statically non-empty"),
        )
    }

    fn execute_command(
        &self,
        command: CounterCommand,
    ) -> CommandEffect<CounterCommand, CounterEffectOutcome> {
        let effect = match &command {
            CounterCommand::Increment { .. } => {
                CounterEffectOutcome::IncrementApplied { rows_written: 1 }
            }
            CounterCommand::Decrement { .. } => {
                CounterEffectOutcome::DecrementApplied { rows_matched: 1 }
            }
            CounterCommand::Query => CounterEffectOutcome::ReadCompleted { rows_read: 7 },
            CounterCommand::ResetTracking => CounterEffectOutcome::TrackingValidated {
                predicate_held: true,
            },
        };
        CommandEffect::new(command, effect)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("engine poisoned for test")]
pub struct PoisonError;
