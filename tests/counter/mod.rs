//! Mock Counter daemon used across executor tests.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_executor::{
    BatchEffects, BatchPlan, CommandExecutor, Lowering, OperationEffects, OperationPlan,
};
use signal_frame::RequestPayload;
use signal_sema::SemaOperation;
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

impl CounterCommand {
    pub fn sema_operation(&self) -> SemaOperation {
        match self {
            Self::Increment { .. } => SemaOperation::Assert,
            Self::Decrement { .. } => SemaOperation::Retract,
            Self::Query => SemaOperation::Match,
            Self::ResetTracking => SemaOperation::Validate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterEffect {
    pub sema_operation: SemaOperation,
    pub outcome: CounterEffectOutcome,
}

impl CounterEffect {
    pub fn new(sema_operation: SemaOperation, outcome: CounterEffectOutcome) -> Self {
        Self {
            sema_operation,
            outcome,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterEffectOutcome {
    Wrote {
        rows_written: u64,
        rows_matched: u64,
    },
    Read {
        rows_read: u64,
    },
    Stream {
        subscription_token: u64,
    },
    Validated {
        predicate_held: bool,
    },
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
    type Effect = CounterEffect;

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
        effects: &[Self::Effect],
    ) -> Self::Reply {
        match operation {
            CounterOperation::Increment(_) => CounterReply::Incremented {
                rows_written: first_wrote_for(effects, SemaOperation::Assert),
            },
            CounterOperation::Decrement(_) => CounterReply::Decremented {
                rows_matched: first_matched_for(effects, SemaOperation::Retract),
            },
            CounterOperation::Query => CounterReply::Queried {
                rows_read: first_read_for(effects, SemaOperation::Match),
            },
            CounterOperation::ResetTracking => CounterReply::TrackingReset,
        }
    }
}

fn first_wrote_for(effects: &[CounterEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.sema_operation, &effect.outcome) {
            (sema_operation, CounterEffectOutcome::Wrote { rows_written, .. })
                if sema_operation == wanted =>
            {
                Some(*rows_written)
            }
            _ => None,
        })
        .unwrap_or(0)
}
fn first_matched_for(effects: &[CounterEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.sema_operation, &effect.outcome) {
            (sema_operation, CounterEffectOutcome::Wrote { rows_matched, .. })
                if sema_operation == wanted =>
            {
                Some(*rows_matched)
            }
            _ => None,
        })
        .unwrap_or(0)
}
fn first_read_for(effects: &[CounterEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.sema_operation, &effect.outcome) {
            (sema_operation, CounterEffectOutcome::Read { rows_read })
                if sema_operation == wanted =>
            {
                Some(*rows_read)
            }
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
    type Effect = CounterEffect;
    type Error = PoisonError;
    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Effect>, Self::Error> {
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
    ) -> OperationEffects<CounterEffect> {
        let effects: Vec<CounterEffect> = operation_plan
            .into_commands()
            .into_iter()
            .map(|command| self.execute_command(command))
            .collect();
        self.committed += effects.len() as u64;
        OperationEffects::new(effects)
    }

    fn execute_command(&self, command: CounterCommand) -> CounterEffect {
        match command.sema_operation() {
            SemaOperation::Assert => CounterEffect::new(
                SemaOperation::Assert,
                CounterEffectOutcome::Wrote {
                    rows_written: 1,
                    rows_matched: 0,
                },
            ),
            SemaOperation::Mutate => CounterEffect::new(
                SemaOperation::Mutate,
                CounterEffectOutcome::Wrote {
                    rows_written: 1,
                    rows_matched: 1,
                },
            ),
            SemaOperation::Retract => CounterEffect::new(
                SemaOperation::Retract,
                CounterEffectOutcome::Wrote {
                    rows_written: 1,
                    rows_matched: 1,
                },
            ),
            SemaOperation::Match => CounterEffect::new(
                SemaOperation::Match,
                CounterEffectOutcome::Read { rows_read: 7 },
            ),
            SemaOperation::Subscribe => CounterEffect::new(
                SemaOperation::Subscribe,
                CounterEffectOutcome::Stream {
                    subscription_token: 42,
                },
            ),
            SemaOperation::Validate => CounterEffect::new(
                SemaOperation::Validate,
                CounterEffectOutcome::Validated {
                    predicate_held: true,
                },
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("engine poisoned for test")]
pub struct PoisonError;
