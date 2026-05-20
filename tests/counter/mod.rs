//! Mock Counter daemon used across executor tests.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_executor::{Lowering, SemaEffect, SemaEffectOutcome, SemaEngine};
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
    pub fn new() -> Self { Self }
}

impl Default for CounterLowering {
    fn default() -> Self { Self::new() }
}

impl Lowering for CounterLowering {
    type Operation = CounterOperation;
    type Reply = CounterReply;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<Vec<SemaOperation>, Self::Reply> {
        match operation {
            CounterOperation::Increment(magnitude) | CounterOperation::Decrement(magnitude)
                if *magnitude == 0 =>
            {
                Err(CounterReply::MagnitudeRejected {
                    reason: MagnitudeRejectionReason::ZeroMagnitude,
                })
            }
            CounterOperation::Increment(_) => Ok(vec![SemaOperation::Assert]),
            CounterOperation::Decrement(_) => Ok(vec![SemaOperation::Retract]),
            CounterOperation::Query => Ok(vec![SemaOperation::Match]),
            CounterOperation::ResetTracking => Ok(Vec::new()),
        }
    }

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[SemaEffect],
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

fn first_wrote_for(effects: &[SemaEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.operation, &effect.outcome) {
            (op, SemaEffectOutcome::Wrote { rows_written, .. }) if op == wanted => Some(*rows_written),
            _ => None,
        })
        .unwrap_or(0)
}

fn first_matched_for(effects: &[SemaEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.operation, &effect.outcome) {
            (op, SemaEffectOutcome::Wrote { rows_matched, .. }) if op == wanted => Some(*rows_matched),
            _ => None,
        })
        .unwrap_or(0)
}

fn first_read_for(effects: &[SemaEffect], wanted: SemaOperation) -> u64 {
    effects
        .iter()
        .find_map(|effect| match (effect.operation, &effect.outcome) {
            (op, SemaEffectOutcome::Read { rows_read }) if op == wanted => Some(*rows_read),
            _ => None,
        })
        .unwrap_or(0)
}

pub struct CounterEngine {
    committed: u64,
    poisoned: bool,
}

impl CounterEngine {
    pub fn new() -> Self { Self { committed: 0, poisoned: false } }
    pub fn with_poison() -> Self { Self { committed: 0, poisoned: true } }
    pub fn committed_op_count(&self) -> u64 { self.committed }
}

impl Default for CounterEngine {
    fn default() -> Self { Self::new() }
}

impl SemaEngine for CounterEngine {
    type Error = PoisonError;

    fn execute_atomic(&mut self, ops: Vec<SemaOperation>) -> Result<Vec<SemaEffect>, Self::Error> {
        if self.poisoned {
            return Err(PoisonError);
        }
        let effects: Vec<SemaEffect> = ops
            .into_iter()
            .map(|op| match op {
                SemaOperation::Assert => SemaEffect::new(
                    op,
                    SemaEffectOutcome::Wrote { rows_written: 1, rows_matched: 0 },
                ),
                SemaOperation::Mutate | SemaOperation::Retract => SemaEffect::new(
                    op,
                    SemaEffectOutcome::Wrote { rows_written: 1, rows_matched: 1 },
                ),
                SemaOperation::Match => {
                    SemaEffect::new(op, SemaEffectOutcome::Read { rows_read: 7 })
                }
                SemaOperation::Subscribe => SemaEffect::new(
                    op,
                    SemaEffectOutcome::Stream { subscription_token: 42 },
                ),
                SemaOperation::Validate => SemaEffect::new(
                    op,
                    SemaEffectOutcome::Validated { predicate_held: true },
                ),
            })
            .collect();
        self.committed += effects.len() as u64;
        Ok(effects)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("engine poisoned for test")]
pub struct PoisonError;
