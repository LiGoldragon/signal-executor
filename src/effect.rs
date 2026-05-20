//! [`SemaEffect`]: a small generic Sema-classified effect record.
//!
//! The executor is generic over component-local effect records. This
//! type remains available for simple daemons and tests that only need
//! to describe a Sema operation class plus a compact outcome. More
//! specific components should define their own effect type and use
//! [`Lowering::reply_from_effects`](crate::Lowering::reply_from_effects)
//! over that type.
//!
//! The `outcome` field is a closed enum keyed off the operation
//! class -- write operations report row counts, read operations
//! report matched rows, stream operations report the subscription
//! token, validation operations report whether the predicate held.
//! The variant set is closed because the [`SemaOperation`] set is
//! closed; new operation variants in signal-sema arrive here as
//! new [`SemaEffectOutcome`] variants in the same change.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_sema::SemaOperation;

/// What happened after a single [`SemaOperation`] committed.
///
/// One effect per requested Sema operation, in request order. The
/// effect's `operation` field cites which Sema operation produced
/// it; the `outcome` field carries operation-class-specific
/// result data.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SemaEffect {
    pub operation: SemaOperation,
    pub outcome: SemaEffectOutcome,
}

impl SemaEffect {
    pub fn new(operation: SemaOperation, outcome: SemaEffectOutcome) -> Self {
        Self { operation, outcome }
    }

    /// Returns true when the effect indicates a state-changing
    /// operation that committed (Assert, Mutate, Retract with a
    /// non-zero row count).
    pub fn is_write_commit(&self) -> bool {
        let class_matches = matches!(
            self.operation,
            SemaOperation::Assert | SemaOperation::Mutate | SemaOperation::Retract,
        );
        let outcome_committed = matches!(
            &self.outcome,
            SemaEffectOutcome::Wrote { rows_written, .. } if *rows_written > 0,
        );
        class_matches && outcome_committed
    }
}

/// Operation-class-specific outcome of a committed Sema operation.
///
/// The variant set is closed because the [`SemaOperation`] set is
/// closed. Daemons read the variant that matches the operation
/// class -- a `Match` operation always produces `Read`, a `Subscribe`
/// always produces `Stream`, etc.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum SemaEffectOutcome {
    /// `Assert` / `Mutate` / `Retract` -- a write committed. The
    /// `rows_written` count names how many typed records the
    /// operation produced; `rows_matched` (relevant to `Mutate` /
    /// `Retract`) names how many existing records were addressed.
    Wrote {
        rows_written: u64,
        rows_matched: u64,
    },
    /// `Match` -- a read completed. `rows_read` names how many
    /// typed records the pattern matched.
    Read { rows_read: u64 },
    /// `Subscribe` -- a stream opened. `subscription_token` is the
    /// engine-assigned identifier the daemon will use to address
    /// the stream on close. The token is opaque to the executor.
    Stream { subscription_token: u64 },
    /// `Validate` -- a dry-run completed. `predicate_held` reports
    /// whether the dry-run predicate succeeded.
    Validated { predicate_held: bool },
}
