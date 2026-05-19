//! Unit tests for `SemaEffect` and `SemaEffectOutcome`.

use signal_executor::{SemaEffect, SemaEffectOutcome};
use signal_sema::SemaOperation;

#[test]
fn write_commit_witness_recognises_assert_with_rows() {
    let effect = SemaEffect::new(
        SemaOperation::Assert,
        SemaEffectOutcome::Wrote {
            rows_written: 1,
            rows_matched: 0,
        },
    );
    assert!(effect.is_write_commit());
}

#[test]
fn write_commit_witness_recognises_mutate_with_rows() {
    let effect = SemaEffect::new(
        SemaOperation::Mutate,
        SemaEffectOutcome::Wrote {
            rows_written: 2,
            rows_matched: 2,
        },
    );
    assert!(effect.is_write_commit());
}

#[test]
fn write_commit_witness_rejects_zero_row_write() {
    let effect = SemaEffect::new(
        SemaOperation::Retract,
        SemaEffectOutcome::Wrote {
            rows_written: 0,
            rows_matched: 0,
        },
    );
    assert!(!effect.is_write_commit());
}

#[test]
fn write_commit_witness_rejects_read() {
    let effect = SemaEffect::new(
        SemaOperation::Match,
        SemaEffectOutcome::Read { rows_read: 3 },
    );
    assert!(!effect.is_write_commit());
}

#[test]
fn write_commit_witness_rejects_stream() {
    let effect = SemaEffect::new(
        SemaOperation::Subscribe,
        SemaEffectOutcome::Stream {
            subscription_token: 1,
        },
    );
    assert!(!effect.is_write_commit());
}

#[test]
fn write_commit_witness_rejects_validate() {
    let effect = SemaEffect::new(
        SemaOperation::Validate,
        SemaEffectOutcome::Validated {
            predicate_held: true,
        },
    );
    assert!(!effect.is_write_commit());
}
