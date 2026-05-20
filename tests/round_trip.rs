//! End-to-end tests through a mock Counter daemon.
//!
//! The Counter mock implements `Lowering` for a four-operation channel
//! (`Increment`, `Decrement`, `Query`, `ResetTracking`) backed by a
//! mock `CommandExecutor` that returns canned effects. The tests cover:
//!
//! - Single-operation accepted round trip.
//! - Multi-operation accepted round trip (Increment + Query + Decrement;
//!   effects correlate per operation via the executor's span tracking).
//! - Lowering rejection: single-operation and multi-operation shapes (typed contract
//!   reply detail rides in `SubReply::Failed { detail: Some(...) }`).
//! - Engine rejection (the engine returns Err on a poisoned command; the
//!   wire reply is `AcceptedOutcome::BatchAborted`).
//! - Observer publication ordering (operation-received before
//!   effects; effects in commit order).
//! - ResetTracking lowers to a typed command, not an empty Sema operation list.
//! - Doc check: `SubReply::Invalidated` covers both the
//!   ran-but-not-authoritative and lowered-but-not-committed cases.

use signal_executor::{
    CommandEffect, CommandExecutor, Executor, Lowering, ObserverSet, RecordedEvent,
    RecordingChannel,
};
use signal_frame::{
    AcceptedOutcome, BatchFailureReason, CommitStatus, OperationFailureReason, Reply,
    RequestBuilder, RequestPayload, RetryClassification, SubReply,
};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome};

mod counter;

use counter::{
    CounterCommand, CounterEffectOutcome, CounterEngine, CounterEngineFailure, CounterLowering,
    CounterOperation, CounterReply, MagnitudeRejectionReason,
};

type CounterCommandEffect = CommandEffect<CounterCommand, CounterEffectOutcome>;

#[test]
fn single_increment_round_trip() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = CounterOperation::Increment(3).into_request();
    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(outcome, AcceptedOutcome::Committed);
    assert_eq!(per_operation.len(), 1);

    let SubReply::Ok(payload) = per_operation.head() else {
        panic!("expected SubReply::Ok");
    };
    let CounterReply::Incremented { rows_written } = payload else {
        panic!("expected Incremented reply, got {payload:?}");
    };
    assert_eq!(*rows_written, 1);
}

#[test]
fn multi_operation_round_trip_correlates_replies() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(5))
        .with(CounterOperation::Query)
        .with(CounterOperation::Decrement(2))
        .build()
        .expect("non-empty request builds");

    let reply = executor.execute(request);
    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(outcome, AcceptedOutcome::Committed);
    assert_eq!(per_operation.len(), 3);

    let mut payloads = per_operation.iter();

    let SubReply::Ok(payload) = payloads.next().expect("head") else {
        panic!("expected SubReply::Ok at index 0");
    };
    assert!(matches!(payload, CounterReply::Incremented { .. }));

    let SubReply::Ok(payload) = payloads.next().expect("tail[0]") else {
        panic!("expected SubReply::Ok at index 1");
    };
    assert!(matches!(payload, CounterReply::Queried { .. }));

    let SubReply::Ok(payload) = payloads.next().expect("tail[1]") else {
        panic!("expected SubReply::Ok at index 2");
    };
    assert!(matches!(payload, CounterReply::Decremented { .. }));
}

#[test]
fn single_operation_lowering_rejection_returns_typed_failed_subreply() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    // 0 is the rejected magnitude per the Counter lowering.
    let request = CounterOperation::Increment(0).into_request();
    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected accepted-but-aborted reply");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::OperationAborted {
            failed_at: 0,
            reason: OperationFailureReason::DomainRejection,
        },
    );
    let SubReply::Failed { reason, detail } = per_operation.head() else {
        panic!("expected failed subreply");
    };
    assert_eq!(*reason, OperationFailureReason::DomainRejection);
    assert!(matches!(
        detail,
        Some(CounterReply::MagnitudeRejected {
            reason: MagnitudeRejectionReason::ZeroMagnitude,
        }),
    ));

    // The engine never saw a call; its committed-operation counter is 0.
    assert_eq!(executor.command_executor().committed_operation_count(), 0);
}

#[test]
fn multi_operation_lowering_rejection_invalidates_skips_and_fails() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(3))
        .with(CounterOperation::Increment(0))
        .with(CounterOperation::Query)
        .build()
        .expect("non-empty request builds");

    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected accepted-but-aborted reply");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::OperationAborted {
            failed_at: 1,
            reason: OperationFailureReason::DomainRejection,
        },
    );

    let replies: Vec<_> = per_operation.iter().collect();
    assert_eq!(replies.len(), 3);
    assert!(matches!(replies[0], SubReply::Invalidated));
    assert!(matches!(
        replies[1],
        SubReply::Failed {
            reason: OperationFailureReason::DomainRejection,
            detail: Some(CounterReply::MagnitudeRejected {
                reason: MagnitudeRejectionReason::ZeroMagnitude,
            }),
        },
    ));
    assert!(matches!(replies[2], SubReply::Skipped));
    assert_eq!(executor.command_executor().committed_operation_count(), 0);
}

#[test]
fn engine_rejection_returns_batch_aborted_reply() {
    let engine = CounterEngine::with_poison();
    let mut executor = Executor::new(CounterLowering::new(), engine, ObserverSet::no_op());

    let request = CounterOperation::Increment(1).into_request();
    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected accepted-but-batch-aborted reply");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::BatchAborted {
            reason: BatchFailureReason::EngineRejected,
            retry: RetryClassification::NotRetryable,
            commit: CommitStatus::NotCommitted,
        },
    );
    assert_eq!(per_operation.len(), 1);
    assert!(matches!(per_operation.head(), SubReply::Invalidated));

    // The typed engine error stays daemon-side via the executor's
    // last_engine_error stash, retrievable for logs / metrics /
    // supervision but NOT carried on the wire reply.
    let taken = executor.take_last_engine_error();
    assert_eq!(taken, Some(CounterEngineFailure::Poisoned));
    // Once taken, the stash is cleared.
    assert!(executor.take_last_engine_error().is_none());

    // Engine returned Err so no effects committed -- counter still 0.
    assert_eq!(executor.command_executor().committed_operation_count(), 0);
}

#[test]
fn engine_failure_classification_is_projected_to_batch_abort_reply() {
    let engine = CounterEngine::with_lost_commit_acknowledgement();
    let mut executor = Executor::new(CounterLowering::new(), engine, ObserverSet::no_op());

    let request = CounterOperation::Increment(1).into_request();
    let reply = executor.execute(request);

    let Reply::Accepted { outcome, .. } = reply else {
        panic!("expected accepted-but-batch-aborted reply");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::BatchAborted {
            reason: BatchFailureReason::EngineUnavailable,
            retry: RetryClassification::Retryable,
            commit: CommitStatus::Unknown,
        },
    );
    assert_eq!(
        executor.take_last_engine_error(),
        Some(CounterEngineFailure::LostCommitAcknowledgement),
    );
}

#[test]
fn multi_operation_engine_rejection_is_all_or_nothing() {
    // Witnesses the atomicity contract: when execute_atomic_batch returns
    // Err for a multi-operation request, no effect is visible to the
    // caller (the engine's commit counter stays at 0) and the
    // wire reply is a batch abort. No successful partial reply ever
    // appears.
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::with_poison(),
        ObserverSet::no_op(),
    );

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(2))
        .with(CounterOperation::Query)
        .with(CounterOperation::Decrement(1))
        .build()
        .expect("non-empty request builds");

    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected accepted-but-batch-aborted reply");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::BatchAborted {
            reason: BatchFailureReason::EngineRejected,
            retry: RetryClassification::NotRetryable,
            commit: CommitStatus::NotCommitted,
        },
    );
    assert_eq!(per_operation.len(), 3);
    assert!(
        per_operation
            .iter()
            .all(|reply| matches!(reply, SubReply::Invalidated)),
    );
    assert_eq!(executor.command_executor().committed_operation_count(), 0);
}

#[test]
fn observer_publication_order_accepted() {
    let recording =
        std::sync::Arc::new(RecordingChannel::<CounterOperation, CounterCommandEffect>::new());
    let observers = ObserverSet::new(ArcChannel(recording.clone()));

    let mut executor = Executor::new(CounterLowering::new(), CounterEngine::new(), observers);

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(2))
        .with(CounterOperation::Decrement(1))
        .build()
        .expect("non-empty request builds");

    let reply = executor.execute(request);
    assert!(matches!(reply, Reply::Accepted { .. }));

    let events = recording.events();

    // OperationReceived for every payload first, then EffectEmitted
    // for every committed component-local command effect in commit order.
    assert_eq!(events.len(), 4);
    assert!(matches!(
        events[0],
        RecordedEvent::OperationReceived(CounterOperation::Increment(2)),
    ));
    assert!(matches!(
        events[1],
        RecordedEvent::OperationReceived(CounterOperation::Decrement(1)),
    ));
    let RecordedEvent::EffectEmitted(effect) = &events[2] else {
        panic!("expected first committed effect");
    };
    assert_eq!(
        effect.sema_observation(),
        SemaObservation::new(SemaOperation::Assert, SemaOutcome::Asserted),
    );
    let RecordedEvent::EffectEmitted(effect) = &events[3] else {
        panic!("expected second committed effect");
    };
    assert_eq!(
        effect.sema_observation(),
        SemaObservation::new(SemaOperation::Retract, SemaOutcome::Retracted),
    );
}

#[test]
fn observer_receives_operations_even_on_lowering_rejection() {
    let recording =
        std::sync::Arc::new(RecordingChannel::<CounterOperation, CounterCommandEffect>::new());
    let observers = ObserverSet::new(ArcChannel(recording.clone()));

    let mut executor = Executor::new(CounterLowering::new(), CounterEngine::new(), observers);

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(3))
        .with(CounterOperation::Increment(0))
        .build()
        .expect("non-empty request builds");

    let reply = executor.execute(request);
    // Lowering rejection manifests as Reply::Accepted with operation-aborted
    // outcome, not as a kernel rejection.
    assert!(matches!(
        reply,
        Reply::Accepted {
            outcome: AcceptedOutcome::OperationAborted { .. },
            ..
        },
    ));

    let events = recording.events();
    // Both operations were observed; no effects (lowering failed).
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], RecordedEvent::OperationReceived(_)));
    assert!(matches!(events[1], RecordedEvent::OperationReceived(_)));
    assert!(
        events
            .iter()
            .all(|event| !matches!(event, RecordedEvent::EffectEmitted(_))),
        "no effects should be emitted on lowering rejection",
    );
}

#[test]
fn reset_tracking_lowers_to_typed_command() {
    // ResetTracking is deliberately represented as a typed component-local
    // command. The executor should not need a special empty-lowering path.
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = CounterOperation::ResetTracking.into_request();
    let reply = executor.execute(request);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(outcome, AcceptedOutcome::Committed);
    assert_eq!(per_operation.len(), 1);
    let SubReply::Ok(payload) = per_operation.head() else {
        panic!("expected SubReply::Ok");
    };
    assert!(matches!(payload, CounterReply::TrackingReset));
    assert_eq!(executor.command_executor().committed_operation_count(), 1);
}

#[test]
fn engine_rejection_does_not_carry_contract_reply() {
    // Engine rejection produces a batch-aborted reply with no typed contract
    // detail -- the typed engine cause stays daemon-side, not on the wire.
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::with_poison(),
        ObserverSet::no_op(),
    );

    let request = CounterOperation::Increment(2).into_request();
    let reply = executor.execute(request);

    match reply {
        Reply::Accepted {
            outcome,
            per_operation,
        } => {
            assert_eq!(
                outcome,
                AcceptedOutcome::BatchAborted {
                    reason: BatchFailureReason::EngineRejected,
                    retry: RetryClassification::NotRetryable,
                    commit: CommitStatus::NotCommitted,
                },
            );
            assert!(matches!(per_operation.head(), SubReply::Invalidated));
        }
        Reply::Rejected { .. } => panic!("engine failure must produce accepted batch abort"),
    }
}

// Test-only adapter so `Arc<RecordingChannel>` can be passed as a
// channel (impl ObserverChannel only over the owned channel value).
struct ArcChannel<Operation: Clone + Send + Sync + 'static, Effect: Clone + Send + Sync + 'static>(
    std::sync::Arc<RecordingChannel<Operation, Effect>>,
);

impl<Operation: Clone + Send + Sync + 'static, Effect: Clone + Send + Sync + 'static>
    signal_executor::ObserverChannel<Operation, Effect> for ArcChannel<Operation, Effect>
{
    fn publish_operation_received(&self, operation: &Operation) {
        self.0.publish_operation_received(operation);
    }
    fn publish_effect_emitted(&self, effect: &Effect) {
        self.0.publish_effect_emitted(effect);
    }
}

// Dummy uses of types from the crate so doc-level inference holds.
#[allow(dead_code)]
fn _type_uses(
    _lower: impl Lowering,
    _engine: impl CommandExecutor,
    _command_effect: CounterCommandEffect,
    _counter_outcome: CounterEffectOutcome,
) {
}
