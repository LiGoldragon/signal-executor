//! End-to-end tests through a mock Counter daemon.
//!
//! The Counter mock implements `Lowering` for a three-operation
//! channel (`Increment`, `Decrement`, `Query`) backed by a mock
//! `SemaEngine` that returns canned effects. The tests cover:
//!
//! - Single-operation accepted round trip (Increment lowers to Assert).
//! - Multi-operation accepted round trip (Increment + Query lower to
//!   Assert + Match; effects correlate per operation).
//! - Lowering rejection (zero-magnitude Increment) as a typed
//!   failed sub-reply inside an aborted accepted frame.
//! - Engine rejection (the engine returns Err on a poisoned operation).
//! - Observer publication ordering (operation-received before
//!   effects; effects in commit order).
//! - Empty lowering (Query lowering to no Sema operations is legal).

use signal_executor::{
    Executor, ExecutorOutcome, Lowering, ObserverSet, RecordedEvent, RecordingChannel, SemaEffect,
    SemaEffectOutcome, SemaEngine,
};
use signal_frame::{
    AcceptedOutcome, OperationFailureReason, Reply, RequestBuilder, RequestPayload,
    RequestRejectionReason, SubReply,
};
use signal_sema::SemaOperation;

mod counter;

use counter::{
    CounterEngine, CounterLowering, CounterOperation, CounterRejection, CounterReply, PoisonError,
};

#[test]
fn single_increment_round_trip() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = CounterOperation::Increment(3).into_request();
    let outcome = executor.execute(request);

    let ExecutorOutcome::Accepted { reply, effects } = outcome else {
        panic!("expected accepted outcome");
    };

    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].operation, SemaOperation::Assert);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(outcome, AcceptedOutcome::Completed);
    assert_eq!(per_operation.len(), 1);

    let SubReply::Ok { payload } = per_operation.head() else {
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

    let outcome = executor.execute(request);
    let ExecutorOutcome::Accepted { reply, effects } = outcome else {
        panic!("expected accepted outcome");
    };

    // Two writes (Increment+Decrement) and one read (Query) -- effects
    // arrive in lowering order: Assert, Match, Retract. The Query
    // operation lowers to a Match.
    assert_eq!(effects.len(), 3);
    assert_eq!(effects[0].operation, SemaOperation::Assert);
    assert_eq!(effects[1].operation, SemaOperation::Match);
    assert_eq!(effects[2].operation, SemaOperation::Retract);

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(outcome, AcceptedOutcome::Completed);
    assert_eq!(per_operation.len(), 3);

    let mut payloads = per_operation.iter();

    let SubReply::Ok { payload } = payloads.next().expect("head") else {
        panic!("expected SubReply::Ok at index 0");
    };
    assert!(matches!(payload, CounterReply::Incremented { .. }));

    let SubReply::Ok { payload } = payloads.next().expect("tail[0]") else {
        panic!("expected SubReply::Ok at index 1");
    };
    assert!(matches!(payload, CounterReply::Queried { .. }));

    let SubReply::Ok { payload } = payloads.next().expect("tail[1]") else {
        panic!("expected SubReply::Ok at index 2");
    };
    assert!(matches!(payload, CounterReply::Decremented { .. }));
}

#[test]
fn lowering_rejection_returns_typed_failed_subreply() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    // 0 is the rejected magnitude per the Counter lowering.
    let request = CounterOperation::Increment(0).into_request();
    let outcome = executor.execute(request);

    let ExecutorOutcome::LoweringRejected { reply } = outcome else {
        panic!("expected LoweringRejected");
    };

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::Aborted {
            failed_at: 0,
            reason: OperationFailureReason::DomainRejection,
        },
    );
    assert_eq!(per_operation.len(), 1);
    assert!(matches!(
        per_operation.head(),
        SubReply::Failed {
            reason: OperationFailureReason::DomainRejection,
            detail: Some(CounterReply::Rejected(CounterRejection::ZeroMagnitude)),
        },
    ));

    // The engine never saw a call; its committed-operation counter is 0.
    assert_eq!(executor.sema_engine().committed_operation_count(), 0);
}

#[test]
fn multi_operation_lowering_rejection_invalidates_skips_and_fails() {
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(2))
        .with(CounterOperation::Increment(0))
        .with(CounterOperation::Query)
        .build()
        .expect("non-empty request builds");

    let outcome = executor.execute(request);
    let ExecutorOutcome::LoweringRejected { reply } = outcome else {
        panic!("expected LoweringRejected");
    };

    let Reply::Accepted {
        outcome,
        per_operation,
    } = reply
    else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(
        outcome,
        AcceptedOutcome::Aborted {
            failed_at: 1,
            reason: OperationFailureReason::DomainRejection,
        },
    );

    let mut per_operation = per_operation.iter();
    assert!(matches!(per_operation.next(), Some(SubReply::Invalidated)));
    assert!(matches!(
        per_operation.next(),
        Some(SubReply::Failed {
            reason: OperationFailureReason::DomainRejection,
            detail: Some(CounterReply::Rejected(CounterRejection::ZeroMagnitude)),
        }),
    ));
    assert!(matches!(per_operation.next(), Some(SubReply::Skipped)));
    assert!(per_operation.next().is_none());
    assert_eq!(executor.sema_engine().committed_operation_count(), 0);
}

#[test]
fn kernel_rejection_does_not_carry_contract_reply() {
    let engine = CounterEngine::with_poison();
    let mut executor = Executor::new(CounterLowering::new(), engine, ObserverSet::no_op());

    let request = CounterOperation::Increment(1).into_request();
    let outcome = executor.execute(request);

    let ExecutorOutcome::EngineRejected { reply, error } = outcome else {
        panic!("expected EngineRejected");
    };

    assert!(matches!(
        reply,
        Reply::Rejected {
            reason: RequestRejectionReason::Internal,
        },
    ));
    assert!(matches!(error, PoisonError));

    // Engine returned Err so no effects committed -- counter still 0.
    assert_eq!(executor.sema_engine().committed_operation_count(), 0);
}

#[test]
fn multi_operation_engine_rejection_is_all_or_nothing() {
    // Witnesses the atomicity contract: when execute_atomic returns
    // Err for a multi-operation request, no effect is visible to the
    // caller (the engine's commit counter stays at 0) and the
    // wire Reply is Rejected. No partial reply per-operation
    // surface ever appears.
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

    let outcome = executor.execute(request);

    let ExecutorOutcome::EngineRejected { reply, .. } = outcome else {
        panic!("expected EngineRejected");
    };

    // Wire shape carries no per-operation results -- pre-execution
    // shape from the caller's perspective even though we did call
    // the engine. Atomicity guarantees no committed effect is
    // visible.
    assert!(matches!(
        reply,
        Reply::Rejected {
            reason: RequestRejectionReason::Internal,
        },
    ));
    assert_eq!(executor.sema_engine().committed_operation_count(), 0);
}

#[test]
fn observer_publication_order_accepted() {
    let recording = std::sync::Arc::new(RecordingChannel::<CounterOperation>::new());
    let observers = ObserverSet::new(ArcChannel(recording.clone()));

    let mut executor = Executor::new(CounterLowering::new(), CounterEngine::new(), observers);

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(2))
        .with(CounterOperation::Decrement(1))
        .build()
        .expect("non-empty request builds");

    let outcome = executor.execute(request);
    assert!(outcome.is_accepted());

    let events = recording.events();

    // OperationReceived for every payload first, then SemaEffectEmitted
    // for every committed effect in commit order.
    assert_eq!(events.len(), 4);
    assert!(matches!(
        events[0],
        RecordedEvent::OperationReceived(CounterOperation::Increment(2)),
    ));
    assert!(matches!(
        events[1],
        RecordedEvent::OperationReceived(CounterOperation::Decrement(1)),
    ));
    assert!(matches!(
        events[2],
        RecordedEvent::SemaEffectEmitted(SemaEffect {
            operation: SemaOperation::Assert,
            ..
        }),
    ));
    assert!(matches!(
        events[3],
        RecordedEvent::SemaEffectEmitted(SemaEffect {
            operation: SemaOperation::Retract,
            ..
        }),
    ));
}

#[test]
fn observer_receives_operations_even_on_lowering_rejection() {
    let recording = std::sync::Arc::new(RecordingChannel::<CounterOperation>::new());
    let observers = ObserverSet::new(ArcChannel(recording.clone()));

    let mut executor = Executor::new(CounterLowering::new(), CounterEngine::new(), observers);

    let request = RequestBuilder::new()
        .with(CounterOperation::Increment(3))
        .with(CounterOperation::Increment(0))
        .with(CounterOperation::Query)
        .build()
        .expect("non-empty request builds");

    let outcome = executor.execute(request);
    assert!(matches!(outcome, ExecutorOutcome::LoweringRejected { .. }));

    let events = recording.events();
    // Every operation was observed; no effects (lowering failed).
    assert_eq!(events.len(), 3);
    assert!(matches!(events[0], RecordedEvent::OperationReceived(_)));
    assert!(matches!(events[1], RecordedEvent::OperationReceived(_)));
    assert!(matches!(events[2], RecordedEvent::OperationReceived(_)));
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, RecordedEvent::SemaEffectEmitted(_))),
        "no effects should be emitted on lowering rejection",
    );
}

#[test]
fn empty_lowering_is_legal() {
    // Configure operation has zero Sema effect by design (the
    // configure handler just records a daemon-side intent and
    // emits no Sema operations). The executor should accept,
    // call the engine with an empty operation vector, and build the reply
    // from the empty effects slice.
    let mut executor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );

    let request = CounterOperation::ResetTracking.into_request();
    let outcome = executor.execute(request);
    let ExecutorOutcome::Accepted { reply, effects } = outcome else {
        panic!("expected Accepted");
    };

    assert_eq!(effects.len(), 0);

    let Reply::Accepted { per_operation, .. } = reply else {
        panic!("expected Reply::Accepted");
    };
    assert_eq!(per_operation.len(), 1);
    let SubReply::Ok { payload } = per_operation.head() else {
        panic!("expected SubReply::Ok");
    };
    assert!(matches!(payload, CounterReply::TrackingReset));
}

#[test]
fn outcome_reply_accessor_works_for_every_variant() {
    // Build all three outcome variants and assert reply() reads them.
    let mut acceptor = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );
    let accepted = acceptor.execute(CounterOperation::Increment(1).into_request());
    assert!(matches!(accepted.reply(), Reply::Accepted { .. }));
    assert!(accepted.is_accepted());

    let mut lowering_rejector = Executor::new(
        CounterLowering::new(),
        CounterEngine::new(),
        ObserverSet::no_op(),
    );
    let lowering_rejected =
        lowering_rejector.execute(CounterOperation::Increment(0).into_request());
    assert!(matches!(lowering_rejected.reply(), Reply::Accepted { .. }));
    assert!(lowering_rejected.is_rejected());

    let mut engine_rejector = Executor::new(
        CounterLowering::new(),
        CounterEngine::with_poison(),
        ObserverSet::no_op(),
    );
    let engine_rejected = engine_rejector.execute(CounterOperation::Increment(1).into_request());
    assert!(matches!(
        engine_rejected.reply(),
        Reply::Rejected {
            reason: RequestRejectionReason::Internal,
        },
    ));
    assert!(engine_rejected.is_rejected());
}

// Test-only adapter so `Arc<RecordingChannel>` can be passed as a
// channel (impl ObserverChannel only over the owned channel value).
struct ArcChannel<Operation: Clone + Send + Sync + 'static>(
    std::sync::Arc<RecordingChannel<Operation>>,
);

impl<Operation: Clone + Send + Sync + 'static> signal_executor::ObserverChannel<Operation>
    for ArcChannel<Operation>
{
    fn publish_operation_received(&self, operation: &Operation) {
        self.0.publish_operation_received(operation);
    }
    fn publish_sema_effect_emitted(&self, effect: &SemaEffect) {
        self.0.publish_sema_effect_emitted(effect);
    }
}

// Dummy uses of types from the crate so doc-level inference holds.
#[allow(dead_code)]
fn _type_uses(
    _lower: impl Lowering,
    _engine: impl SemaEngine,
    _effect: SemaEffect,
    _outcome: SemaEffectOutcome,
) {
}
