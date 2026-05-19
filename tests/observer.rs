//! Unit tests for the observer surfaces (`ObserverSet`,
//! `RecordingChannel`, `RecordedEvent`).

use signal_executor::{
    ObserverChannel, ObserverSet, RecordedEvent, RecordingChannel, SemaEffect, SemaEffectOutcome,
};
use signal_sema::SemaOperation;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DummyOperation(&'static str);

#[test]
fn no_op_observer_publishes_silently() {
    let set: ObserverSet<DummyOperation> = ObserverSet::no_op();
    set.publish_operation_received(&DummyOperation("Submit"));
    set.publish_sema_effect_emitted(&SemaEffect::new(
        SemaOperation::Assert,
        SemaEffectOutcome::Wrote {
            rows_written: 1,
            rows_matched: 0,
        },
    ));
    // No assertions -- no panic is the witness.
}

#[test]
fn recording_channel_logs_operations_then_effects() {
    let channel = RecordingChannel::<DummyOperation>::new();
    channel.publish_operation_received(&DummyOperation("Increment"));
    channel.publish_sema_effect_emitted(&SemaEffect::new(
        SemaOperation::Assert,
        SemaEffectOutcome::Wrote {
            rows_written: 3,
            rows_matched: 0,
        },
    ));

    let events = channel.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        RecordedEvent::OperationReceived(DummyOperation("Increment"))
    ));
    assert!(matches!(events[1], RecordedEvent::SemaEffectEmitted(_)));
}

#[test]
fn observer_set_clones_share_underlying_channel() {
    let channel = RecordingChannel::<DummyOperation>::new();
    // Use the channel directly to capture events for assertion;
    // wrap a fresh copy in the observer set via the same Arc.
    use std::sync::Arc;

    struct ArcChannel(Arc<RecordingChannel<DummyOperation>>);
    impl ObserverChannel<DummyOperation> for ArcChannel {
        fn publish_operation_received(&self, operation: &DummyOperation) {
            self.0.publish_operation_received(operation);
        }
        fn publish_sema_effect_emitted(&self, effect: &SemaEffect) {
            self.0.publish_sema_effect_emitted(effect);
        }
    }

    let shared = Arc::new(channel);
    let observers = ObserverSet::new(ArcChannel(shared.clone()));
    let cloned = observers.clone();

    observers.publish_operation_received(&DummyOperation("First"));
    cloned.publish_operation_received(&DummyOperation("Second"));

    let events = shared.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        RecordedEvent::OperationReceived(DummyOperation("First"))
    ));
    assert!(matches!(
        events[1],
        RecordedEvent::OperationReceived(DummyOperation("Second"))
    ));
}
