//! Unit tests for the observer surfaces (`ObserverSet`,
//! `RecordingChannel`, `RecordedEvent`).

use signal_executor::{
    CommandEffect, ObserverChannel, ObserverSet, RecordedEvent, RecordingChannel,
};
use signal_sema::{SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
struct DummyOperation(&'static str);

#[derive(Debug, Clone, PartialEq, Eq)]
enum DummyCommand {
    Submit,
}

impl ToSemaOperation for DummyCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::Submit => SemaOperation::Assert,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DummyEffect {
    Applied,
}

impl ToSemaOutcome for DummyEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::Applied => SemaOutcome::Asserted,
        }
    }
}

type DummyCommandEffect = CommandEffect<DummyCommand, DummyEffect>;

fn dummy_command_effect() -> DummyCommandEffect {
    CommandEffect::new(DummyCommand::Submit, DummyEffect::Applied)
}

#[test]
fn no_op_observer_publishes_silently() {
    let set: ObserverSet<DummyOperation, DummyCommandEffect> = ObserverSet::no_op();
    set.publish_operation_received(&DummyOperation("Submit"));
    set.publish_effect_emitted(&dummy_command_effect());
    // No assertions -- no panic is the witness.
}

#[test]
fn recording_channel_logs_operations_then_effects() {
    let channel = RecordingChannel::<DummyOperation, DummyCommandEffect>::new();
    channel.publish_operation_received(&DummyOperation("Increment"));
    channel.publish_effect_emitted(&dummy_command_effect());

    let events = channel.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        RecordedEvent::OperationReceived(DummyOperation("Increment"))
    ));
    assert!(matches!(events[1], RecordedEvent::EffectEmitted(_)));
}

#[test]
fn observer_set_clones_share_underlying_channel() {
    let channel = RecordingChannel::<DummyOperation, DummyCommandEffect>::new();
    // Use the channel directly to capture events for assertion;
    // wrap a fresh copy in the observer set via the same Arc.
    use std::sync::Arc;

    struct ArcChannel(Arc<RecordingChannel<DummyOperation, DummyCommandEffect>>);
    impl ObserverChannel<DummyOperation, DummyCommandEffect> for ArcChannel {
        fn publish_operation_received(&self, operation: &DummyOperation) {
            self.0.publish_operation_received(operation);
        }
        fn publish_effect_emitted(&self, effect: &DummyCommandEffect) {
            self.0.publish_effect_emitted(effect);
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
