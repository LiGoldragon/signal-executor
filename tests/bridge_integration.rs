//! End-to-end integration test for FrameObserverBridge.

use std::sync::{Arc, Mutex};

use signal_executor::{
    CommandEffect, Executor, FrameObserverBridge, ObservableSet, ObservationProjection,
    ObserverDelivery, ObserverSet,
};
use signal_frame::{Reply, RequestPayload, SubscriptionTokenInner};
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome};

mod counter;

use counter::{
    CounterCommand, CounterEffectOutcome, CounterEngine, CounterLowering, CounterOperation,
    CounterReply,
};

type CounterCommandEffect = CommandEffect<CounterCommand, CounterEffectOutcome>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationReceived {
    operation_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EffectEmitted {
    observation: SemaObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterObserverFilter {
    All,
}

struct CounterObserverSet {
    subscribers: Mutex<Vec<(SubscriptionTokenInner, CounterObserverFilter)>>,
    next_id: Mutex<u64>,
}

impl CounterObserverSet {
    fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            next_id: Mutex::new(1),
        }
    }

    fn register(&self, filter: CounterObserverFilter) -> SubscriptionTokenInner {
        let mut next_id = self.next_id.lock().unwrap();
        let token = SubscriptionTokenInner::new(*next_id);
        *next_id += 1;
        self.subscribers.lock().unwrap().push((token, filter));
        token
    }
}

impl ObservableSet for CounterObserverSet {
    type Token = SubscriptionTokenInner;
    type OperationEvent = OperationReceived;
    type EffectEvent = EffectEmitted;

    fn publish_operation_received<F>(&self, event: &OperationReceived, mut deliver: F)
    where
        F: FnMut(SubscriptionTokenInner, &OperationReceived),
    {
        for (token, _filter) in self.subscribers.lock().unwrap().iter() {
            deliver(*token, event);
        }
    }

    fn publish_effect_emitted<F>(&self, event: &EffectEmitted, mut deliver: F)
    where
        F: FnMut(SubscriptionTokenInner, &EffectEmitted),
    {
        for (token, _filter) in self.subscribers.lock().unwrap().iter() {
            deliver(*token, event);
        }
    }
}

struct CounterProjection;

impl ObservationProjection for CounterProjection {
    type Operation = CounterOperation;
    type Effect = CounterCommandEffect;
    type OperationEvent = OperationReceived;
    type EffectEvent = EffectEmitted;

    fn operation_event(&self, operation: &CounterOperation) -> OperationReceived {
        OperationReceived {
            operation_kind: match operation {
                CounterOperation::Increment(_) => "Increment",
                CounterOperation::Decrement(_) => "Decrement",
                CounterOperation::Query => "Query",
                CounterOperation::ResetTracking => "ResetTracking",
            }
            .into(),
        }
    }

    fn effect_event(&self, effect: &CounterCommandEffect) -> EffectEmitted {
        EffectEmitted {
            observation: effect.sema_observation(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeliveredEvent {
    Operation(SubscriptionTokenInner, OperationReceived),
    Effect(SubscriptionTokenInner, EffectEmitted),
}

struct RecordingDelivery {
    delivered: Arc<Mutex<Vec<DeliveredEvent>>>,
}

impl ObserverDelivery for RecordingDelivery {
    type Token = SubscriptionTokenInner;
    type OperationEvent = OperationReceived;
    type EffectEvent = EffectEmitted;

    fn deliver_operation_event(&self, token: SubscriptionTokenInner, event: &OperationReceived) {
        self.delivered
            .lock()
            .unwrap()
            .push(DeliveredEvent::Operation(token, event.clone()));
    }

    fn deliver_effect_event(&self, token: SubscriptionTokenInner, event: &EffectEmitted) {
        self.delivered
            .lock()
            .unwrap()
            .push(DeliveredEvent::Effect(token, event.clone()));
    }
}

#[test]
fn frame_observer_bridge_delivers_projected_events_in_order() {
    let observer_set = CounterObserverSet::new();
    let token = observer_set.register(CounterObserverFilter::All);
    let delivered: Arc<Mutex<Vec<DeliveredEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let delivery = RecordingDelivery {
        delivered: delivered.clone(),
    };

    let bridge = FrameObserverBridge::new(CounterProjection, observer_set, delivery);
    let observers = ObserverSet::new(bridge);

    let mut executor = Executor::new(CounterLowering::new(), CounterEngine::new(), observers);

    let request = CounterOperation::Increment(3).into_request();
    let reply = executor.execute(request);
    assert!(matches!(reply, Reply::Accepted { .. }));

    let events = delivered.lock().unwrap().clone();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0],
        DeliveredEvent::Operation(
            token,
            OperationReceived {
                operation_kind: "Increment".into()
            },
        )
    );
    match &events[1] {
        DeliveredEvent::Effect(t, event) => {
            assert_eq!(*t, token);
            assert_eq!(
                event.observation,
                SemaObservation::new(SemaOperation::Assert, SemaOutcome::Asserted),
            );
        }
        _ => panic!("expected effect event second"),
    }
}

#[allow(dead_code)]
fn _type_use(_outcome: CounterEffectOutcome, _reply: CounterReply) {}
