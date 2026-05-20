//! End-to-end integration test for FrameObserverBridge.

use std::sync::{Arc, Mutex};

use signal_executor::{
    Executor, FrameObserverBridge, ObservableSet, ObservationProjection, ObserverDelivery,
    ObserverSet,
};
use signal_frame::{Reply, RequestPayload, SubscriptionTokenInner};

mod counter;

use counter::{
    CounterEffect, CounterEffectOutcome, CounterEngine, CounterLowering, CounterOperation,
    CounterReply,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationReceived {
    operation_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SemaEffectEmitted {
    effect_label: String,
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
    type EffectEvent = SemaEffectEmitted;

    fn publish_operation_received<F>(&self, event: &OperationReceived, mut deliver: F)
    where
        F: FnMut(SubscriptionTokenInner, &OperationReceived),
    {
        for (token, _filter) in self.subscribers.lock().unwrap().iter() {
            deliver(*token, event);
        }
    }

    fn publish_effect_emitted<F>(&self, event: &SemaEffectEmitted, mut deliver: F)
    where
        F: FnMut(SubscriptionTokenInner, &SemaEffectEmitted),
    {
        for (token, _filter) in self.subscribers.lock().unwrap().iter() {
            deliver(*token, event);
        }
    }
}

struct CounterProjection;

impl ObservationProjection for CounterProjection {
    type Operation = CounterOperation;
    type Effect = CounterEffect;
    type OperationEvent = OperationReceived;
    type EffectEvent = SemaEffectEmitted;

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

    fn effect_event(&self, effect: &CounterEffect) -> SemaEffectEmitted {
        SemaEffectEmitted {
            effect_label: format!("{:?}", effect.sema_operation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeliveredEvent {
    Operation(SubscriptionTokenInner, OperationReceived),
    Effect(SubscriptionTokenInner, SemaEffectEmitted),
}

struct RecordingDelivery {
    delivered: Arc<Mutex<Vec<DeliveredEvent>>>,
}

impl ObserverDelivery for RecordingDelivery {
    type Token = SubscriptionTokenInner;
    type OperationEvent = OperationReceived;
    type EffectEvent = SemaEffectEmitted;

    fn deliver_operation_event(&self, token: SubscriptionTokenInner, event: &OperationReceived) {
        self.delivered
            .lock()
            .unwrap()
            .push(DeliveredEvent::Operation(token, event.clone()));
    }

    fn deliver_effect_event(&self, token: SubscriptionTokenInner, event: &SemaEffectEmitted) {
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
            assert!(event.effect_label.contains("Assert"));
        }
        _ => panic!("expected effect event second"),
    }
}

#[allow(dead_code)]
fn _type_use(_outcome: CounterEffectOutcome, _reply: CounterReply) {}
