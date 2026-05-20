//! [`FrameObserverBridge`]: the crate-boundary projection bridge.
//!
//! Per /246 §3 + /141 hole 3: the executor publishes **raw execution
//! facts** (`Operation`, `CommandEffect<Command, ComponentEffect>`);
//! the macro-generated `<Channel>ObserverSet` publishes **channel event records**
//! (`OperationEvent`, `EffectEvent`). Something has to project raw
//! facts into channel records.
//!
//! `FrameObserverBridge` is that something. It composes three pieces:
//!
//! 1. A [`signal_frame::ObservationProjection`] impl -- contract-
//!    specific knowledge of how the channel's
//!    `OperationEvent` / `EffectEvent` records are built from
//!    raw operations / command-effect pairs.
//! 2. A [`signal_frame::ObservableSet`] impl (typically the
//!    macro-generated `<Channel>ObserverSet`) -- knows about
//!    subscriber filters and registration bookkeeping.
//! 3. An [`ObserverDelivery`] impl -- writes events to subscriber
//!    sockets. Daemon-supplied.
//!
//! The bridge impls [`ObserverChannel<Operation, Effect>`] so it plugs
//! directly into [`Executor::observers`](crate::Executor::observers).
//! When the executor publishes a raw fact, the bridge projects it
//! through the projection, asks the observer set to fan out to
//! matching subscribers, and routes each match through the delivery
//! callback.
//!
//! Re-exports `ObservableSet` and `ObservationProjection` from
//! `signal-frame` for downstream convenience; the traits live there
//! to keep signal-frame's dependency direction sound (it does not
//! reach back into signal-executor).

use signal_frame::{ObservableSet, ObservationProjection};

use crate::observer::ObserverChannel;

/// Daemon-supplied callback that ships an event to a subscriber's
/// socket / queue / sink.
///
/// One method per publication moment so the daemon's frame-encoding
/// path can dispatch by event role without runtime tag inspection.
/// The bridge calls the matching method when the observer set's
/// fanout selects a subscriber for the event.
///
/// The `Token` associated type matches the observer set's
/// subscription token (typically the macro-generated
/// `<Channel>ObserverSubscriptionToken`).
pub trait ObserverDelivery {
    /// Subscription token type the bridge passes through to identify
    /// the recipient.
    type Token;
    /// `operation_event` record type emitted by the channel.
    type OperationEvent;
    /// `effect_event` record type emitted by the channel.
    type EffectEvent;

    /// Deliver an `OperationEvent` to the subscriber identified by
    /// `token`. The daemon's impl encodes the event into a wire
    /// frame and writes it to the subscriber's socket.
    fn deliver_operation_event(&self, token: Self::Token, event: &Self::OperationEvent);

    /// Deliver an `EffectEvent` to the subscriber identified by
    /// `token`.
    fn deliver_effect_event(&self, token: Self::Token, event: &Self::EffectEvent);
}

/// Crate-boundary bridge between the executor's raw publication
/// surface and the macro-generated observer set.
///
/// Composing three roles (projection, observer set, delivery) keeps
/// each piece focused. A daemon that does not observe simply does
/// not construct this bridge; the executor accepts any
/// [`ObserverChannel`] impl (including the built-in `no_op`).
pub struct FrameObserverBridge<Projection, ObserverSetImpl, Delivery> {
    projection: Projection,
    observer_set: ObserverSetImpl,
    delivery: Delivery,
}

impl<Projection, ObserverSetImpl, Delivery>
    FrameObserverBridge<Projection, ObserverSetImpl, Delivery>
where
    Projection: ObservationProjection,
    ObserverSetImpl: ObservableSet<
            OperationEvent = Projection::OperationEvent,
            EffectEvent = Projection::EffectEvent,
        >,
    Delivery: ObserverDelivery<
            Token = ObserverSetImpl::Token,
            OperationEvent = Projection::OperationEvent,
            EffectEvent = Projection::EffectEvent,
        >,
{
    /// Compose the three pieces into a single observer bridge.
    pub fn new(projection: Projection, observer_set: ObserverSetImpl, delivery: Delivery) -> Self {
        Self {
            projection,
            observer_set,
            delivery,
        }
    }

    /// Borrow the observer set. Useful for register/unregister
    /// during subscription lifecycle handling.
    pub fn observer_set(&self) -> &ObserverSetImpl {
        &self.observer_set
    }

    /// Borrow the observer set mutably.
    pub fn observer_set_mut(&mut self) -> &mut ObserverSetImpl {
        &mut self.observer_set
    }

    /// Borrow the projection.
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Borrow the delivery callback.
    pub fn delivery(&self) -> &Delivery {
        &self.delivery
    }
}

impl<Projection, ObserverSetImpl, Delivery>
    ObserverChannel<Projection::Operation, Projection::Effect>
    for FrameObserverBridge<Projection, ObserverSetImpl, Delivery>
where
    Projection: ObservationProjection,
    ObserverSetImpl: ObservableSet<
            OperationEvent = Projection::OperationEvent,
            EffectEvent = Projection::EffectEvent,
        >,
    Delivery: ObserverDelivery<
            Token = ObserverSetImpl::Token,
            OperationEvent = Projection::OperationEvent,
            EffectEvent = Projection::EffectEvent,
        >,
{
    fn publish_operation_received(&self, operation: &Projection::Operation) {
        let event = self.projection.operation_event(operation);
        self.observer_set
            .publish_operation_received(&event, |token, projected_event| {
                self.delivery
                    .deliver_operation_event(token, projected_event);
            });
    }

    fn publish_effect_emitted(&self, effect: &Projection::Effect) {
        let event = self.projection.effect_event(effect);
        self.observer_set
            .publish_effect_emitted(&event, |token, projected_event| {
                self.delivery.deliver_effect_event(token, projected_event);
            });
    }
}
