//! [`ObserverSet`] and [`ObserverChannel`]: observer bookkeeping
//! used by the executor to publish operation-received and
//! effect-emitted events.
//!
//! The shape here is intentionally minimal: an [`ObserverChannel`]
//! trait names the publish surface the executor calls, and
//! [`ObserverSet`] is the concrete bookkeeping type the daemon
//! holds. The actual transport (which channel, which subscribers,
//! whether they are routed via the macro-generated observable
//! stream) is daemon-side concern and is filled in by the
//! `observable` block the `signal_channel!` macro will emit.
//!
//! When the macro lands, daemons will instantiate an
//! [`ObserverChannel`] that bridges the executor's calls to the
//! macro-generated publish surface. Until then, daemons may
//! plug a custom impl (recording observer for tests; a no-op
//! observer for unit benchmarks; a real subscriber-set observer
//! for production).

use std::sync::{Arc, Mutex};

/// The publish surface the executor calls on a per-channel basis.
///
/// Implementors decide how to fan out to subscribers (per-channel
/// `Vec<Sender>`, a broadcast channel, a kernel actor). The
/// executor calls these methods at the points named in the
/// observer-publication-ordering contract; see
/// [`Executor`](crate::Executor) for the ordering guarantees.
///
/// `Operation` and `Effect` are generic so daemons that observe
/// the same channel against different transport shapes can share
/// the trait. The executor only ever calls
/// [`Self::publish_operation_received`] with `&Operation` from
/// the request payload set and
/// [`Self::publish_effect_emitted`] with `&Effect` after
/// each effect commits.
pub trait ObserverChannel<Operation, Effect> {
    /// Publish that an inbound contract operation was received.
    /// Called once per operation before lowering happens.
    fn publish_operation_received(&self, operation: &Operation);

    /// Publish that a component-local effect emitted after atomic commit.
    /// Called once per effect, in commit order, after the engine
    /// returns successfully.
    fn publish_effect_emitted(&self, effect: &Effect);
}

/// Concrete observer bookkeeping the executor uses.
///
/// Wraps an [`ObserverChannel`] in an [`Arc`] so the daemon can
/// share the same publish surface between the executor and the
/// macro-generated subscription stream.
///
/// The default is a no-op observer suitable for daemons that have
/// not yet wired the observable block; calling `publish_*` on it
/// is a structural no-op.
pub struct ObserverSet<Operation, Effect> {
    channel: Arc<dyn ObserverChannel<Operation, Effect> + Send + Sync>,
}

impl<Operation, Effect> ObserverSet<Operation, Effect> {
    /// Construct an observer set backed by the given channel.
    pub fn new<Channel>(channel: Channel) -> Self
    where
        Channel: ObserverChannel<Operation, Effect> + Send + Sync + 'static,
    {
        Self {
            channel: Arc::new(channel),
        }
    }

    /// Construct a no-op observer set. Useful for daemons that
    /// have not yet wired an observable block, and for tests
    /// that do not care about observer publication.
    pub fn no_op() -> Self
    where
        Operation: 'static,
        Effect: 'static,
    {
        Self::new(NoOpChannel::<Operation, Effect>::new())
    }

    /// Publish an inbound contract operation. Forwarded to the
    /// underlying channel.
    pub fn publish_operation_received(&self, operation: &Operation) {
        self.channel.publish_operation_received(operation);
    }

    /// Publish a component-local effect that committed. Forwarded to the
    /// underlying channel.
    pub fn publish_effect_emitted(&self, effect: &Effect) {
        self.channel.publish_effect_emitted(effect);
    }
}

impl<Operation, Effect> Clone for ObserverSet<Operation, Effect> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

/// No-op observer channel. Used by [`ObserverSet::no_op`].
struct NoOpChannel<Operation, Effect> {
    _phantom: std::marker::PhantomData<fn(Operation, Effect)>,
}

impl<Operation, Effect> NoOpChannel<Operation, Effect> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<Operation, Effect> ObserverChannel<Operation, Effect> for NoOpChannel<Operation, Effect> {
    fn publish_operation_received(&self, _operation: &Operation) {}
    fn publish_effect_emitted(&self, _effect: &Effect) {}
}

/// Recording observer channel for tests. Each `publish_*` call
/// appends a typed event into an internal log the test can read
/// back to assert ordering and content.
pub struct RecordingChannel<Operation: Clone, Effect: Clone> {
    log: Mutex<Vec<RecordedEvent<Operation, Effect>>>,
}

impl<Operation: Clone, Effect: Clone> RecordingChannel<Operation, Effect> {
    pub fn new() -> Self {
        Self {
            log: Mutex::new(Vec::new()),
        }
    }

    pub fn events(&self) -> Vec<RecordedEvent<Operation, Effect>> {
        self.log.lock().expect("recording channel poisoned").clone()
    }
}

impl<Operation: Clone, Effect: Clone> Default for RecordingChannel<Operation, Effect> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Operation: Clone, Effect: Clone> ObserverChannel<Operation, Effect>
    for RecordingChannel<Operation, Effect>
{
    fn publish_operation_received(&self, operation: &Operation) {
        self.log
            .lock()
            .expect("recording channel poisoned")
            .push(RecordedEvent::OperationReceived(operation.clone()));
    }

    fn publish_effect_emitted(&self, effect: &Effect) {
        self.log
            .lock()
            .expect("recording channel poisoned")
            .push(RecordedEvent::EffectEmitted(effect.clone()));
    }
}

/// Typed event recorded by [`RecordingChannel`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedEvent<Operation, Effect> {
    OperationReceived(Operation),
    EffectEmitted(Effect),
}
