//! Lowering trait and Executor struct.
//!
//! `signal-executor` is the shared library a triad daemon uses to
//! translate its public contract operations into component-local
//! commands, execute them atomically through a [`CommandExecutor`], correlate
//! each component-local effect back to a per-operation reply, and publish operation
//! / effect events to subscribed observers.
//!
//! The crate is **library only**. It owns no runtime, no socket
//! mechanics, no actor supervision, no `tokio`. Daemons that drive
//! the executor wire it into whatever runtime they already use.
//!
//! Per /246 §1: `Executor::execute` returns `Reply<L::Reply>`
//! directly. Engine errors are stashed on the executor for
//! daemon-side retrieval via
//! [`Executor::take_last_engine_error`](executor::Executor::take_last_engine_error);
//! the wire reply on engine failure is an accepted batch abort.
//!
//! Per /246 §3: the [`FrameObserverBridge`] composes three pieces
//! (`ObservationProjection` + `ObservableSet` +
//! [`ObserverDelivery`]) into an [`ObserverChannel<Operation, Effect>`]
//! that the executor publishes through. The traits live in
//! signal-frame so signal-frame's macro can emit the per-channel
//! impls without depending on signal-executor; this crate composes
//! them into the bridge struct.
//!
//! See `ARCHITECTURE.md` for the atomicity contract, failure-mode
//! taxonomy, reply correlation, and observer publication ordering.

pub mod bridge;
pub mod effect;
pub mod engine;
pub mod error;
pub mod executor;
pub mod lowering;
pub mod observer;

pub use bridge::{FrameObserverBridge, ObserverDelivery};
pub use effect::{SemaEffect, SemaEffectOutcome};
pub use engine::CommandExecutor;
pub use error::Error;
pub use executor::Executor;
pub use lowering::{BatchEffects, BatchPlan, Lowering, OperationEffects, OperationPlan};
pub use observer::{ObserverChannel, ObserverSet, RecordedEvent, RecordingChannel};

#[deprecated(
    note = "use CommandExecutor; Sema is now classification, not executable command shape"
)]
pub use engine::CommandExecutor as SemaEngine;

// Re-export the projection/observable-set traits for daemon convenience.
pub use signal_frame::{ObservableSet, ObservationProjection};
