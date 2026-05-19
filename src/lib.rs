//! Lowering trait and Executor struct.
//!
//! `signal-executor` is the shared library a triad daemon uses to
//! translate its public contract operations into Sema operations,
//! execute them atomically through a [`SemaEngine`], correlate each
//! Sema effect back to a per-operation reply, and publish operation /
//! effect events to subscribed observers.
//!
//! The crate is **library only**. It owns no runtime, no socket
//! mechanics, no actor supervision, no `tokio`. Daemons that drive
//! the executor wire it into whatever runtime they already use.
//!
//! See `ARCHITECTURE.md` for the atomicity contract, failure-mode
//! taxonomy, reply correlation, and observer publication ordering.
//! The motivating design lives in the primary workspace at
//! `reports/designer/243-reply-naming-observer-hook-executor-trait.md`
//! §3, with the broader migration spec at
//! `reports/designer/241-signal-architecture-migration-guide.md`.

pub mod effect;
pub mod engine;
pub mod error;
pub mod executor;
pub mod lowering;
pub mod observer;

pub use effect::{SemaEffect, SemaEffectOutcome};
pub use engine::SemaEngine;
pub use error::Error;
pub use executor::{Executor, ExecutorOutcome};
pub use lowering::Lowering;
pub use observer::{ObserverChannel, ObserverSet, RecordedEvent, RecordingChannel};
