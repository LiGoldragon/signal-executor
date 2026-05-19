## signal-executor

`Lowering` trait, `SemaEngine` trait, and `Executor<L, S>` struct.
The shared library a triad daemon uses to translate its public
contract operations into Sema operations against `redb` tables, with
atomicity, per-operation reply mapping, and observer publication.

This crate is part of the signal-architecture migration that splits
the universal `SignalVerb` model into contract-local public verbs
plus a Sema execution vocabulary. The motivating design is in the
primary workspace at
`reports/designer/243-reply-naming-observer-hook-executor-trait.md`
and the broader migration spec at
`reports/designer/241-signal-architecture-migration-guide.md`.

## What this crate owns

- `Lowering` -- per-daemon trait with three associated types
  (`Operation`, `Reply`, `RejectionReason`) and two methods
  (`lower`, `reply_from_effects`). The contract-to-Sema bridge.
- `SemaEngine` -- atomic-commit trait with one associated type
  (`Error`) and one method (`execute_atomic`).
- `Executor<L: Lowering, S: SemaEngine>` -- composes the two
  traits over an `ObserverSet`; exposes `execute(Request) ->
  ExecutorOutcome`.
- `ExecutorOutcome` -- closed enum with three terminal variants
  (`Accepted`, `LoweringRejected`, `EngineRejected`), each
  carrying the wire `Reply` plus per-path daemon-side material.
- `SemaEffect` + `SemaEffectOutcome` -- what happened after a
  `SemaOperation` committed; the engine returns one per operation.
- `ObserverChannel` + `ObserverSet` + `RecordingChannel` +
  `RecordedEvent` -- observer publication surfaces for the
  introspection vocabulary.

## What this crate does not own

- Frame envelope, handshake, exchange identifiers, async correlation,
  streams, reply plumbing -- those live in `signal-frame`.
- Sema operation vocabulary and pattern primitives -- those live
  in `signal-sema`.
- The actual database engine. Daemons implement `SemaEngine` over
  their own backend.
- Public component operation vocabulary, async runtime, sockets,
  actor supervision, daemon-side policy.

## Atomicity contract

`SemaEngine::execute_atomic` guarantees all-or-none commit. The
executor binds every operation in one `execute_atomic` call so
the caller sees only two states: every state effect happened, or
none did. On `Err`, the wire `Reply` is `Reply::Rejected { reason:
RequestRejectionReason::Internal }` and the typed engine error is
carried via `ExecutorOutcome::EngineRejected`.

See `ARCHITECTURE.md` for the full failure-mode taxonomy, reply
correlation contract, and observer publication ordering.

## Usage shape

```rust,ignore
use signal_executor::{Executor, ExecutorOutcome, Lowering, ObserverSet, SemaEngine};
use signal_frame::Request;

struct SpiritLowering { /* state */ }

impl Lowering for SpiritLowering {
    type Operation = SpiritOperation;
    type Reply = SpiritReply;
    type RejectionReason = SpiritRejectionReason;

    fn lower(&self, op: &Self::Operation)
        -> Result<Vec<SemaOperation>, Self::RejectionReason>
    { /* ... */ }

    fn reply_from_effects(&self, op: &Self::Operation, effects: &[SemaEffect])
        -> Self::Reply
    { /* ... */ }
}

struct SpiritEngine { /* ... */ }

impl SemaEngine for SpiritEngine {
    type Error = SpiritEngineError;
    fn execute_atomic(&mut self, ops: Vec<SemaOperation>)
        -> Result<Vec<SemaEffect>, Self::Error>
    { /* ... */ }
}

fn handle(executor: &mut Executor<SpiritLowering, SpiritEngine>,
          request: Request<SpiritOperation>) {
    match executor.execute(request) {
        ExecutorOutcome::Accepted { reply, .. } => send(reply),
        ExecutorOutcome::LoweringRejected { reply, .. } => send(reply),
        ExecutorOutcome::EngineRejected { reply, .. } => send(reply),
    }
}
```

The Counter mock in `tests/counter/mod.rs` is a fuller worked
example.
