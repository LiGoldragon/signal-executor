## signal-executor

`Lowering` trait, `SemaEngine` trait, and `Executor<L, S>` struct.
The shared library a triad daemon uses to translate its public
contract operations into executable Sema commands, with atomicity,
per-operation reply mapping, and observer publication.

Public contracts speak contract-local operations. `Lowering`
translates those public operations into executable commands for that
daemon's Sema engine. A command may project to a broad
`SemaOperation` class for observation, but the command is the value
that carries the table, record, predicate, revision, and other
execution detail.

## What this crate owns

- `Lowering` -- per-daemon trait with three associated types
  (`Operation`, `Reply`, `Command`) and two methods
  (`lower`, `reply_from_effects`). The contract-to-Sema bridge.
- `SemaEngine` -- atomic-commit trait with two associated types
  (`Command`, `Error`) and one method (`execute_atomic`).
- `Executor<L: Lowering, S: SemaEngine>` -- composes the two
  traits over an `ObserverSet`; exposes `execute(Request) ->
  ExecutorOutcome`.
- `ExecutorOutcome` -- closed enum with three terminal variants
  (`Accepted`, `LoweringRejected`, `EngineRejected`), each
  carrying the wire `Reply` plus per-path daemon-side material.
- `SemaEffect` + `SemaEffectOutcome` -- what happened after an
  executable command committed; the effect cites the broad
  `SemaOperation` class for observation.
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
executor binds every lowered command in one `execute_atomic` call so
the caller sees only two states: every state effect happened, or
none did.

Domain rejection during lowering is not a frame/kernel rejection. It
returns `Reply::Accepted { outcome: Aborted, per_operation: ... }`,
with the domain reply carried in the failed operation's
`SubReply::Failed.detail`.

When the engine itself returns `Err`, the wire `Reply` is
`Reply::Rejected { reason: RequestRejectionReason::Internal }` and
the typed engine error is carried via
`ExecutorOutcome::EngineRejected`.

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
    type Command = SpiritCommand;

    fn lower(&self, op: &Self::Operation)
        -> Result<Vec<Self::Command>, Self::Reply>
    { /* ... */ }

    fn reply_from_effects(&self, op: &Self::Operation, effects: &[SemaEffect])
        -> Self::Reply
    { /* ... */ }
}

struct SpiritEngine { /* ... */ }

impl SemaEngine for SpiritEngine {
    type Command = SpiritCommand;
    type Error = SpiritEngineError;
    fn execute_atomic(&mut self, commands: Vec<Self::Command>)
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
