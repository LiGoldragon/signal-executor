## signal-executor

`signal-executor` is the shared library a component daemon uses to
execute one `signal-frame::Request<Operation>` through the current
three-layer design:

```text
contract Operation  ->  component Command  ->  Sema observation class
external vocabulary     executable payload      payloadless classification
```

The executor does not execute `SemaOperation`. Execution is
component-local through the daemon's own `Command` and
`ComponentEffect` records. `signal-sema` supplies only the
payloadless observation projection: `SemaOperation`, `SemaOutcome`,
and `SemaObservation`.

## What this crate owns

- `Lowering` -- per-daemon trait that maps a public contract
  operation to an `OperationPlan<Command>` and maps committed
  `OperationEffects` back to a contract reply.
- `CommandExecutor` -- atomic commit trait over a
  `BatchPlan<Command>`.
- `Executor` -- composes lowering, atomic command execution, reply
  correlation, and observation publication.
- `CommandEffect<Command, ComponentEffect>` -- one executed command
  paired with the component-local effect it produced.
- `OperationEffects` and `BatchEffects` -- committed effects grouped
  by their source operation.
- `ObserverChannel`, `ObserverSet`, `RecordingChannel`, and
  `RecordedEvent` -- executor-facing observer publication surfaces.
- `FrameObserverBridge` -- bridge from raw executor facts to
  macro-generated observable streams through `ObservationProjection`.

## What this crate does not own

- Frame envelope, codec, streams, reply plumbing, or the
  `signal_channel!` macro output -- those live in `signal-frame`.
- Sema operation/outcome classification -- that lives in
  `signal-sema`.
- Database execution -- daemons implement `CommandExecutor` over
  their own storage engine.
- Public component operation vocabulary, async runtime, sockets,
  actor supervision, authentication, routing, or policy.

## Atomicity contract

`CommandExecutor::execute_atomic_batch` guarantees all-or-none
commit. It receives a grouped `BatchPlan<Command>` and returns either
committed `BatchEffects<Command, ComponentEffect>` or an error.

Domain rejection during lowering is not a frame/kernel rejection. It
returns `Reply::Accepted` with an operation-aborted outcome and the
typed domain reply carried in `SubReply::Failed.detail`.

Engine rejection is also not a frame/kernel rejection. The wire reply
is `Reply::Accepted` with a batch-aborted outcome, and the typed
engine error stays daemon-side through
`Executor::take_last_engine_error`.

## Observation

Observers receive:

```text
OperationReceived(operation)          before lowering
EffectEmitted(command_effect)         after atomic commit
```

`command_effect.sema_observation()` projects the local command/effect
pair into:

```rust
SemaObservation {
    operation: SemaOperation,
    outcome: SemaOutcome,
}
```

That projection is available when the local command implements
`ToSemaOperation` and the local effect implements `ToSemaOutcome`.

## Usage shape

```rust,ignore
use signal_executor::{
    BatchEffects, BatchPlan, CommandExecutor, Executor, Lowering,
    OperationEffects, OperationPlan,
};
use signal_frame::Request;

struct SpiritLowering;

impl Lowering for SpiritLowering {
    type Operation = SpiritOperation;
    type Reply = SpiritReply;
    type Command = SpiritCommand;
    type ComponentEffect = SpiritEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<OperationPlan<Self::Command>, Self::Reply> {
        // Map public contract operation to local executable command.
        todo!()
    }

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply {
        // Map committed local effects back to the contract reply.
        todo!()
    }
}

struct SpiritEngine;

impl CommandExecutor for SpiritEngine {
    type Command = SpiritCommand;
    type ComponentEffect = SpiritEffect;
    type Error = SpiritEngineError;

    fn execute_atomic_batch(
        &mut self,
        plan: BatchPlan<Self::Command>,
    ) -> Result<BatchEffects<Self::Command, Self::ComponentEffect>, Self::Error> {
        // Commit every command or no command.
        todo!()
    }
}

fn handle(
    executor: &mut Executor<SpiritLowering, SpiritEngine>,
    request: Request<SpiritOperation>,
) -> SpiritFrameReply {
    executor.execute(request)
}
```

The Counter mock in `tests/counter/mod.rs` is the fuller worked
example.
