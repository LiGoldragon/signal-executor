## signal-executor Architecture

`signal-executor` is the shared library a component daemon uses to
execute one `signal-frame::Request<Operation>` without making the
wire contract, local database code, and observer machinery leak into
each other.

The current design is the three-layer model:

```text
contract Operation  ->  component Command  ->  Sema observation class
external vocabulary     executable payload      payloadless classification
```

The executor never executes `SemaOperation`. `SemaOperation` and
`SemaOutcome` are payloadless observation classifications from
`signal-sema`. Execution is component-local: each daemon defines its
own `Command` and `ComponentEffect` records.

## Constraints

- `signal-executor` is a Rust library crate.
- `signal-executor` contains no daemon, actor, socket, redb, or
  runtime code.
- `signal-executor` contains no Persona-specific, Criome-specific,
  or component-specific payload records.
- `signal-executor` depends on `signal-frame` for request/reply,
  non-empty batch, observer, and macro-adjacent types.
- `signal-executor` depends on `signal-sema` only for payloadless
  classification traits and records: `ToSemaOperation`,
  `ToSemaOutcome`, and `SemaObservation`.
- `signal-executor` does not depend on `sema-engine`.
- `signal-executor` does not own `SemaOperation` payloads.
- `signal-executor` does not expose a `SemaEffect` type.
- Engine failures are represented on the wire as an accepted
  batch-abort reply, not as `Reply::Rejected`.
- Engine failures classify themselves through
  `BatchErrorClassification`; the wire reply carries the failure
  reason, retry classification, and commit status supplied by the
  typed engine error.
- The typed engine error is retained daemon-side through
  `Executor::take_last_engine_error`.
- Observer publication never rolls back committed state.

## Public Surface

| Item | Shape | Purpose |
|---|---|---|
| `Lowering` | trait | Daemon-owned bridge from public contract `Operation` to local executable `Command`, and from committed `OperationEffects` to contract `Reply`. |
| `OperationPlan<Command>` | struct | Non-empty command plan for one source operation. |
| `BatchPlan<Command>` | struct | Non-empty batch of operation plans; preserves source-operation grouping. |
| `BatchErrorClassification` | trait | Converts a component executor error into wire-safe batch-abort metadata without carrying the typed error on the wire. |
| `CommandExecutor` | trait | Component-local atomic commit point over `BatchPlan<Command>`; its error type implements `BatchErrorClassification`. |
| `CommandEffect<Command, ComponentEffect>` | struct | One executed local command plus the local effect it produced. |
| `OperationEffects<Command, ComponentEffect>` | struct | Non-empty command effects for one source operation. |
| `BatchEffects<Command, ComponentEffect>` | struct | Non-empty operation effects for the whole request. |
| `Executor<Lowering, CommandExecutor>` | struct | Executes a frame request by composing lowering, atomic command execution, reply correlation, and observation. |
| `ObserverChannel<Operation, Effect>` | trait | Executor-facing publication surface. In normal use `Effect` is `CommandEffect<Command, ComponentEffect>`. |
| `ObserverSet<Operation, Effect>` | struct | Shared observer handle used by the executor. |
| `FrameObserverBridge` | struct | Connects raw executor facts to macro-generated observable streams through `ObservationProjection`. |

There is intentionally no `SemaEngine` alias. The old name made Sema
sound executable. The executable surface is `CommandExecutor`.

## Execution Flow

```text
Request<Operation>
  |
  | publish OperationReceived for every operation
  v
Lowering::lower(operation) -> OperationPlan<Command>
  |
  | all source operations lower successfully
  v
CommandExecutor::execute_atomic_batch(BatchPlan<Command>)
  |
  | committed
  v
BatchEffects<Command, ComponentEffect>
  |
  | publish CommandEffect events in commit order
  v
Lowering::reply_from_effects(operation, effects)
  |
  v
Reply<ContractReply>
```

`OperationPlan<Command>` is the source-operation boundary. A single
contract operation may lower to many component-local commands, but
the commands remain grouped under the operation that produced them.
That grouping is why the executor does not need a sidecar
`source_index`.

## Atomicity

`CommandExecutor::execute_atomic_batch` is the only state-changing
call the executor makes. It receives a `BatchPlan<Command>` and
returns either:

- `Ok(BatchEffects<Command, ComponentEffect>)`: every command
  committed, and the result still preserves source-operation
  grouping; or
- `Err(Error)`: no command committed.

The executor does not retry, compensate, or roll back. The component
executor is responsible for all-or-nothing behavior. The shared
executor makes that contract visible by having no partial-commit
return shape.

## Failure Modes

| Path | When | Wire reply | Side channel |
|---|---|---|---|
| Committed | Every operation lowered and the command executor committed atomically. | `Reply::Accepted { outcome: Committed, per_operation: Ok(...) }` | Observer events are published after commit. |
| Domain rejection | `Lowering::lower` rejected one source operation. | `Reply::Accepted { outcome: OperationAborted { failed_at, reason: DomainRejection }, ... }` | No command executor call; no effect events. |
| Engine rejection | `CommandExecutor::execute_atomic_batch` returned `Err`. | `Reply::Accepted { outcome: BatchAborted { reason, retry, commit }, per_operation: Invalidated... }` using the engine error's `BatchErrorClassification`. | Typed engine error is stored in `Executor::take_last_engine_error`. |

`Reply::Rejected` remains a kernel/frame rejection shape. It is not
used for component executor failure because the frame was accepted and
the failure happened after execution planning began.

## Observation

The executor publishes two moments:

```text
OperationReceived(operation)          before lowering
EffectEmitted(command_effect)         after atomic commit
```

The effect is not a Sema effect. It is the component-local pair:

```rust
CommandEffect<Command, ComponentEffect>
```

Generic observers that need the workspace-wide classification call:

```rust
command_effect.sema_observation()
```

That method is available when:

```rust
Command: ToSemaOperation
ComponentEffect: ToSemaOutcome
```

The result is:

```rust
SemaObservation {
    operation: SemaOperation,
    outcome: SemaOutcome,
}
```

The daemon can still publish richer component-specific event records
through `FrameObserverBridge` and `ObservationProjection`.

## Observer Bridge

The crate boundary is intentionally split:

```text
signal-executor
  raw facts:
    Operation
    CommandEffect<Command, ComponentEffect>

signal-frame macro output
  observable stream:
    OperationEvent
    EffectEvent

daemon projection
  ObservationProjection:
    Operation -> OperationEvent
    CommandEffect<Command, ComponentEffect> -> EffectEvent
```

`FrameObserverBridge` composes:

- an `ObservationProjection`,
- a macro-generated or daemon-owned `ObservableSet`, and
- an `ObserverDelivery` callback that writes projected events to
  subscribers.

This avoids a dependency inversion. `signal-frame` never depends on
`signal-executor`, and `signal-executor` never knows a component's
stream records.

## Non-Goals

- No public component operation vocabulary.
- No request/reply codec or socket protocol.
- No daemon lifecycle or actor supervision.
- No redb or Sema table execution.
- No authentication, routing, or policy.
- No retry or backoff strategy.
- No global executable database-command language.

## Code Map

```text
src/lib.rs       module entry and re-exports
src/engine.rs    CommandExecutor trait
src/error.rs     crate-boundary Error enum
src/executor.rs  Executor and failure mapping
src/lowering.rs  Lowering, OperationPlan, BatchPlan,
                 CommandEffect, OperationEffects, BatchEffects
src/observer.rs  ObserverChannel, ObserverSet, RecordingChannel,
                 RecordedEvent
src/bridge.rs    FrameObserverBridge and ObserverDelivery

tests/counter/mod.rs       Counter mock with operation, command,
                           effect, reply, lowering, and executor impls
tests/command_effect.rs    CommandEffect projection witnesses
tests/observer.rs          ObserverSet and RecordingChannel witnesses
tests/bridge_integration.rs FrameObserverBridge projection witness
tests/round_trip.rs        End-to-end acceptance, rejection, atomicity,
                           and observer-ordering witnesses
```

## See Also

- `/git/github.com/LiGoldragon/signal-frame/ARCHITECTURE.md`
  for request/reply and observable stream mechanics.
- `/git/github.com/LiGoldragon/signal-sema/ARCHITECTURE.md`
  for payloadless Sema classification.
