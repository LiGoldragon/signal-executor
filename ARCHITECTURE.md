## signal-executor Architecture

`signal-executor` owns the shared library a triad daemon uses to
translate its public contract operations into Sema operations,
commit them atomically through a `SemaEngine`, correlate each
Sema effect back to a per-operation reply, and publish operation /
effect events to subscribed observers.

It is below the daemon and above `signal-sema`. The daemon supplies
two impls: a `Lowering` over its contract operation enum, and a
`SemaEngine` over its concrete database backend. The executor
composes those two impls into a uniform `execute(Request) ->
ExecutorOutcome` pipeline.

The design that introduced this crate is documented in the primary
workspace at
`reports/designer/243-reply-naming-observer-hook-executor-trait.md`
§3. The broader migration spec lives at
`reports/designer/241-signal-architecture-migration-guide.md`.

## Constraints

- `signal-executor` is a Rust library crate.
- `signal-executor` contains no daemon, actor, socket, redb, or
  runtime code.
- `signal-executor` contains no Persona-specific, Criome-specific,
  or component-specific payload records. Every contract-level
  vocabulary is supplied by the daemon's `Lowering` impl.
- `signal-executor` depends on `signal-frame` (for `Request`,
  `Reply`, `SubReply`, `AcceptedOutcome`, `NonEmpty`,
  `RequestRejectionReason`, `RequestPayload`) and on `signal-sema`
  (for `SemaOperation`). It does not depend on `sema-engine` the
  engine; daemons reach their actual engine through the
  `SemaEngine` trait this crate defines.
- The crate is synchronous. Daemons that drive the executor wire it
  into whatever async runtime they already use.
- Public types and methods carry typed errors via `thiserror` per
  `~/primary/skills/rust/errors.md`.
- Type names inside the crate do not restate the `Executor` or
  `Signal` namespace; the domain is implicit.

## Public surface

| Item | Shape | Use |
|---|---|---|
| `Lowering` | trait | per-daemon contract-to-Sema bridge; three associated types (`Operation`, `Reply`, `RejectionReason`) and two methods (`lower`, `reply_from_effects`). |
| `SemaEngine` | trait | atomic commit point with one associated type (`Error`) and one method (`execute_atomic`). |
| `Executor<L, S>` | struct | composes `L: Lowering` and `S: SemaEngine` over a shared `ObserverSet`; exposes `execute(Request<L::Operation>) -> ExecutorOutcome<L, S>`. |
| `ExecutorOutcome<L, S>` | enum | three terminal variants: `Accepted { reply, effects }`, `LoweringRejected { reply, reason }`, `EngineRejected { reply, error }`. Borrow `reply()` for the wire shape; `is_accepted()` / `is_rejected()` for branching. |
| `SemaEffect` | struct | what happened after a `SemaOperation` committed: `operation` plus `outcome: SemaEffectOutcome`. |
| `SemaEffectOutcome` | enum | closed variant set keyed off operation class: `Wrote { rows_written, rows_matched }`, `Read { rows_read }`, `Stream { subscription_token }`, `Validated { predicate_held }`. |
| `ObserverChannel<Operation>` | trait | per-channel publish surface the executor calls. Two methods: `publish_operation_received(&Operation)` and `publish_sema_effect_emitted(&SemaEffect)`. |
| `ObserverSet<Operation>` | struct | concrete observer bookkeeping wrapping an `ObserverChannel` in `Arc`; clones share the underlying channel. `ObserverSet::no_op()` for daemons that have not yet wired observation. |
| `RecordingChannel<Operation>` | struct | test-only `ObserverChannel` impl recording events into an internal log for assertion. |
| `RecordedEvent<Operation>` | enum | `OperationReceived(Operation)` / `SemaEffectEmitted(SemaEffect)`. |
| `Error` | enum | crate-boundary structural errors (currently one variant, `EmptyRequest`; reserved for input shape checks). |

## Atomicity contract

`SemaEngine::execute_atomic` is the **single binding point** for
state effects. The trait guarantees:

- either every operation in the input `Vec<SemaOperation>` commits,
  in which case the engine returns one `SemaEffect` per operation
  in request order, or
- no operation commits, and the engine returns `Err(Self::Error)`.

There is no partial-commit return shape. Daemons that need
partial-commit semantics split the work across separate
`Executor::execute` calls -- atomicity is structural per call.

The executor does not retry, does not rollback, does not compensate.
It is the engine's job to enforce all-or-nothing. The executor's
contribution is structural: by funnelling every operation through
one `execute_atomic` call, it makes "all" or "none" the only two
states the caller sees.

When `execute_atomic` returns `Err`, the wire `Reply` is
`Reply::Rejected { reason: RequestRejectionReason::Internal }`. The
engine's typed error is carried separately on the daemon side via
`ExecutorOutcome::EngineRejected { error, .. }`.

## Failure-mode taxonomy

Three terminal paths, one terminal variant per path:

```mermaid
flowchart TD
    request["Request&lt;L::Operation&gt;"]
    lower["lower() for each op"]
    engine["execute_atomic(ops)"]
    map["reply_from_effects() for each op"]

    accepted["ExecutorOutcome::Accepted<br/>{ reply, effects }"]
    lrej["ExecutorOutcome::LoweringRejected<br/>{ reply, reason }"]
    erej["ExecutorOutcome::EngineRejected<br/>{ reply, error }"]

    request --> lower
    lower -- Ok --> engine
    lower -- Err --> lrej
    engine -- Ok --> map
    engine -- Err --> erej
    map --> accepted
```

| Path | When | Wire `Reply` | Daemon-side carry |
|---|---|---|---|
| `Accepted` | Every operation lowered and the engine committed atomically. | `Reply::Accepted { outcome: Completed, per_operation: NonEmpty<SubReply::Ok { payload }> }`. | `effects: Vec<SemaEffect>` for post-execution use (logs, metrics, derived events). |
| `LoweringRejected` | A `Lowering::lower` call returned `Err`. The engine was not called; no state effect occurred. | `Reply::Rejected { reason: RequestRejectionReason::Internal }`. | `reason: L::RejectionReason` -- the daemon's typed domain rejection. |
| `EngineRejected` | `SemaEngine::execute_atomic` returned `Err`. No state effect committed (atomicity contract). | `Reply::Rejected { reason: RequestRejectionReason::Internal }`. | `error: S::Error` -- the engine's typed failure cause. |

Post-commit publication failures (the observer set's
`publish_*` methods) do **not** roll back state. By the time the
executor reaches the publication step, the engine has already
committed. A panicking observer is a daemon-side bug; the wire
reply is unaffected.

## Reply correlation

`Lowering::reply_from_effects` is called once per operation, in
request order, with the full effects slice. The impl is responsible
for selecting which effects correspond to which operation -- typically
by counting forward through the slice in the order `lower` produced
operations.

The executor preserves request order:

1. The input `Request<L::Operation>` carries `NonEmpty<L::Operation>`.
   The executor iterates `payloads()` in request order to call
   `lower` and `reply_from_effects` -- never re-orders.
2. The output `Reply::Accepted` carries
   `NonEmpty<SubReply<L::Reply>>`. The executor builds it from the
   `(head_op, tail_ops)` split of the input non-empty, preserving
   non-emptiness without re-checking shape.

A `Lowering` impl that produces an empty `Vec` for one operation
(legitimate for validation-only or no-op operations) still
participates in `reply_from_effects` -- it just consults the
effects slice without claiming any of it.

## Observer publication ordering

The executor publishes in fixed order:

1. **OperationReceived** fires for every payload in the request,
   in request order, **before** any `lower` call.
2. **SemaEffectEmitted** fires for every effect, in commit order,
   **after** the engine returns successfully.

This is a hard ordering. Tests in `tests/round_trip.rs` witness it
via a `RecordingChannel`.

On lowering rejection, every operation up to (and including) the
rejecting one is observed under `OperationReceived`, but no
`SemaEffectEmitted` fires because no effect was committed.

On engine rejection, every operation is observed, but again no
`SemaEffectEmitted` fires because no effect was committed.

```mermaid
flowchart LR
    op1["OperationReceived(op_1)"]
    op2["OperationReceived(op_2)"]
    opN["OperationReceived(op_N)"]
    e1["SemaEffectEmitted(effect_1)"]
    eM["SemaEffectEmitted(effect_M)"]

    op1 --> op2 --> opN --> e1 --> eM
```

Where:
- N is the number of operations in the request.
- M is the number of effects the engine emitted (commit order).
- N and M may differ -- one operation may lower to many Sema
  operations, or to zero.

## Macro coordination

A parallel work-stream extends `signal-frame`'s `signal_channel!`
macro with an `observable` block that injects publish surfaces and
a subscription stream on the channel. When that work lands, the
macro will emit per-channel functions roughly shaped like:

```rust
fn publish_operation_received(&self, operation: &ChannelOperation);
fn publish_sema_effect_emitted(&self, effect: &SemaEffect);
```

`signal-executor`'s `ObserverChannel<Operation>` trait is designed
so that daemons can adapt the macro's emitted functions to the
trait by writing a thin newtype adapter (one trait impl per
channel). The exact shape of the macro's emitted code will be
reconciled in a follow-up once the macro work lands; today's
`ObserverChannel` trait is the executor-facing half of that
boundary.

## Boundary

```mermaid
flowchart TB
    daemon["component daemon"]
    contract["public component contract"]
    executor["signal-executor<br/>Lowering + SemaEngine + Executor"]
    sema["signal-sema<br/>SemaOperation"]
    engine["sema-engine<br/>registered record execution"]

    daemon -- "operates on" --> executor
    daemon -- "supplies impl" --> executor
    contract -- "defines vocabulary" --> daemon
    executor --> sema
    daemon --> engine
    engine -- "is reached through" --> executor
```

`signal-executor` does not see `sema-engine` directly; the daemon
implements `SemaEngine` over its concrete engine instance and
hands the impl to the executor.

## Non-goals

- No public component operation vocabulary.
- No request/reply frame mechanics (those live in `signal-frame`).
- No Sema operation vocabulary or pattern primitives (those live
  in `signal-sema`).
- No `sema-engine` integration. Daemons bridge their engine into
  the `SemaEngine` trait themselves.
- No authentication, routing, or daemon-side policy.
- No async runtime, actor supervision, socket plumbing, or redb
  table management.
- No retry, backoff, or compensation logic. Atomicity is the
  engine's contract; the executor is the structural binding point.

## Code map

```text
src/lib.rs       module entry and re-exports
src/effect.rs    SemaEffect and SemaEffectOutcome with witness predicate
src/engine.rs    SemaEngine trait (associated Error type, execute_atomic)
src/error.rs     crate-boundary Error enum (reserved input-shape variants)
src/executor.rs  Executor struct and ExecutorOutcome enum
src/lowering.rs  Lowering trait (Operation, Reply, RejectionReason)
src/observer.rs  ObserverChannel trait, ObserverSet struct,
                 RecordingChannel + RecordedEvent for tests

tests/counter/mod.rs  Counter mock: operation/reply/rejection enums,
                      Lowering impl, SemaEngine impl (canned effects
                      plus poisoned variant for engine-rejection
                      tests)
tests/effect.rs       Unit tests for SemaEffect::is_write_commit
                      across all operation classes and outcomes
tests/observer.rs     Unit tests for ObserverSet, RecordingChannel,
                      RecordedEvent ordering, and Arc-shared channels
tests/round_trip.rs   End-to-end tests: single-op accepted, multi-op
                      accepted, lowering rejection (engine never
                      called), engine rejection (no state visible),
                      observer publication ordering, empty-lowering,
                      ExecutorOutcome::reply accessor
```

## See also

- `~/primary/reports/designer/243-reply-naming-observer-hook-executor-trait.md`
  -- the design this crate implements.
- `~/primary/reports/designer/241-signal-architecture-migration-guide.md`
  -- the broader migration spec.
- `~/primary/reports/designer/242-hole-finding-spirit-migration.md`
  -- the motivating hole (hole 5).
- `/git/github.com/LiGoldragon/signal-frame/ARCHITECTURE.md`
  -- the frame mechanics this crate consumes (`Request`, `Reply`,
  `NonEmpty`, `RequestPayload`).
- `/git/github.com/LiGoldragon/signal-sema/ARCHITECTURE.md`
  -- the Sema operation vocabulary this crate composes with.
