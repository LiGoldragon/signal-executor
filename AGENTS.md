## signal-executor - Agent Instructions

Read `/home/li/primary/AGENTS.md` first.

This repository is a library crate. It provides the shared executor
machinery a triad daemon uses to translate its public contract
operations into component-local executable commands, with
atomicity, per-operation reply mapping, and observer publication.

It is not a daemon, not an actor runtime, and not a component
contract; it has no public wire surface of its own.

## Required Local Reading

1. `ARCHITECTURE.md`
2. `skills.md`
3. `/home/li/primary/skills/rust-discipline.md`
4. `/home/li/primary/skills/rust/errors.md`
5. `/home/li/primary/skills/rust/methods.md`
6. `/home/li/primary/skills/contract-repo.md`
7. `/home/li/primary/skills/naming.md`
8. `/home/li/primary/skills/nix-discipline.md`
9. `/home/li/primary/skills/jj.md`

## Local Rules

- Keep this crate library-only.
- Keep component-domain payloads out; everything is generic over
  `Lowering::Operation`, `Lowering::Reply`, and
  `Lowering::Command`.
- Do not depend on `sema-engine` directly. The actual engine that
  commits executable commands is reached through the `CommandExecutor`
  trait, which daemons implement when wiring their backend.
- Add a test for every public type and trait method. Round-trip
  the orchestration through the mock `Lowering` + mock
  `CommandExecutor`; do not prove behavior by reading source.
- No flags on any binary that may grow under `examples/`. NOTA in,
  NOTA out.
