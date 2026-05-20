## signal-executor Skills

This repository follows the workspace skills in
`/home/li/primary/skills/`.

## Local Discipline

- Keep the crate small and trait-shaped. The two real types are
  the `Lowering` trait, the `CommandExecutor` trait, and the
  `Executor<Lowering, CommandExecutor>` struct that composes them.
- Do not add runtime dependencies (no `tokio`, no actor
  runtime). The executor is synchronous; daemon code wires it
  into whatever runtime the daemon already runs.
- Do not introduce component-domain names. Every contract-level
  vocabulary is supplied by the daemon's `Lowering` impl.
- Reply mapping flows through `Lowering::reply_from_effects` --
  per-operation, positional, deterministic. The lowering owns the
  canonical reply rule for multi-command operation plans; document
  whether it selects, aggregates, or treats the last command effect as
  canonical. Do not invent a channel-side reply assembler.
- Observer publication is best-effort and post-commit: failures
  in publication never roll back state effects. Document each
  publication's ordering with a test.
- Add a constraint test when adding an architectural rule.
