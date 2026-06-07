# signal-executor Intent

`signal-executor` is the shared executor library for triad daemons.
It translates accepted Signal operations into component-local commands,
executes those commands atomically through a daemon-owned backend, maps
the committed effects back into per-operation replies, and publishes
observer facts after commit.

It is not a daemon, not a signal contract, and not a database engine.
It owns no socket protocol, actor supervision, redb/sema store, or
component-domain payload vocabulary. Daemons supply the local command
and effect nouns through `Lowering` and `CommandExecutor`.

The public Signal contract vocabulary stays component-owned. Workspace
Sema words are available here only as daemon-side payloadless
classification helpers (`ToSemaOperation`, `ToSemaOutcome`,
`SemaObservation`) for internal projection. Public observer events
should project into contract-owned event nouns and outcomes, not expose
`SemaObservation` as the wire payload.

The crate follows the current schema-free `signal-frame` stack directly.
It must not depend on the retired `schema`, `schema-rust`, or
`signal-core` surfaces.
