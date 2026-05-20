//! Unit tests for `CommandEffect`.

use signal_executor::CommandEffect;
use signal_sema::{SemaObservation, SemaOperation, SemaOutcome, ToSemaOperation, ToSemaOutcome};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExampleCommand {
    Write,
    Read,
}

impl ToSemaOperation for ExampleCommand {
    fn to_sema_operation(&self) -> SemaOperation {
        match self {
            Self::Write => SemaOperation::Assert,
            Self::Read => SemaOperation::Match,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExampleEffect {
    Wrote,
    ReadRows,
}

impl ToSemaOutcome for ExampleEffect {
    fn to_sema_outcome(&self) -> SemaOutcome {
        match self {
            Self::Wrote => SemaOutcome::Asserted,
            Self::ReadRows => SemaOutcome::Matched,
        }
    }
}

#[test]
fn command_effect_projects_write_to_payloadless_sema_observation() {
    let effect = CommandEffect::new(ExampleCommand::Write, ExampleEffect::Wrote);

    assert_eq!(
        effect.sema_observation(),
        SemaObservation::new(SemaOperation::Assert, SemaOutcome::Asserted),
    );
}

#[test]
fn command_effect_projects_read_to_payloadless_sema_observation() {
    let effect = CommandEffect::new(ExampleCommand::Read, ExampleEffect::ReadRows);

    assert_eq!(
        effect.sema_observation(),
        SemaObservation::new(SemaOperation::Match, SemaOutcome::Matched),
    );
}

#[test]
fn command_effect_keeps_component_local_payloads_available() {
    let effect = CommandEffect::new(ExampleCommand::Write, ExampleEffect::Wrote);

    assert_eq!(effect.command(), &ExampleCommand::Write);
    assert_eq!(effect.effect(), &ExampleEffect::Wrote);
    assert_eq!(
        effect.into_parts(),
        (ExampleCommand::Write, ExampleEffect::Wrote),
    );
}
