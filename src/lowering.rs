//! Lowering trait and plan records.

use signal_frame::{NonEmpty, RequestPayload};

use crate::effect::SemaEffect;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlan<Command> {
    commands: NonEmpty<Command>,
}

impl<Command> OperationPlan<Command> {
    pub fn new(commands: NonEmpty<Command>) -> Self {
        Self { commands }
    }

    pub fn single(command: Command) -> Self {
        Self {
            commands: NonEmpty::single(command),
        }
    }

    pub fn commands(&self) -> &NonEmpty<Command> {
        &self.commands
    }

    pub fn into_commands(self) -> NonEmpty<Command> {
        self.commands
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPlan<Command> {
    operations: NonEmpty<OperationPlan<Command>>,
}

impl<Command> BatchPlan<Command> {
    pub fn new(operations: NonEmpty<OperationPlan<Command>>) -> Self {
        Self { operations }
    }

    pub fn single(operation: OperationPlan<Command>) -> Self {
        Self {
            operations: NonEmpty::single(operation),
        }
    }

    pub fn operations(&self) -> &NonEmpty<OperationPlan<Command>> {
        &self.operations
    }

    pub fn into_operations(self) -> NonEmpty<OperationPlan<Command>> {
        self.operations
    }
}

pub trait Lowering {
    type Operation: RequestPayload;
    type Reply;
    type Command;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<OperationPlan<Self::Command>, Self::Reply>;

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[SemaEffect],
    ) -> Self::Reply;
}
