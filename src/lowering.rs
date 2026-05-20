//! Lowering trait and plan records.

use signal_frame::{NonEmpty, RequestPayload};
use signal_sema::{SemaObservation, ToSemaOperation, ToSemaOutcome};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandEffect<Command, ComponentEffect> {
    command: Command,
    effect: ComponentEffect,
}

impl<Command, ComponentEffect> CommandEffect<Command, ComponentEffect> {
    pub fn new(command: Command, effect: ComponentEffect) -> Self {
        Self { command, effect }
    }

    pub fn command(&self) -> &Command {
        &self.command
    }

    pub fn effect(&self) -> &ComponentEffect {
        &self.effect
    }

    pub fn into_parts(self) -> (Command, ComponentEffect) {
        (self.command, self.effect)
    }

    pub fn sema_observation(&self) -> SemaObservation
    where
        Command: ToSemaOperation,
        ComponentEffect: ToSemaOutcome,
    {
        SemaObservation::from_projection(&self.command, &self.effect)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEffects<Command, ComponentEffect> {
    command_effects: NonEmpty<CommandEffect<Command, ComponentEffect>>,
}

impl<Command, ComponentEffect> OperationEffects<Command, ComponentEffect> {
    pub fn new(command_effects: NonEmpty<CommandEffect<Command, ComponentEffect>>) -> Self {
        Self { command_effects }
    }

    pub fn single(command: Command, effect: ComponentEffect) -> Self {
        Self {
            command_effects: NonEmpty::single(CommandEffect::new(command, effect)),
        }
    }

    pub fn command_effects(&self) -> &NonEmpty<CommandEffect<Command, ComponentEffect>> {
        &self.command_effects
    }

    pub fn component_effects(&self) -> impl Iterator<Item = &ComponentEffect> {
        self.command_effects.iter().map(CommandEffect::effect)
    }

    pub fn into_command_effects(self) -> NonEmpty<CommandEffect<Command, ComponentEffect>> {
        self.command_effects
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEffects<Command, ComponentEffect> {
    operations: NonEmpty<OperationEffects<Command, ComponentEffect>>,
}

impl<Command, ComponentEffect> BatchEffects<Command, ComponentEffect> {
    pub fn new(operations: NonEmpty<OperationEffects<Command, ComponentEffect>>) -> Self {
        Self { operations }
    }

    pub fn single(operation: OperationEffects<Command, ComponentEffect>) -> Self {
        Self {
            operations: NonEmpty::single(operation),
        }
    }

    pub fn from_head_and_tail(
        head: OperationEffects<Command, ComponentEffect>,
        tail: Vec<OperationEffects<Command, ComponentEffect>>,
    ) -> Self {
        Self {
            operations: NonEmpty::from_head_and_tail(head, tail),
        }
    }

    pub fn operations(&self) -> &NonEmpty<OperationEffects<Command, ComponentEffect>> {
        &self.operations
    }

    pub fn into_operations(self) -> NonEmpty<OperationEffects<Command, ComponentEffect>> {
        self.operations
    }
}

pub trait Lowering {
    type Operation: RequestPayload;
    type Reply;
    type Command;
    type ComponentEffect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<OperationPlan<Self::Command>, Self::Reply>;

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &OperationEffects<Self::Command, Self::ComponentEffect>,
    ) -> Self::Reply;
}
