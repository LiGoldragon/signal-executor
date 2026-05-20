//! Lowering trait and plan records.

use signal_frame::{NonEmpty, RequestPayload};

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
pub struct OperationEffects<Effect> {
    effects: Vec<Effect>,
}

impl<Effect> OperationEffects<Effect> {
    pub fn new(effects: Vec<Effect>) -> Self {
        Self { effects }
    }

    pub fn empty() -> Self {
        Self {
            effects: Vec::new(),
        }
    }

    pub fn single(effect: Effect) -> Self {
        Self {
            effects: vec![effect],
        }
    }

    pub fn effects(&self) -> &[Effect] {
        &self.effects
    }

    pub fn into_effects(self) -> Vec<Effect> {
        self.effects
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEffects<Effect> {
    operations: NonEmpty<OperationEffects<Effect>>,
}

impl<Effect> BatchEffects<Effect> {
    pub fn new(operations: NonEmpty<OperationEffects<Effect>>) -> Self {
        Self { operations }
    }

    pub fn single(operation: OperationEffects<Effect>) -> Self {
        Self {
            operations: NonEmpty::single(operation),
        }
    }

    pub fn from_head_and_tail(
        head: OperationEffects<Effect>,
        tail: Vec<OperationEffects<Effect>>,
    ) -> Self {
        Self {
            operations: NonEmpty::from_head_and_tail(head, tail),
        }
    }

    pub fn operations(&self) -> &NonEmpty<OperationEffects<Effect>> {
        &self.operations
    }

    pub fn into_operations(self) -> NonEmpty<OperationEffects<Effect>> {
        self.operations
    }
}

pub trait Lowering {
    type Operation: RequestPayload;
    type Reply;
    type Command;
    type Effect;

    fn lower(
        &self,
        operation: &Self::Operation,
    ) -> Result<OperationPlan<Self::Command>, Self::Reply>;

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[Self::Effect],
    ) -> Self::Reply;
}
