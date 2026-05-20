//! CommandExecutor: atomic commit point for component-local commands.

pub trait CommandExecutor {
    type Command;
    type Effect;
    type Error;
    fn execute_atomic(
        &mut self,
        commands: Vec<Self::Command>,
    ) -> Result<Vec<Self::Effect>, Self::Error>;
}
