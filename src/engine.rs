//! SemaEngine: atomic commit point for component-local commands.

use crate::effect::SemaEffect;

pub trait SemaEngine {
    type Command;
    type Error;
    fn execute_atomic(
        &mut self,
        commands: Vec<Self::Command>,
    ) -> Result<Vec<SemaEffect>, Self::Error>;
}
