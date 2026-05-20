//! SemaEngine: atomic commit point for Sema operations.

use crate::effect::SemaEffect;
use signal_sema::SemaOperation;

pub trait SemaEngine {
    type Error;
    fn execute_atomic(&mut self, ops: Vec<SemaOperation>) -> Result<Vec<SemaEffect>, Self::Error>;
}
