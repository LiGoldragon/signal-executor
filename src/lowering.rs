//! Lowering trait per /246 §1.

use signal_frame::RequestPayload;
use signal_sema::SemaOperation;

use crate::effect::SemaEffect;

pub trait Lowering {
    type Operation: RequestPayload;
    type Reply;

    fn lower(&self, operation: &Self::Operation) -> Result<Vec<SemaOperation>, Self::Reply>;

    fn reply_from_effects(
        &self,
        operation: &Self::Operation,
        effects: &[SemaEffect],
    ) -> Self::Reply;
}
