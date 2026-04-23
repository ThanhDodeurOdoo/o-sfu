#![allow(
    dead_code,
    reason = "The verification model is only use by dedicated proof harnesses."
)]

mod invariants;
mod model;
#[cfg(kani)]
mod proofs;
#[cfg(kani)]
mod rtp_proofs;

pub(crate) use self::model::ProofRouterModel;
