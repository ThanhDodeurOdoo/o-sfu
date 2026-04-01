#![allow(
    dead_code,
    reason = "The verification model is only exercised by dedicated proof harnesses."
)]

mod invariants;
mod model;
#[cfg(kani)]
mod proofs;

pub(crate) use self::model::ProofRouterModel;
