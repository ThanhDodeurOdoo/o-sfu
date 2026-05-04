#![allow(
    dead_code,
    reason = "The verification model is only use by dedicated proof harnesses."
)]

#[cfg(test)]
mod drift_tests;
mod invariants;
mod model;
#[cfg(kani)]
mod proofs;

pub(crate) use self::model::ProofRouterModel;
