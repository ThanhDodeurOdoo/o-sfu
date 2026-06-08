//! public test-support surface for router crate users
//!
//! normal tests get detached snapshots plus the full router invariant predicate
//! proof-only storage predicates stay under `proof` so non-Kani callers do not
//! couple themselves to internal map layout

pub mod rtp_samples;

pub use crate::model::test_support::{
    RelationSnapshot, RouterStateSnapshot, router_satisfies_invariants, router_state_snapshot,
};

#[cfg(kani)]
pub mod proof {
    pub use crate::model::test_support::proof::*;
}
