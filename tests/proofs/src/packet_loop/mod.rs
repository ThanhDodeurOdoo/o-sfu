//! direct Kani harnesses over production packet-loop verification seams
//!
//! this module intentionally avoids a shadow packet-loop model. A harness stays
//! here only when it can call production packet-loop state or helpers directly

#[cfg(all(kani, feature = "packet-loop-proofs"))]
mod proofs;
