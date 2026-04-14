//! Hidden integration-test access to the preserved current-wire compatibility catalog.
//!
//! Production callers should use the native signaling protocol. This module exists only so
//! integration tests can keep pinning the legacy stub-bus path to the preserved Odoo wire shape
//! while Phase 9 deletion work is still in progress.

pub mod current_bus {
    pub use crate::signaling::current_bus::*;
}

pub mod current_protocol {
    pub use crate::signaling::current_protocol::*;
}
