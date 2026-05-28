//! curated `host`, `wire` and `bundle` facade for `o-sfu-protocol`

mod bundle_api;
mod core;
mod host_bridge;
pub mod manifest;
mod shared;
mod signaling;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

/// browser host state-machine API
pub mod host {
    pub use crate::{core::*, host_bridge::*};
}

/// native websocket protocol API
pub mod wire {
    pub use crate::{shared::*, signaling::*};
}

/// browser bundle compatibility API for Odoo
pub mod bundle {
    pub use crate::{bundle_api::*, shared::*};
}
