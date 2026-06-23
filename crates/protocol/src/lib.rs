//! browser and native signaling protocol for `o-sfu`
//!
//! `o-sfu-protocol` keeps client-side protocol state pure
//! the core state machine accepts host events and returns ordered command
//! batches that the embedding host executes through its own WebSocket,
//! `RTCPeerConnection` and timer APIs
//!
//! the crate has 3 public facades:
//!
//! - `host` is the state-machine surface for browser, native and test hosts
//! - `wire` contains JSON envelope and signaling payload types
//! - `bundle` preserves the browser bundle compatibility API used by Odoo
//!
//! ```no_run
//! use o_sfu_protocol::host::{Command, ProtocolCore};
//!
//! let mut core = ProtocolCore::new();
//! let commands = core.connect(
//!     "wss://sfu.example.test/ws",
//!     "signed-admission-token",
//!     Some("discuss-room".to_owned()),
//! );
//!
//! assert!(commands
//!     .iter()
//!     .any(|command| matches!(command, Command::Connect { .. })));
//! ```
//!
//! hosts should execute every command in order and then feed observed socket,
//! timer and peer-connection results back into `ProtocolCore`

mod bundle_api;
mod core;
mod host_bridge;
mod shared;
mod signaling;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

/// host-owned protocol state machine and side-effect commands
///
/// hosts drive `ProtocolCore` by reporting WebSocket, timer and peer-connection
/// events
/// every transition returns `CommandBatch`, so side effects stay explicit and
/// ordered at the host boundary
pub mod host {
    pub use crate::{core::*, host_bridge::*};
}

/// JSON wire envelopes and signaling payloads
///
/// this facade contains the serialized protocol contract exchanged over the
/// WebSocket
/// browser and native hosts should share these types instead of duplicating
/// envelope names or request payload shapes
pub mod wire {
    pub use crate::{shared::*, signaling::*};
}

/// browser bundle compatibility API
pub mod bundle {
    pub use crate::{bundle_api::*, shared::*};
}
