//! Narrow helper surface for integration tests and fuzz targets.
//!
//! This module stays doc-hidden so runtime internals can keep moving without
//! accidentally becoming a stable crate API.

pub mod auth {
    pub use crate::runtime::auth::*;
}

pub mod client_batch {
    pub use crate::runtime::websocket_server::io::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_client_batch,
    };
}

pub mod http {
    pub use crate::runtime::http_server::contract::*;
}
