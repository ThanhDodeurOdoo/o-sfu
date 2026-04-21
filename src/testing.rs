//! This module stays doc-hidden so runtime internals can keep moving without
//! accidentally becoming a stable crate API.

pub mod auth {
    pub use crate::runtime::auth::*;
}

pub mod client_batch {
    pub use crate::runtime::websocket_server::io::{
        decode_client_batch, ClientBatchDecodeError, ClientBatchDecodeFailureKind,
        MAX_CLIENT_BATCH_ENVELOPES, MAX_CLIENT_FRAME_BYTES,
    };
}

pub mod http {
    pub use crate::runtime::http_server::contract::*;
}
