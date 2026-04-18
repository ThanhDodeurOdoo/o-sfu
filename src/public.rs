//! made public for tests
//! so that runtime can remain mostly private

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
