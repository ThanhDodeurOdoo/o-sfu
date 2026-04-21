//! This module is doc-hidden to avoid tests looking like a stable API

pub mod auth {
    pub use crate::runtime::auth::{
        HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims, WebSocketConnectClaims, sign,
        verify,
    };
}

pub mod client_batch {
    pub use crate::runtime::websocket_server::io::decode_client_batch;
}

pub mod http {
    pub use crate::runtime::http_server::contract::{
        CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH, IncomingBitRateStats,
        METRICS_PATH, STATS_PATH, StatsResponse,
    };
}

pub mod server {
    pub use crate::runtime::testing::{
        TestServer, decode_protocol_welcome_batch, spawn_test_server,
    };
}

pub mod concurrency {
    pub use crate::runtime::testing::{
        ActiveChannelRegistry, RelayTargetRegistry, SourcePolicyDirtyState, WorkerHandleSlot,
    };
}
