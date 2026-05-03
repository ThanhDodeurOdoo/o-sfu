pub mod config;
pub use o_sfu_core as core;
pub(crate) mod application;
mod runtime;
pub(crate) mod time;

/// Runtime authentication claims and JWT signing/verification helpers.
pub mod auth {
    pub use crate::runtime::auth::{
        AuthenticationError, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims,
        WebSocketConnectClaims, sign, verify,
    };
}

/// Public HTTP control-plane contract and request-origin helpers.
pub mod http {
    pub use crate::runtime::{
        http_server::contract::{
            CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
            DISCONNECT_PATH, IncomingBitRateStatsResponse, METRICS_PATH, NOOP_PATH, NoopResponse,
            RoomResponse, STATS_PATH, StatsResponse,
        },
        request_origin::{RequestOrigin, resolve_request_origin},
    };
}

/// WebSocket ingress parsing helpers used by verification and fuzz targets.
pub mod websocket {
    pub use crate::runtime::websocket_server::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_auth_payload_text, decode_client_batch,
    };
}

pub use self::runtime::{Runtime, run};
