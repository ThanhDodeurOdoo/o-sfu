pub mod auth {
    pub use crate::runtime::auth::{
        AuthenticationError, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims,
        WebSocketConnectClaims, sign, verify,
    };
}

pub mod client_batch {
    pub use crate::runtime::websocket_server::io::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_client_batch,
    };
}

pub mod http {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use axum::http::{HeaderMap, HeaderValue, header};

    pub use crate::runtime::http_server::contract::{
        CHANNEL_PATH, CreateRoomQuery, DISCONNECT_PATH, IncomingBitRateStatsResponse, METRICS_PATH,
        RoomResponse, STATS_PATH, StatsResponse,
    };
    use crate::{
        config::{
            Config, DiagnosticsConfig, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
            TelemetryConfig,
        },
        runtime::{http_server::request_base_url, options::RuntimeOptions, resolve_remote_address},
    };

    fn testing_config(trust_proxy_headers: bool) -> Config {
        Config {
            auth_key: "dGVzdC1rZXk=".to_owned(),
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8070)),
            authentication_timeout_ms: 10_000,
            room_size: 100,
            user_timeout_ms: 10_000,
            ping_interval_ms: 60_000,
            trust_proxy_headers,
            feature_flags: RuntimeFeatureFlags::default(),
            codec_flags: MediaCodecFlags::default(),
            diagnostics: DiagnosticsConfig::default(),
            telemetry: TelemetryConfig::default(),
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_bitrate_in_bps: 8_000_000,
            max_bitrate_out_bps: 10_000_000,
            rtc_port_range: RtcPortRange::new(40_000, 49_999),
            rtc_media_worker_count: 1,
        }
    }

    fn insert_header(headers: &mut HeaderMap, name: &'static str, value: Option<&str>) {
        let Some(value) = value else {
            return;
        };
        let Ok(value) = HeaderValue::from_str(value) else {
            return;
        };
        headers.insert(name, value);
    }

    pub fn resolve_request_origin(
        host: Option<&str>,
        forwarded_host: Option<&str>,
        forwarded_proto: Option<&str>,
        forwarded_for: Option<&str>,
        trust_proxy_headers: bool,
        connect_info: Option<SocketAddr>,
    ) -> (String, String) {
        let mut headers = HeaderMap::new();
        insert_header(&mut headers, header::HOST.as_str(), host);
        insert_header(&mut headers, "x-forwarded-host", forwarded_host);
        insert_header(&mut headers, "x-forwarded-proto", forwarded_proto);
        insert_header(&mut headers, "x-forwarded-for", forwarded_for);

        let config = testing_config(trust_proxy_headers);
        let options = RuntimeOptions::from_config(&config);
        (
            request_base_url(&headers, &options.http),
            resolve_remote_address(&headers, options.http.trust_proxy_headers, connect_info),
        )
    }
}

pub mod websocket {
    pub use o_sfu_protocol::signaling::WebSocketCloseCode;

    use crate::runtime::websocket_server::decode_auth_payload_text;

    pub fn decode_auth_payload(payload: &str) -> Result<(), WebSocketCloseCode> {
        decode_auth_payload_text(payload).map(|_payload| ())
    }
}

pub mod rtc {
    use crate::runtime::client_rtp_capabilities_from_answer;

    #[must_use]
    pub fn project_answer_capabilities(answer_sdp: &str) -> bool {
        client_rtp_capabilities_from_answer(answer_sdp).is_some()
    }
}

pub mod server {
    pub use crate::runtime::testing::{
        TestServer, decode_protocol_welcome_batch, spawn_test_server,
    };
}

pub mod concurrency {
    pub use crate::runtime::testing::{
        ActiveRoomRegistry, RelayTargetRegistry, SourcePolicyDirtyState, WorkerHandleSlot,
    };
}

pub mod transport {
    pub use o_sfu_protocol::shared::UserId;

    pub use crate::runtime::{RemoteAddrDemux, TransportSessionKey, test_transport_session_key};
}
