pub(super) use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

pub(super) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
pub(super) use serde::de::DeserializeOwned;
pub(super) use tokio::sync::mpsc;
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
pub(super) use crate::{
    config::{Config, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags, TransportBackend},
    runtime::{
        RuntimeState,
        channel::{ChannelAdmissionPolicy, ChannelConfig, ChannelManager},
        metrics::RuntimeMetrics,
        transport_adapter::RuntimeTransportAdapter,
    },
    signaling::{
        auth::{self, HttpChannelClaims, HttpDisconnectClaims, RegisteredJwtClaims},
        http::{
            CHANNEL_PATH, ChannelResponse, CreateChannelQuery, DISCONNECT_PATH, NOOP_PATH,
            NoopResponse, STATS_PATH, StatsResponse,
        },
        shared::{SessionId, SessionPermissions},
    },
};

pub(super) const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

pub(super) fn test_config() -> Config {
    Config {
        auth_key: TEST_AUTH_KEY.to_owned(),
        bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
        authentication_timeout_ms: 10_000,
        channel_size: 100,
        session_timeout_ms: 10_000,
        ping_interval_ms: 60_000,
        feature_flags: RuntimeFeatureFlags::default(),
        codec_flags: MediaCodecFlags::default(),
        public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        rtc_port_range: RtcPortRange::new(40_000, 49_999),
        rtc_media_worker_count: 1,
        transport_backend: TransportBackend::Stub,
    }
}

pub(super) fn test_state() -> RuntimeState {
    let config = test_config();
    RuntimeState {
        channels: Arc::new(ChannelManager::for_test_with_admission_policy(
            ChannelAdmissionPolicy::new(config.channel_size),
        )),
        config,
        metrics: Arc::new(RuntimeMetrics::default()),
        transport_adapter: RuntimeTransportAdapter::builder().stub().build(),
    }
}

pub(super) fn signed_channel_claims(issuer: Option<&str>, key: Option<&str>) -> Option<String> {
    auth::sign(
        &HttpChannelClaims {
            registered: RegisteredJwtClaims {
                iss: issuer.map(str::to_owned),
                ..RegisteredJwtClaims::default()
            },
            key: key.map(str::to_owned),
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) fn signed_disconnect_claims(
    session_ids_by_channel: BTreeMap<String, Vec<SessionId>>,
) -> Option<String> {
    auth::sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            session_ids_by_channel,
        },
        TEST_AUTH_KEY,
    )
    .ok()
}

pub(super) fn build_request(builder: HttpRequestBuilder, body: Body) -> Option<Request<Body>> {
    builder.body(body).ok()
}

pub(super) async fn parse_json<T>(response: AxumResponse) -> Option<T>
where
    T: DeserializeOwned,
{
    let bytes = to_bytes(response.into_body(), usize::MAX).await.ok()?;
    serde_json::from_slice::<T>(&bytes).ok()
}
