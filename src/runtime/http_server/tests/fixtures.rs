pub(super) use std::collections::BTreeMap;

pub(super) use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header, request::Builder as HttpRequestBuilder},
    response::Response as AxumResponse,
};
pub(super) use o_sfu_protocol::shared::UserId;
pub(super) use serde::de::DeserializeOwned;
pub(super) use tower::util::ServiceExt;

pub(super) use super::super::app;
pub(super) use crate::{
    config::Bitrate,
    runtime::{
        ConnectionId, RuntimeState,
        auth::{self, HttpDisconnectClaims, HttpRoomClaims, RegisteredJwtClaims},
        diagnostics::types::{
            DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSourceSelectionReason,
            DiagnosticsSummaryResponse, DiagnosticsUserDetail, DiagnosticsUserLookupConflict,
            DiagnosticsUserSummary, DiagnosticsWorkerSummary,
        },
        http_server::contract::{
            CHANNEL_PATH, CreateRoomQuery, DIAGNOSTICS_ROOMS_PATH, DIAGNOSTICS_SUMMARY_PATH,
            DIAGNOSTICS_WORKERS_PATH, DISCONNECT_PATH, METRICS_PATH, NOOP_PATH, NoopResponse,
            RoomResponse, STATS_PATH, StatsResponse,
        },
        media_transport::{MediaTransport, TransportPlacementPressureSnapshot},
        room::{JoinUserRequest, RoomConfig},
        test_support::{
            RuntimeMetricsSnapshotTestExt, RuntimeTestBuilder, RuntimeTestState, TEST_AUTH_KEY,
            test_outbound_sender,
        },
    },
};

pub(super) fn test_state() -> RuntimeState {
    RuntimeTestBuilder::new().build_runtime_state()
}

pub(super) fn test_state_with_handles() -> RuntimeTestState {
    RuntimeTestBuilder::new().build_state()
}

pub(super) fn signed_room_claims(issuer: Option<&str>, key: Option<&str>) -> Option<String> {
    auth::sign(
        &HttpRoomClaims {
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
    user_ids_by_room: BTreeMap<String, Vec<UserId>>,
) -> Option<String> {
    auth::sign(
        &HttpDisconnectClaims {
            registered: RegisteredJwtClaims::default(),
            user_ids_by_room,
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

pub(super) async fn parse_text(response: AxumResponse) -> Option<String> {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.ok()?;
    String::from_utf8(bytes.to_vec()).ok()
}
