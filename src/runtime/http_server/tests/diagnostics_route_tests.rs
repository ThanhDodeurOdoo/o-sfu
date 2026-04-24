#![allow(
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::use_self,
    reason = "the diagnostics route tests favor direct assertion style and a single end-to-end scenario over helper indirection"
)]

use std::net::SocketAddr;

use super::fixtures::*;
use o_sfu_router::{MediaKind, MediaStream};

use crate::runtime::channel::Channel;
use crate::runtime::test_rtp_samples::{
    sample_client_rtp_capabilities, sample_video_rtp_parameters,
};
use o_sfu_protocol::shared::{DownloadStates, StreamType};

fn test_video_rtp_parameters(ssrc: u64) -> MediaStream {
    sample_video_rtp_parameters(None, u32::try_from(ssrc).unwrap_or(u32::MAX))
}

async fn publish_video_stream(
    channel: &Channel,
    session_id: &SessionId,
    stream_type: StreamType,
    ssrc: u64,
    transport_adapter: &RuntimeTransportAdapter,
) {
    let Some(connection_id) = channel
        .test_api()
        .inspect()
        .session_connection_id(session_id)
        .await
    else {
        panic!("session should exist before publishing");
    };
    assert!(
        channel
            .apply_session_negotiated(
                session_id,
                connection_id,
                sample_client_rtp_capabilities(),
                transport_adapter,
            )
            .await
    );
    assert!(
        channel
            .test_api()
            .media()
            .publish_track(
                session_id,
                stream_type,
                MediaKind::Video,
                test_video_rtp_parameters(ssrc),
                transport_adapter,
            )
            .await
            .is_some()
    );
}

#[tokio::test]
async fn diagnostics_routes_are_forbidden_without_token_on_public_listener() {
    let mut state = test_state();
    state.config.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    let request = build_request(Request::get(DIAGNOSTICS_SUMMARY_PATH), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(state).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn diagnostics_routes_require_the_configured_bearer_token() {
    let mut state = test_state();
    state.config.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    let unauthorized = build_request(Request::get(DIAGNOSTICS_SUMMARY_PATH), Body::empty());
    assert!(unauthorized.is_some());
    let Some(unauthorized) = unauthorized else {
        return;
    };
    let unauthorized_response = app(state.clone()).oneshot(unauthorized).await;
    assert!(unauthorized_response.is_ok());
    let Some(unauthorized_response) = unauthorized_response.ok() else {
        return;
    };
    assert_eq!(unauthorized_response.status(), StatusCode::UNAUTHORIZED);

    let authorized = build_request(
        Request::get(DIAGNOSTICS_SUMMARY_PATH)
            .header(header::AUTHORIZATION, "Bearer operator-secret"),
        Body::empty(),
    );
    assert!(authorized.is_some());
    let Some(authorized) = authorized else {
        return;
    };
    let authorized_response = app(state).oneshot(authorized).await;
    assert!(authorized_response.is_ok());
    let Some(authorized_response) = authorized_response.ok() else {
        return;
    };
    assert_eq!(authorized_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn diagnostics_routes_return_live_channel_and_session_details() {
    let state = test_state();
    let channel = state
        .channel_manager
        .serve_channel(
            "issuer-a",
            None,
            &ChannelConfig::default(),
            Some("203.0.113.10"),
        )
        .await;
    let (alice_tx, _alice_rx) = mpsc::unbounded_channel();
    let (bob_tx, _bob_rx) = mpsc::unbounded_channel();
    let alice_session_id = SessionId::Integer(1);
    let bob_session_id = SessionId::Integer(2);
    let alice_join = channel
        .test_api()
        .lifecycle()
        .join_session(
            alice_session_id.clone(),
            None,
            SessionPermissions::default(),
            alice_tx,
        )
        .await;
    let bob_join = channel
        .test_api()
        .lifecycle()
        .join_session(
            bob_session_id.clone(),
            None,
            SessionPermissions::default(),
            bob_tx,
        )
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    publish_video_stream(
        &channel,
        &alice_session_id,
        StreamType::Camera,
        22_222,
        &state.transport_adapter,
    )
    .await;
    channel
        .test_api()
        .media()
        .update_subscription(
            &bob_session_id,
            &alice_session_id,
            &DownloadStates {
                audio: None,
                camera: Some(true),
                screen: None,
            },
            &state.transport_adapter,
        )
        .await;
    let channels_request = build_request(Request::get(DIAGNOSTICS_CHANNELS_PATH), Body::empty());
    assert!(channels_request.is_some());
    let Some(channels_request) = channels_request else {
        return;
    };
    let channels_response = app(state.clone()).oneshot(channels_request).await;
    assert!(channels_response.is_ok());
    let Some(channels_response) = channels_response.ok() else {
        return;
    };
    assert_eq!(channels_response.status(), StatusCode::OK);
    let channel_summaries: Option<Vec<DiagnosticsChannelSummary>> =
        parse_json(channels_response).await;
    assert!(channel_summaries.is_some());
    let Some(channel_summaries) = channel_summaries else {
        return;
    };
    assert_eq!(channel_summaries.len(), 1);
    assert_eq!(channel_summaries[0].session_count, 2);
    assert_eq!(channel_summaries[0].publication_count, 1);
    assert_eq!(channel_summaries[0].subscription_count, 0);

    let detail_request = build_request(
        Request::get(format!("/internal/diagnostics/channels/{}", channel.uuid())),
        Body::empty(),
    );
    assert!(detail_request.is_some());
    let Some(detail_request) = detail_request else {
        return;
    };
    let detail_response = app(state.clone()).oneshot(detail_request).await;
    assert!(detail_response.is_ok());
    let Some(detail_response) = detail_response.ok() else {
        return;
    };
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail: Option<DiagnosticsChannelDetail> = parse_json(detail_response).await;
    assert!(detail.is_some());
    let Some(detail) = detail else {
        return;
    };
    assert_eq!(detail.summary.uuid, channel.uuid());
    assert_eq!(detail.summary.remote_address, "203.0.113.10");
    assert_eq!(detail.sessions.len(), 2);
    assert!(
        detail
            .recent_events
            .iter()
            .any(|event| event.event == "publish.committed")
    );

    let session_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/sessions/{}",
            alice_session_id.clone().into_integer_string()
        )),
        Body::empty(),
    );
    assert!(session_request.is_some());
    let Some(session_request) = session_request else {
        return;
    };
    let session_response = app(state.clone()).oneshot(session_request).await;
    assert!(session_response.is_ok());
    let Some(session_response) = session_response.ok() else {
        return;
    };
    assert_eq!(session_response.status(), StatusCode::OK);
    let session_detail: Option<DiagnosticsSessionDetail> = parse_json(session_response).await;
    assert!(session_detail.is_some());
    let Some(session_detail) = session_detail else {
        return;
    };
    assert_eq!(session_detail.channel_uuid, channel.uuid());
    assert_eq!(session_detail.session.session_id, alice_session_id);
    assert_eq!(session_detail.session.publications.len(), 1);
    assert_eq!(session_detail.session.publications[0].source_id, 1);
    assert_eq!(session_detail.session.publications[0].encoding_ids.len(), 1);
    assert!(
        session_detail
            .recent_events
            .iter()
            .any(|event| event.event == "session.joined")
    );

    let summary_request = build_request(Request::get(DIAGNOSTICS_SUMMARY_PATH), Body::empty());
    assert!(summary_request.is_some());
    let Some(summary_request) = summary_request else {
        return;
    };
    let summary_response = app(state).oneshot(summary_request).await;
    assert!(summary_response.is_ok());
    let Some(summary_response) = summary_response.ok() else {
        return;
    };
    assert_eq!(summary_response.status(), StatusCode::OK);
    let summary: Option<DiagnosticsSummaryResponse> = parse_json(summary_response).await;
    assert!(summary.is_some());
    let Some(summary) = summary else {
        return;
    };
    assert_eq!(summary.channels_active, 1);
    assert_eq!(summary.sessions_active, 2);
    assert_eq!(summary.publications_active, 1);
    assert_eq!(summary.subscriptions_active, 0);
}

#[tokio::test]
async fn diagnostics_session_lookup_reports_ambiguous_matches() {
    let state = test_state();
    let first_channel = state
        .channel_manager
        .serve_channel(
            "issuer-a",
            None,
            &ChannelConfig::default(),
            Some("203.0.113.10"),
        )
        .await;
    let second_channel = state
        .channel_manager
        .serve_channel(
            "issuer-b",
            None,
            &ChannelConfig::default(),
            Some("203.0.113.11"),
        )
        .await;
    let (first_tx, _first_rx) = mpsc::unbounded_channel();
    let (second_tx, _second_rx) = mpsc::unbounded_channel();
    assert!(
        first_channel
            .test_api()
            .lifecycle()
            .join_session(
                SessionId::Integer(7),
                None,
                SessionPermissions::default(),
                first_tx,
            )
            .await
            .is_ok()
    );
    assert!(
        second_channel
            .test_api()
            .lifecycle()
            .join_session(
                SessionId::Integer(7),
                None,
                SessionPermissions::default(),
                second_tx,
            )
            .await
            .is_ok()
    );

    let request = build_request(
        Request::get("/internal/diagnostics/sessions/7"),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(state).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let conflict: Option<DiagnosticsSessionLookupConflict> = parse_json(response).await;
    assert!(conflict.is_some());
    let Some(conflict) = conflict else {
        return;
    };
    assert_eq!(conflict.requested_session_id, "7");
    assert_eq!(conflict.matching_channel_uuids.len(), 2);
}

trait SessionIdExt {
    fn into_integer_string(self) -> String;
}

impl SessionIdExt for SessionId {
    fn into_integer_string(self) -> String {
        match self {
            SessionId::Integer(value) => value.to_string(),
            SessionId::String(value) => value,
        }
    }
}
