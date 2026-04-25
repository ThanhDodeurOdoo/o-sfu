#![allow(
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::use_self,
    reason = "the diagnostics route tests favor direct assertion style and a single end-to-end scenario over helper indirection"
)]

use std::net::SocketAddr;

use o_sfu_protocol::shared::StreamType;
use o_sfu_router::{MediaKind, MediaStream};

use super::fixtures::*;
use crate::runtime::{
    diagnostics::{DiagnosticsTemporalLayerMetadata, DiagnosticsTemporalLayerSelection},
    room::Room,
    test_rtp_samples::{sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters},
};

fn test_simulcast_video_rtp_parameters() -> MediaStream {
    sample_simulcast_video_rtp_parameters(None)
}

async fn make_session_ready(
    room: &Room,
    user_id: &UserId,
    transport_adapter: &RuntimeTransportAdapter,
) {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist before publishing");
    };
    assert!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            sample_client_rtp_capabilities(),
            transport_adapter,
        )
        .await
    );
}

async fn publish_media_stream(
    room: &Room,
    user_id: &UserId,
    stream_type: StreamType,
    parameters: MediaStream,
    transport_adapter: &RuntimeTransportAdapter,
) {
    make_session_ready(room, user_id, transport_adapter).await;
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                stream_type,
                MediaKind::Video,
                parameters,
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
async fn diagnostics_routes_return_live_room_and_user_details() {
    let state = test_state();
    let room = state
        .room_manager
        .serve_room(
            "issuer-a",
            None,
            &RoomConfig::default(),
            Some("203.0.113.10"),
        )
        .await;
    let (alice_tx, _alice_rx) = mpsc::unbounded_channel();
    let (bob_tx, _bob_rx) = mpsc::unbounded_channel();
    let (carol_tx, _carol_rx) = mpsc::unbounded_channel();
    let alice_session_id = UserId::Integer(1);
    let bob_session_id = UserId::Integer(2);
    let carol_session_id = UserId::Integer(3);
    let alice_join = room
        .test_api()
        .lifecycle()
        .join_user(
            alice_session_id.clone(),
            None,
            UserPermissions::default(),
            alice_tx,
        )
        .await;
    let bob_join = room
        .test_api()
        .lifecycle()
        .join_user(
            bob_session_id.clone(),
            None,
            UserPermissions::default(),
            bob_tx,
        )
        .await;
    let carol_join = room
        .test_api()
        .lifecycle()
        .join_user(
            carol_session_id.clone(),
            None,
            UserPermissions::default(),
            carol_tx,
        )
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    assert!(carol_join.is_ok());
    make_session_ready(&room, &bob_session_id, &state.transport_adapter).await;
    make_session_ready(&room, &carol_session_id, &state.transport_adapter).await;
    publish_media_stream(
        &room,
        &alice_session_id,
        StreamType::Camera,
        test_simulcast_video_rtp_parameters(),
        &state.transport_adapter,
    )
    .await;
    let rooms_request = build_request(Request::get(DIAGNOSTICS_ROOMS_PATH), Body::empty());
    assert!(rooms_request.is_some());
    let Some(rooms_request) = rooms_request else {
        return;
    };
    let rooms_response = app(state.clone()).oneshot(rooms_request).await;
    assert!(rooms_response.is_ok());
    let Some(rooms_response) = rooms_response.ok() else {
        return;
    };
    assert_eq!(rooms_response.status(), StatusCode::OK);
    let room_summaries: Option<Vec<DiagnosticsRoomSummary>> = parse_json(rooms_response).await;
    assert!(room_summaries.is_some());
    let Some(room_summaries) = room_summaries else {
        return;
    };
    assert_eq!(room_summaries.len(), 1);
    assert_eq!(room_summaries[0].user_count, 3);
    assert_eq!(room_summaries[0].publication_count, 1);
    assert_eq!(room_summaries[0].subscription_count, 2);

    let detail_request = build_request(
        Request::get(format!("/internal/diagnostics/rooms/{}", room.uuid())),
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
    let detail: Option<DiagnosticsRoomDetail> = parse_json(detail_response).await;
    assert!(detail.is_some());
    let Some(detail) = detail else {
        return;
    };
    assert_eq!(detail.summary.uuid, room.uuid());
    assert_eq!(detail.summary.remote_address, "203.0.113.10");
    assert_eq!(detail.users.len(), 3);
    assert_eq!(detail.sources.len(), 1);
    assert_eq!(detail.sources[0].source_id, 1);
    assert_eq!(detail.sources[0].encodings.len(), 2);
    assert_eq!(detail.sources[0].encodings[0].rid.as_deref(), Some("lo"));
    assert_eq!(
        detail.sources[0].encodings[0].temporal_layer_metadata,
        DiagnosticsTemporalLayerMetadata::Absent
    );
    assert_eq!(detail.sources[0].encodings[1].rid.as_deref(), Some("hi"));
    assert_eq!(
        detail.sources[0].encodings[1].temporal_layer_metadata,
        DiagnosticsTemporalLayerMetadata::Absent
    );
    assert!(
        detail
            .recent_events
            .iter()
            .any(|event| event.event == "publish.committed")
    );

    let session_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/users/{}",
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
    let session_detail: Option<DiagnosticsUserDetail> = parse_json(session_response).await;
    assert!(session_detail.is_some());
    let Some(session_detail) = session_detail else {
        return;
    };
    assert_eq!(session_detail.room_id, room.uuid());
    assert_eq!(session_detail.user.user_id, alice_session_id);
    assert_eq!(session_detail.user.publications.len(), 1);
    assert_eq!(session_detail.user.publications[0].source_id, 1);
    assert_eq!(session_detail.user.publications[0].encoding_ids.len(), 2);
    assert!(
        session_detail
            .recent_events
            .iter()
            .any(|event| event.event == "user.joined")
    );

    let bob_session_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/users/{}",
            bob_session_id.clone().into_integer_string()
        )),
        Body::empty(),
    );
    assert!(bob_session_request.is_some());
    let Some(bob_session_request) = bob_session_request else {
        return;
    };
    let bob_session_response = app(state.clone()).oneshot(bob_session_request).await;
    assert!(bob_session_response.is_ok());
    let Some(bob_session_response) = bob_session_response.ok() else {
        return;
    };
    assert_eq!(bob_session_response.status(), StatusCode::OK);
    let bob_session_detail: Option<DiagnosticsUserDetail> = parse_json(bob_session_response).await;
    assert!(bob_session_detail.is_some());
    let Some(bob_session_detail) = bob_session_detail else {
        return;
    };
    assert_eq!(bob_session_detail.user.subscriptions.len(), 1);
    let subscription = &bob_session_detail.user.subscriptions[0];
    assert_eq!(subscription.source_id, 1);
    assert_eq!(subscription.selection.selected_encoding_id, Some(1));
    assert_eq!(subscription.selection.selected_rid.as_deref(), Some("lo"));
    assert_eq!(subscription.selection.selected_temporal_layer_id, None);
    assert_eq!(
        subscription.selection.temporal_layer_selection,
        DiagnosticsTemporalLayerSelection::NotSelected
    );
    assert_eq!(
        subscription.selection.selection_reason,
        DiagnosticsSourceSelectionReason::ReceiverAdaptation
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
    assert_eq!(summary.rooms_active, 1);
    assert_eq!(summary.users_active, 3);
    assert_eq!(summary.publications_active, 1);
    assert_eq!(summary.subscriptions_active, 2);
}

#[tokio::test]
async fn diagnostics_user_lookup_reports_ambiguous_matches() {
    let state = test_state();
    let first_room = state
        .room_manager
        .serve_room(
            "issuer-a",
            None,
            &RoomConfig::default(),
            Some("203.0.113.10"),
        )
        .await;
    let second_room = state
        .room_manager
        .serve_room(
            "issuer-b",
            None,
            &RoomConfig::default(),
            Some("203.0.113.11"),
        )
        .await;
    let (first_tx, _first_rx) = mpsc::unbounded_channel();
    let (second_tx, _second_rx) = mpsc::unbounded_channel();
    assert!(
        first_room
            .test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(7),
                None,
                UserPermissions::default(),
                first_tx,
            )
            .await
            .is_ok()
    );
    assert!(
        second_room
            .test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(7),
                None,
                UserPermissions::default(),
                second_tx,
            )
            .await
            .is_ok()
    );

    let request = build_request(Request::get("/internal/diagnostics/users/7"), Body::empty());
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
    let conflict: Option<DiagnosticsUserLookupConflict> = parse_json(response).await;
    assert!(conflict.is_some());
    let Some(conflict) = conflict else {
        return;
    };
    assert_eq!(conflict.requested_user_id, "7");
    assert_eq!(conflict.matching_room_ids.len(), 2);
}

trait SessionIdExt {
    fn into_integer_string(self) -> String;
}

impl SessionIdExt for UserId {
    fn into_integer_string(self) -> String {
        match self {
            UserId::Integer(value) => value.to_string(),
            UserId::String(value) => value,
        }
    }
}
