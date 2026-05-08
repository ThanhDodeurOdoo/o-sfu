#![allow(
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::use_self,
    reason = "the diagnostics route tests favor direct assertion style and a single end-to-end scenario over helper indirection"
)]

use std::net::SocketAddr;

use o_sfu_protocol::shared::StreamType;
use o_sfu_router::{
    MediaKind, MediaStream,
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
    },
};
use serde_json::Value;

use super::fixtures::*;
use crate::{
    application::stream_catalog::source_publish_intent_for_stream_type,
    core::{
        SessionNegotiationOutcome,
        server::session::{UserId, UserInfo, UserPermissions},
    },
    runtime::{
        diagnostics::{DiagnosticsTemporalLayerMetadata, DiagnosticsTemporalLayerSelection},
        room::Room,
    },
};

fn test_simulcast_video_rtp_parameters() -> MediaStream {
    sample_simulcast_video_rtp_parameters(None)
}

const DIAGNOSTICS_ROUTE_PATHS: &[&str] = &[
    DIAGNOSTICS_SUMMARY_PATH,
    DIAGNOSTICS_ROOMS_PATH,
    "/internal/diagnostics/rooms/test-room",
    "/internal/diagnostics/rooms/test-room/users",
    "/internal/diagnostics/node-graph/rooms/test-room",
    "/internal/diagnostics/node-graph/rooms/test-room/users/1",
    "/internal/diagnostics/users/1",
];

async fn assert_diagnostics_path_status(
    state: &RuntimeState,
    path: &str,
    authorization: Option<&str>,
    expected_status: StatusCode,
) {
    let mut request_builder = Request::get(path);
    if let Some(authorization) = authorization {
        request_builder = request_builder.header(header::AUTHORIZATION, authorization);
    }
    let request = build_request(request_builder, Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(state.clone()).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), expected_status);
}

async fn make_session_ready(room: &Room, user_id: &UserId, media_transport: &MediaTransport) {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist before publishing");
    };
    assert_eq!(
        room.apply_session_negotiated(
            user_id,
            connection_id,
            sample_client_rtp_capabilities(),
            media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
}

async fn publish_media_stream(
    room: &Room,
    user_id: &UserId,
    stream_type: StreamType,
    parameters: MediaStream,
    media_transport: &MediaTransport,
) {
    make_session_ready(room, user_id, media_transport).await;
    assert!(
        room.test_api()
            .media()
            .publish_intent(
                user_id,
                &source_publish_intent_for_stream_type(stream_type),
                MediaKind::Video,
                parameters,
                media_transport,
            )
            .await
            .is_some()
    );
}

#[tokio::test]
async fn diagnostics_routes_are_forbidden_without_token_on_public_listener() {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    for path in DIAGNOSTICS_ROUTE_PATHS {
        assert_diagnostics_path_status(&state, path, None, StatusCode::FORBIDDEN).await;
    }
}

#[tokio::test]
async fn diagnostics_routes_require_the_configured_bearer_token() {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    for path in DIAGNOSTICS_ROUTE_PATHS {
        assert_diagnostics_path_status(&state, path, None, StatusCode::UNAUTHORIZED).await;
        assert_diagnostics_path_status(
            &state,
            path,
            Some("Basic operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await;
        assert_diagnostics_path_status(
            &state,
            path,
            Some("jwt operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await;
    }

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
    let test_state = test_state_with_handles();
    let room = test_state
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
    make_session_ready(&room, &bob_session_id, &test_state.media_transport).await;
    make_session_ready(&room, &carol_session_id, &test_state.media_transport).await;
    publish_media_stream(
        &room,
        &alice_session_id,
        StreamType::Camera,
        test_simulcast_video_rtp_parameters(),
        &test_state.media_transport,
    )
    .await;
    if let Some(fake) = test_state.media_transport.as_fake_transport() {
        fake.set_receiver_bandwidth_estimate(bob_session_id.clone(), 200_000);
    }
    for _ in 0..2 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(
                &alice_session_id,
                UserInfo::default(),
                false,
                &test_state.media_transport,
            )
            .await;
    }
    let rooms_request = build_request(Request::get(DIAGNOSTICS_ROOMS_PATH), Body::empty());
    assert!(rooms_request.is_some());
    let Some(rooms_request) = rooms_request else {
        return;
    };
    let rooms_response = app(test_state.state.clone()).oneshot(rooms_request).await;
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
    assert_eq!(room_summaries[0].source_count, 1);
    assert_eq!(room_summaries[0].publication_count, 1);
    assert_eq!(room_summaries[0].subscription_count, 2);

    let room_users_request = build_request(
        Request::get(format!("/internal/diagnostics/rooms/{}/users", room.uuid())),
        Body::empty(),
    );
    assert!(room_users_request.is_some());
    let Some(room_users_request) = room_users_request else {
        return;
    };
    let room_users_response = app(test_state.state.clone())
        .oneshot(room_users_request)
        .await;
    assert!(room_users_response.is_ok());
    let Some(room_users_response) = room_users_response.ok() else {
        return;
    };
    assert_eq!(room_users_response.status(), StatusCode::OK);
    let room_users: Option<Vec<DiagnosticsUserSummary>> = parse_json(room_users_response).await;
    assert!(room_users.is_some());
    let Some(room_users) = room_users else {
        return;
    };
    assert_eq!(room_users.len(), 3);
    let Some(alice_summary) = room_users.iter().find(|user| user.user_key == "1") else {
        panic!("alice summary should be listed");
    };
    assert_eq!(alice_summary.room_id, room.uuid());
    assert_eq!(alice_summary.publication_count, 1);
    assert_eq!(alice_summary.subscription_count, 0);
    assert_eq!(alice_summary.media_worker_id, 0);

    let detail_request = build_request(
        Request::get(format!("/internal/diagnostics/rooms/{}", room.uuid())),
        Body::empty(),
    );
    assert!(detail_request.is_some());
    let Some(detail_request) = detail_request else {
        return;
    };
    let detail_response = app(test_state.state.clone()).oneshot(detail_request).await;
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

    let room_graph_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/node-graph/rooms/{}",
            room.uuid()
        )),
        Body::empty(),
    );
    assert!(room_graph_request.is_some());
    let Some(room_graph_request) = room_graph_request else {
        return;
    };
    let room_graph_response = app(test_state.state.clone())
        .oneshot(room_graph_request)
        .await;
    assert!(room_graph_response.is_ok());
    let Some(room_graph_response) = room_graph_response.ok() else {
        return;
    };
    assert_eq!(room_graph_response.status(), StatusCode::OK);
    let room_graph: Option<Value> = parse_json(room_graph_response).await;
    assert!(room_graph.is_some());
    let Some(room_graph) = room_graph else {
        return;
    };
    assert!(room_graph["nodes"].as_array().is_some_and(|nodes| {
        nodes
            .iter()
            .any(|node| node["id"] == format!("room:{}", room.uuid()))
    }));

    let old_channel_graph_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/node-graph/channels/{}",
            room.uuid()
        )),
        Body::empty(),
    );
    assert!(old_channel_graph_request.is_some());
    let Some(old_channel_graph_request) = old_channel_graph_request else {
        return;
    };
    let old_channel_graph_response = app(test_state.state.clone())
        .oneshot(old_channel_graph_request)
        .await;
    assert!(old_channel_graph_response.is_ok());
    let Some(old_channel_graph_response) = old_channel_graph_response.ok() else {
        return;
    };
    assert_eq!(old_channel_graph_response.status(), StatusCode::NOT_FOUND);

    let alice_graph_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/node-graph/rooms/{}/users/{}",
            room.uuid(),
            alice_session_id.clone().into_integer_string()
        )),
        Body::empty(),
    );
    assert!(alice_graph_request.is_some());
    let Some(alice_graph_request) = alice_graph_request else {
        return;
    };
    let alice_graph_response = app(test_state.state.clone())
        .oneshot(alice_graph_request)
        .await;
    assert!(alice_graph_response.is_ok());
    let Some(alice_graph_response) = alice_graph_response.ok() else {
        return;
    };
    assert_eq!(alice_graph_response.status(), StatusCode::OK);
    let alice_graph: Option<Value> = parse_json(alice_graph_response).await;
    assert!(alice_graph.is_some());
    let Some(alice_graph) = alice_graph else {
        return;
    };
    assert!(
        alice_graph["nodes"]
            .as_array()
            .is_some_and(|nodes| nodes.iter().any(|node| node["id"] == "worker:0"))
    );
    assert!(
        alice_graph["edges"]
            .as_array()
            .is_some_and(|edges| edges.iter().any(|edge| edge["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("deliver:"))
                && edge["detail__direction"] == "outbound"))
    );

    let bob_graph_request = build_request(
        Request::get(format!(
            "/internal/diagnostics/node-graph/rooms/{}/users/{}",
            room.uuid(),
            bob_session_id.clone().into_integer_string()
        )),
        Body::empty(),
    );
    assert!(bob_graph_request.is_some());
    let Some(bob_graph_request) = bob_graph_request else {
        return;
    };
    let bob_graph_response = app(test_state.state.clone())
        .oneshot(bob_graph_request)
        .await;
    assert!(bob_graph_response.is_ok());
    let Some(bob_graph_response) = bob_graph_response.ok() else {
        return;
    };
    assert_eq!(bob_graph_response.status(), StatusCode::OK);
    let bob_graph: Option<Value> = parse_json(bob_graph_response).await;
    assert!(bob_graph.is_some());
    let Some(bob_graph) = bob_graph else {
        return;
    };
    assert!(
        bob_graph["edges"]
            .as_array()
            .is_some_and(|edges| edges.iter().any(|edge| edge["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("deliver:"))
                && edge["detail__direction"] == "inbound"))
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
    let session_response = app(test_state.state.clone()).oneshot(session_request).await;
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
    let bob_session_response = app(test_state.state.clone())
        .oneshot(bob_session_request)
        .await;
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
    assert_eq!(
        subscription
            .selection
            .latest_receiver_bandwidth_estimate_bps,
        Some(200_000)
    );
    assert_eq!(
        subscription.selection.selected_video_budget_bps,
        Some(200_000)
    );
    assert_eq!(subscription.selection.active_video_route_count, 1);
    assert_eq!(subscription.selection.selected_video_bitrate_bps, 150_000);
    assert_eq!(subscription.selection.over_budget_exception_reason, None);

    let summary_request = build_request(Request::get(DIAGNOSTICS_SUMMARY_PATH), Body::empty());
    assert!(summary_request.is_some());
    let Some(summary_request) = summary_request else {
        return;
    };
    let summary_response = app(test_state.state).oneshot(summary_request).await;
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
    let test_state = test_state_with_handles();
    let first_room = test_state
        .room_manager
        .serve_room(
            "issuer-a",
            None,
            &RoomConfig::default(),
            Some("203.0.113.10"),
        )
        .await;
    let second_room = test_state
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
    let response = app(test_state.state).oneshot(request).await;
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

#[tokio::test]
async fn diagnostics_user_lookup_reports_missing_index_matches() {
    let test_state = test_state_with_handles();
    let request = build_request(
        Request::get("/internal/diagnostics/users/404"),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state.state).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn diagnostics_user_lookup_survives_user_replacement_without_conflict() {
    let test_state = test_state_with_handles();
    let room = test_state
        .room_manager
        .serve_room(
            "issuer-replacement",
            None,
            &RoomConfig::default(),
            Some("203.0.113.12"),
        )
        .await;
    let user_id = UserId::Integer(9);
    let (first_tx, _first_rx) = mpsc::unbounded_channel();
    let (replacement_tx, _replacement_rx) = mpsc::unbounded_channel();
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(user_id.clone(), None, UserPermissions::default(), first_tx,)
            .await
            .is_ok()
    );
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(
                user_id.clone(),
                Some(String::from("replacement")),
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    let request = build_request(Request::get("/internal/diagnostics/users/9"), Body::empty());
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state.state).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::OK);
    let detail: Option<DiagnosticsUserDetail> = parse_json(response).await;
    assert!(detail.is_some());
    let Some(detail) = detail else {
        return;
    };
    assert_eq!(detail.room_id, room.uuid());
    assert_eq!(detail.user.user_id, user_id);
}

#[tokio::test]
async fn diagnostics_user_lookup_drops_room_teardown_entries() {
    let test_state = test_state_with_handles();
    let room = test_state
        .room_manager
        .serve_room(
            "issuer-teardown",
            None,
            &RoomConfig::default(),
            Some("203.0.113.13"),
        )
        .await;
    let room_id = room.uuid().to_owned();
    let user_id = UserId::Integer(11);
    let (tx, _rx) = mpsc::unbounded_channel();
    let join = test_state
        .room_manager
        .join_user(
            &room_id,
            JoinUserRequest {
                user_id: user_id.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: tx,
            },
            &test_state.media_transport,
        )
        .await;
    assert!(join.is_ok());
    let Some((_room, connection_id)) = join.ok() else {
        return;
    };
    assert!(
        test_state
            .room_manager
            .close_session(
                &room_id,
                &user_id,
                connection_id,
                &test_state.media_transport,
            )
            .await
    );

    let request = build_request(
        Request::get("/internal/diagnostics/users/11"),
        Body::empty(),
    );
    assert!(request.is_some());
    let Some(request) = request else {
        return;
    };
    let response = app(test_state.state).oneshot(request).await;
    assert!(response.is_ok());
    let Some(response) = response.ok() else {
        return;
    };
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
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
