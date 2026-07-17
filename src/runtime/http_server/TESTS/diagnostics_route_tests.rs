#![allow(
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::use_self,
    reason = "the diagnostics route tests keep one end-to-end diagnostics scenario with direct field assertions"
)]

use std::{net::SocketAddr, sync::Arc};

use o_sfu_protocol::wire::StreamType;
use o_sfu_router::{
    MediaKind,
    rtp::MediaStream,
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
    },
};
use serde_json::Value;

use super::fixtures::*;
use crate::{
    application::stream_catalog::source_publish_intent_for_stream_type,
    core::{
        prelude::MediaSession,
        server::{
            room::JoinUserRequest,
            session::{UserId, UserInfo, UserPermissions},
        },
    },
    runtime::room::Room,
};

const TEST_DIAGNOSTICS_ROOM: &str = "test-room";
const TEST_DIAGNOSTICS_USER: i64 = 1;

fn diagnostics_route_paths() -> [String; 8] {
    let user_id = TEST_DIAGNOSTICS_USER.to_string();
    [
        route::diagnostics::SUMMARY.to_owned(),
        route::diagnostics::ROOMS.to_owned(),
        route::diagnostics::WORKERS.to_owned(),
        route::diagnostics::ROOM.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::ROOM_USERS.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::ROOM_GRAPH.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", TEST_DIAGNOSTICS_ROOM)
            .replace("{id}", &user_id),
        route::diagnostics::USER.replace("{id}", &user_id),
    ]
}

fn test_simulcast_video_rtp_parameters() -> MediaStream {
    sample_simulcast_video_rtp_parameters(None)
}

async fn diagnostics_status(
    state: &RuntimeState,
    path: &str,
    authorization: Option<&str>,
    expected_status: StatusCode,
) -> TestResult {
    let mut builder = Request::get(path);
    if let Some(authorization) = authorization {
        builder = builder.header(header::AUTHORIZATION, authorization);
    }
    route_status(
        state,
        builder,
        Body::empty(),
        expected_status,
        "diagnostics request should complete",
    )
    .await
}

async fn diagnostics_json<T>(state: &RuntimeState, path: impl AsRef<str>) -> TestResult<T>
where
    T: DeserializeOwned,
{
    route_json(
        state,
        Request::get(path.as_ref()),
        Body::empty(),
        StatusCode::OK,
        "diagnostics request should succeed",
    )
    .await
}

async fn serve_diagnostics_room(
    test_state: &RuntimeTestState,
    issuer: &str,
    remote_address: &str,
) -> Arc<Room> {
    test_state
        .room_manager
        .serve_room(
            issuer,
            TEST_ROOM_KEY,
            &RoomConfig::default(),
            Some(remote_address),
        )
        .await
}

async fn join_room_user(
    state: &RuntimeState,
    room: &Room,
    user_id: UserId,
    context: &'static str,
) -> TestResult<(MediaSession, UserOutboundReceiver)> {
    let (tx, rx) = test_outbound_sender(state);
    let session = require_ok(
        state
            .sfu_core
            .admit_user(
                room.uuid(),
                JoinUserRequest {
                    user_id,
                    label: None,
                    permissions: UserPermissions::default(),
                    sender: tx,
                },
            )
            .await,
        context,
    )?;
    Ok((session, rx))
}

async fn make_session_ready(
    room: &Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> TestResult {
    require_some(
        create_transport_session_offer(room, user_id, media_transport).await,
        "transport session offer should be created",
    )?;
    assert!(
        room.test_api()
            .lifecycle()
            .mark_session_ready(user_id, sample_client_rtp_capabilities(), media_transport)
            .await
    );
    Ok(())
}

async fn publish_media_stream(
    room: &Room,
    user_id: &UserId,
    stream_type: StreamType,
    parameters: MediaStream,
    media_transport: &MediaTransport,
) -> TestResult {
    make_session_ready(room, user_id, media_transport).await?;
    require_some(
        room.test_api()
            .media()
            .publish_intent(
                user_id,
                &source_publish_intent_for_stream_type(stream_type),
                MediaKind::Video,
                parameters,
                media_transport,
            )
            .await,
        "media publish intent should commit",
    )?;
    Ok(())
}

#[tokio::test]
async fn diagnostics_routes_are_forbidden_without_token_on_public_listener() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    for path in diagnostics_route_paths() {
        diagnostics_status(&state, &path, None, StatusCode::FORBIDDEN).await?;
    }
    Ok(())
}

#[tokio::test]
async fn diagnostics_routes_require_the_configured_bearer_token() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    for path in diagnostics_route_paths() {
        diagnostics_status(&state, &path, None, StatusCode::UNAUTHORIZED).await?;
        diagnostics_status(
            &state,
            &path,
            Some("Basic operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
        diagnostics_status(
            &state,
            &path,
            Some("jwt operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }

    diagnostics_status(
        &state,
        route::diagnostics::SUMMARY,
        Some("Bearer operator-secret"),
        StatusCode::OK,
    )
    .await
}

#[tokio::test]
async fn diagnostics_routes_return_live_room_and_user_details() -> TestResult {
    let test_state = test_state_with_handles();
    let room = serve_diagnostics_room(&test_state, "issuer-a", "203.0.113.10").await;
    let alice_user_id = UserId::Integer(1);
    let bob_user_id = UserId::Integer(2);
    let carol_user_id = UserId::Integer(3);
    let (alice_session, alice_rx) = join_room_user(
        &test_state.state,
        &room,
        alice_user_id.clone(),
        "alice should join diagnostics room",
    )
    .await?;
    let (_, bob_rx) = join_room_user(
        &test_state.state,
        &room,
        bob_user_id.clone(),
        "bob should join diagnostics room",
    )
    .await?;
    let (_, carol_rx) = join_room_user(
        &test_state.state,
        &room,
        carol_user_id.clone(),
        "carol should join diagnostics room",
    )
    .await?;
    let _receivers = [alice_rx, bob_rx, carol_rx];
    make_session_ready(&room, &bob_user_id, &test_state.media_transport).await?;
    make_session_ready(&room, &carol_user_id, &test_state.media_transport).await?;
    publish_media_stream(
        &room,
        &alice_user_id,
        StreamType::Camera,
        test_simulcast_video_rtp_parameters(),
        &test_state.media_transport,
    )
    .await?;
    for _ in 0..2 {
        alice_session.update_info(UserInfo::default()).await;
    }

    let room_summaries: Vec<DiagnosticsRoomSummary> =
        diagnostics_json(&test_state.state, route::diagnostics::ROOMS).await?;
    assert_eq!(room_summaries.len(), 1);
    assert_eq!(room_summaries[0].user_count, 3);
    assert_eq!(room_summaries[0].source_count, 1);
    assert_eq!(room_summaries[0].publication_count, 1);
    assert_eq!(room_summaries[0].subscription_count, 2);

    let room_users: Vec<DiagnosticsUserSummary> = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM_USERS.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert_eq!(room_users.len(), 3);
    let alice_summary = require_some(
        room_users.iter().find(|user| user.user_key == "1"),
        "alice summary should be listed",
    )?;
    assert_eq!(alice_summary.room_id, room.uuid());
    assert_eq!(alice_summary.publication_count, 1);
    assert_eq!(alice_summary.subscription_count, 0);
    assert_eq!(alice_summary.media_worker_id, 0);

    let worker_summaries: Vec<DiagnosticsWorkerSummary> =
        diagnostics_json(&test_state.state, route::diagnostics::WORKERS).await?;
    assert_eq!(worker_summaries.len(), 1);
    let worker_summary = &worker_summaries[0];
    assert_eq!(worker_summary.media_worker_id, 0);
    assert_eq!(worker_summary.room_count, 1);
    assert_eq!(worker_summary.user_count, 3);
    assert_eq!(worker_summary.publication_count, 1);
    assert_eq!(worker_summary.subscription_count, 2);
    assert_eq!(worker_summary.pressure.egress_bitrate_bps, 0);
    assert_eq!(worker_summary.pressure.packet_loop_lag_ms, 0);
    assert_eq!(worker_summary.pressure.command_backlog_depth, 0);
    assert_eq!(worker_summary.pressure.relay_mailbox_depth, 0);
    assert_eq!(worker_summary.pressure.worker_pressure_score, 0);

    let detail: DiagnosticsRoomDetail = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert_eq!(detail.summary.uuid, room.uuid());
    assert_eq!(detail.summary.remote_address, "203.0.113.10");
    assert_eq!(detail.users.len(), 3);
    assert_eq!(detail.sources.len(), 1);
    assert_eq!(detail.sources[0].source_id, 1);
    assert_eq!(detail.sources[0].encodings.len(), 2);
    assert_eq!(detail.sources[0].encodings[0].rid.as_deref(), Some("lo"));
    assert_eq!(detail.sources[0].encodings[1].rid.as_deref(), Some("hi"));
    assert!(
        detail
            .recent_events
            .iter()
            .any(|event| event.event == "publish.committed")
    );

    let room_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM_GRAPH.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert!(room_graph["nodes"].as_array().is_some_and(|nodes| {
        nodes
            .iter()
            .any(|node| node["id"] == format!("room:{}", room.uuid()))
    }));

    diagnostics_status(
        &test_state.state,
        &format!("/internal/diagnostics/node-graph/channels/{}", room.uuid()),
        None,
        StatusCode::NOT_FOUND,
    )
    .await?;

    let alice_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", room.uuid())
            .replace("{id}", alice_user_id.path_segment().as_ref()),
    )
    .await?;
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

    let bob_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", room.uuid())
            .replace("{id}", bob_user_id.path_segment().as_ref()),
    )
    .await?;
    assert!(
        bob_graph["edges"]
            .as_array()
            .is_some_and(|edges| edges.iter().any(|edge| edge["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("deliver:"))
                && edge["detail__direction"] == "inbound"))
    );

    let session_detail: DiagnosticsUserDetail = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER.replace("{id}", alice_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_eq!(session_detail.room_id, room.uuid());
    assert_eq!(session_detail.user.user_id, alice_user_id);
    assert_eq!(session_detail.user.publications.len(), 1);
    assert_eq!(session_detail.user.publications[0].source_id, 1);
    assert_eq!(session_detail.user.publications[0].encoding_ids.len(), 2);
    assert!(
        session_detail
            .recent_events
            .iter()
            .any(|event| event.event == "user.joined")
    );

    let bob_session_detail: DiagnosticsUserDetail = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER.replace("{id}", bob_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_eq!(bob_session_detail.user.subscriptions.len(), 1);
    let subscription = &bob_session_detail.user.subscriptions[0];
    assert_eq!(subscription.source_id, 1);
    assert!(subscription.selection.selected_encoding_id.is_some());
    assert_eq!(subscription.selection.selected_rid.as_deref(), Some("hi"));
    assert_eq!(
        subscription.selection.selection_reason,
        DiagnosticsSourceSelectionReason::ReceiverAdaptation
    );
    assert_eq!(
        subscription
            .selection
            .latest_receiver_bandwidth_estimate_bps,
        Some(10_000_000)
    );
    assert_eq!(
        subscription.selection.selected_video_budget_bps,
        Some(10_000_000)
    );
    assert_eq!(subscription.selection.active_video_route_count, 1);
    assert_eq!(subscription.selection.selected_video_bitrate_bps, 900_000);
    assert_eq!(subscription.selection.over_budget_exception_reason, None);

    let summary: DiagnosticsSummaryResponse =
        diagnostics_json(&test_state.state, route::diagnostics::SUMMARY).await?;
    assert_eq!(summary.rooms_active, 1);
    assert_eq!(summary.users_active, 3);
    assert_eq!(summary.publications_active, 1);
    assert_eq!(summary.subscriptions_active, 2);
    Ok(())
}

#[tokio::test]
async fn diagnostics_user_lookup_reports_ambiguous_matches() -> TestResult {
    let test_state = test_state_with_handles();
    let first_room = serve_diagnostics_room(&test_state, "issuer-a", "203.0.113.10").await;
    let second_room = serve_diagnostics_room(&test_state, "issuer-b", "203.0.113.11").await;
    let (_, first_rx) = join_room_user(
        &test_state.state,
        &first_room,
        UserId::Integer(7),
        "first matching user should join",
    )
    .await?;
    let (_, second_rx) = join_room_user(
        &test_state.state,
        &second_room,
        UserId::Integer(7),
        "second matching user should join",
    )
    .await?;
    let _receivers = [first_rx, second_rx];

    let response = route_response(
        &test_state.state,
        Request::get(route::diagnostics::USER.replace("{id}", "7")),
        Body::empty(),
        StatusCode::CONFLICT,
        "ambiguous diagnostics lookup should complete",
    )
    .await?;
    let conflict: DiagnosticsUserLookupConflict =
        response_json(response, "conflict payload should decode").await?;
    assert_eq!(conflict.requested_user_id, "7");
    assert_eq!(conflict.matching_room_ids.len(), 2);
    Ok(())
}

#[tokio::test]
async fn diagnostics_user_lookup_reports_missing_index_matches() -> TestResult {
    let test_state = test_state_with_handles();
    let path = route::diagnostics::USER.replace("{id}", "404");
    diagnostics_status(&test_state.state, &path, None, StatusCode::NOT_FOUND).await
}

#[tokio::test]
async fn diagnostics_user_lookup_survives_user_replacement_without_conflict() -> TestResult {
    let test_state = test_state_with_handles();
    let room = serve_diagnostics_room(&test_state, "issuer-replacement", "203.0.113.12").await;
    let user_id = UserId::Integer(9);
    let (_, _first_rx) = join_room_user(
        &test_state.state,
        &room,
        user_id.clone(),
        "first user should join",
    )
    .await?;
    let (replacement_tx, _replacement_rx) = test_outbound_sender(&test_state.state);
    require_ok(
        room.test_api()
            .lifecycle()
            .join_user(
                user_id.clone(),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await,
        "replacement user should join",
    )?;

    let detail: DiagnosticsUserDetail = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER.replace("{id}", "9"),
    )
    .await?;
    assert_eq!(detail.room_id, room.uuid());
    assert_eq!(detail.user.user_id, user_id);
    Ok(())
}

#[tokio::test]
async fn diagnostics_user_lookup_drops_room_teardown_entries() -> TestResult {
    let test_state = test_state_with_handles();
    let room = serve_diagnostics_room(&test_state, "issuer-teardown", "203.0.113.13").await;
    let room_id = room.uuid().to_owned();
    let user_id = UserId::Integer(11);
    let (tx, _rx) = test_outbound_sender(&test_state.state);
    let session = require_ok(
        test_state
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
            .await,
        "user should join before teardown",
    )?;
    assert!(
        test_state
            .room_manager
            .close_session(
                &room_id,
                &user_id,
                session.connection_id,
                &test_state.media_transport,
            )
            .await
    );

    diagnostics_status(
        &test_state.state,
        &route::diagnostics::USER.replace("{id}", "11"),
        None,
        StatusCode::NOT_FOUND,
    )
    .await
}
