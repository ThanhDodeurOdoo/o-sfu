use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use o_sfu_router::{
    MediaKind, test_sample::sample_video_rtp_parameters as router_sample_video_rtp_parameters,
};
use str0m::{Candidate, Rtc, change::SdpOffer};

use super::{api::NegotiatedPublish, fixtures::*};
use crate::{
    MediaCodecFlags, RtcPortRange,
    runtime::{
        diagnostics::{
            DiagnosticsPolicyPauseReason, DiagnosticsStore, DiagnosticsVideoLayoutRole,
            DiagnosticsVideoRoutePriority,
        },
        metrics::RuntimeMetrics,
        recording::MediaTap,
        room::Room,
        transport_adapter::{
            MediaPort, MediaTransportDeps, NegotiationPort, RtcTransport, RtcTransportConfig,
            SessionBitrateLimits, SessionOffer, SessionPort, SourcePacketGate, TransportMediaId,
            TransportSessionKey, test_support::FakeWebRtcEvent,
        },
    },
};

#[tokio::test]
async fn production_change_pauses_producer_and_broadcasts_info() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    // User 1 publishes a camera track.
    let producer_id = room
        .test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    assert!(producer_id.is_some());

    // Drain the INIT_CONSUMER bootstrap that went to user 2.
    let bootstrap_msgs = drain_outbound(&mut rx2);
    assert!(
        bootstrap_msgs
            .iter()
            .any(|m| matches!(m, UserOutbound::Request(..))),
        "user 2 should have received a bootstrap remote track request"
    );
    // User 1 shouldn't get its own consumer.
    assert!(drain_outbound(&mut rx1).is_empty());

    // Now user 1 sends PRODUCTION_CHANGE: camera off (pause).
    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // Both users should receive a user info broadcast with isCameraOn = false.
    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "user 1 should get info broadcast");
    assert_eq!(msgs2.len(), 1, "user 2 should get info broadcast");

    // Verify the broadcast contains isCameraOn = false.
    let info_msg = &msgs1[0];
    if let UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot)) = info_msg {
        let info = snapshot
            .values()
            .next()
            .expect("snapshot should have one entry");
        assert_eq!(info.is_camera_on, Some(false));
    } else {
        panic!("expected UserInfoChanged, got {info_msg:?}");
    }

    // Resume: user 1 sends PRODUCTION_CHANGE: camera on.
    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Camera, true, &adapter)
        .await;

    let msgs1 = drain_outbound(&mut rx1);
    if let UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot)) = &msgs1[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_camera_on, Some(true));
    } else {
        panic!("expected UserInfoChanged after resume");
    }
}

#[tokio::test]
async fn explicit_unpublish_removes_published_track_and_consumer_routes() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );
    let Some(transport_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            StreamType::Camera,
        )
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert_eq!(
        room.unpublish_track(
            &UserId::Integer(1),
            test_connection_id(0),
            StreamType::Camera,
            &adapter,
        )
        .await,
        UnpublishOutcome::Unpublished
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                StreamType::Camera,
            )
            .await
    );
    assert_transport_media_mapping_is_missing(&room, transport_media_id).await;
    assert_transport_media_owner_mapping_is_missing(&room, transport_media_id).await;

    let publisher_messages = drain_outbound(&mut publisher_rx);
    let subscriber_messages = drain_outbound(&mut subscriber_rx);
    assert!(publisher_messages.iter().any(|message| matches!(
        message,
        UserOutbound::TrackBindingUpdate(update)
            if update.user_id == UserId::Integer(1)
                && update.stream_type == StreamType::Camera
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        UserOutbound::TrackBindingUpdate(update)
            if update.user_id == UserId::Integer(1)
                && update.stream_type == StreamType::Camera
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot))
            if snapshot
                .values()
                .next()
                .is_some_and(|info| info == &UserInfo {
                    is_camera_on: Some(false),
                    ..UserInfo::snapshot_defaults()
                })
    )));

    let removed_media_events = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeWebRtcEvent::MediaRemoved { .. }))
        .count();
    assert_eq!(removed_media_events, 2);
}

#[tokio::test]
async fn multiparty_camera_publish_installs_the_initial_simulcast_selection() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for raw_user_id in [1_i64, 2, 3] {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        room.test_api()
            .lifecycle()
            .join_user(user_id.clone(), None, UserPermissions::default(), sender)
            .await
            .expect("user should join");
        make_session_ready(&room, &user_id).await;
    }

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );

    assert_consumer_packet_selection_update(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
    assert_consumer_packet_selection_update(
        &fake.snapshot_events(),
        &UserId::Integer(3),
        &UserId::Integer(1),
        "lo",
    );
}

#[tokio::test]
async fn multiparty_without_active_audio_uses_thumbnail_policy_without_featured_camera() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_camera(&room, &UserId::Integer(1), &adapter).await;
    publish_camera(&room, &UserId::Integer(2), &adapter).await;

    let events = fake.snapshot_events();
    assert_consumer_packet_selection_update(
        &events,
        &UserId::Integer(3),
        &UserId::Integer(1),
        "lo",
    );
    assert_consumer_packet_selection_update(
        &events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "lo",
    );

    for user_id in [UserId::Integer(1), UserId::Integer(2), UserId::Integer(3)] {
        let (_user_id, info) = room
            .test_api()
            .inspect()
            .user_info_snapshot(&user_id)
            .await
            .expect("user info should exist");
        assert_ne!(info.is_featured, Some(true));
    }
}

#[tokio::test]
async fn two_party_camera_publish_selects_the_highest_consumer_layer() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );
    assert_consumer_packet_selection_update(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_keyframe_request(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
    );
}

#[tokio::test]
async fn joining_a_third_user_lowers_existing_thumbnail_consumers() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );

    let baseline_event_count = fake.snapshot_events().len();
    let (sender, _receiver) = test_sender();
    room.test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            UserId::Integer(3),
            None,
            UserPermissions::default(),
            sender,
            &adapter,
        )
        .await
        .expect("third user should join");

    let events = fake.snapshot_events();
    assert_consumer_packet_selection_update(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
    assert_consumer_keyframe_request(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
    );
}

#[tokio::test]
async fn leaving_a_multiparty_room_restores_the_highest_consumer_layer() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for raw_user_id in [1_i64, 2, 3] {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        room.test_api()
            .lifecycle()
            .join_session_without_transport_cleanup(
                user_id.clone(),
                None,
                UserPermissions::default(),
                sender,
                &adapter,
            )
            .await
            .expect("user should join");
        make_session_ready(&room, &user_id).await;
    }

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );

    let baseline_event_count = fake.snapshot_events().len();
    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_without_transport_cleanup(
                &UserId::Integer(3),
                test_connection_id(2),
                &adapter,
            )
            .await
    );

    let events = fake.snapshot_events();
    assert_consumer_packet_selection_update(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_keyframe_request(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
    );
}

#[tokio::test]
async fn receiver_bandwidth_pressure_downswitches_after_sustained_observations() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_audio_and_camera(&room, &UserId::Integer(1), &adapter).await;
    let (first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;

    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
        .await;
    assert_consumer_packet_selection_update(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );

    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 100_000);
    let baseline_event_count = fake.snapshot_events().len();
    for _ in 0..2 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }

    let events = fake.snapshot_events();
    assert_consumer_packet_selection_update(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
    assert_consumer_keyframe_request(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
    );
}

#[tokio::test]
async fn receiver_bandwidth_recovery_upswitches_conservatively_with_keyframe() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_audio_and_camera(&room, &UserId::Integer(1), &adapter).await;
    let (first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;

    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 100_000);
    for _ in 0..2 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }
    assert_consumer_packet_selection_update(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );

    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 2_000_000);
    let baseline_event_count = fake.snapshot_events().len();
    for _ in 0..3 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }

    let events = fake.snapshot_events();
    let recovery_events = &events[baseline_event_count..];
    assert_consumer_packet_selection_update(
        recovery_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_keyframe_request(recovery_events, &UserId::Integer(2), &UserId::Integer(1));
}

#[tokio::test]
async fn receiver_budget_pauses_visible_thumbnail_after_cheapest_layers_do_not_fit() {
    let (room, adapter, fake) = setup_ready_users_with_fake(&[1, 2, 3, 4]).await;
    for raw_user_id in [1_i64, 3, 4] {
        publish_camera(&room, &UserId::Integer(raw_user_id), &adapter).await;
    }

    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 200_000);
    let baseline_event_count = fake.snapshot_events().len();
    for _ in 0..2 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }

    let events = fake.snapshot_events();
    assert_consumer_activity_update(
        &events[baseline_event_count..],
        &UserId::Integer(2),
        &UserId::Integer(1),
        false,
    );
    let diagnostics = room.diagnostics_user_views(&adapter).await;
    let paused_subscription = diagnostics
        .iter()
        .find(|view| view.user_id == UserId::Integer(2))
        .and_then(|view| {
            view.subscriptions
                .iter()
                .find(|subscription| subscription.producer_user_id == UserId::Integer(1))
        })
        .expect("diagnostics should include the policy-paused subscription");
    assert!(!paused_subscription.selection.policy_allows_delivery);
    assert_eq!(
        paused_subscription.selection.policy_pause_reason,
        Some(DiagnosticsPolicyPauseReason::BudgetPressure)
    );
    assert_eq!(
        paused_subscription.selection.selected_estimated_bitrate_bps,
        Some(150_000)
    );
    assert_eq!(
        paused_subscription
            .selection
            .latest_receiver_bandwidth_estimate_bps,
        Some(200_000)
    );
    assert_eq!(
        paused_subscription.selection.selected_video_budget_bps,
        Some(200_000)
    );
    assert_eq!(
        paused_subscription.selection.active_video_route_count, 1,
        "the diagnostics should describe the receiver-level active video set"
    );
    assert_eq!(
        paused_subscription.selection.selected_video_bitrate_bps, 150_000,
        "the diagnostics should expose the selected receiver video total"
    );
}

#[tokio::test]
async fn receiver_budget_resumes_policy_paused_route_without_erasing_subscription_state() {
    let (room, adapter, fake) = setup_ready_users_with_fake(&[1, 2, 3, 4]).await;
    for raw_user_id in [1_i64, 3, 4] {
        publish_camera(&room, &UserId::Integer(raw_user_id), &adapter).await;
    }

    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 200_000);
    for _ in 0..2 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }
    assert_consumer_activity_update(
        &fake.snapshot_events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        false,
    );

    fake.set_receiver_bandwidth_estimate(UserId::Integer(2), 1_000_000);
    let baseline_event_count = fake.snapshot_events().len();
    for _ in 0..3 {
        room.test_api()
            .lifecycle()
            .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
            .await;
    }

    let events = fake.snapshot_events();
    let recovery_events = &events[baseline_event_count..];
    assert_consumer_activity_update(
        recovery_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        true,
    );
    assert_consumer_keyframe_request(recovery_events, &UserId::Integer(2), &UserId::Integer(1));
}

async fn setup_ready_users_with_fake(
    user_ids: &[i64],
) -> (Arc<Room>, RuntimeTransportAdapter, Arc<FakeWebRtcAdapter>) {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for &raw_user_id in user_ids {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        room.test_api()
            .lifecycle()
            .join_session_without_transport_cleanup(
                user_id.clone(),
                None,
                UserPermissions::default(),
                sender,
                &adapter,
            )
            .await
            .expect("user should join");
        make_session_ready(&room, &user_id).await;
    }
    (room, adapter, fake)
}

async fn setup_three_ready_users_with_fake()
-> (Arc<Room>, RuntimeTransportAdapter, Arc<FakeWebRtcAdapter>) {
    setup_ready_users_with_fake(&[1, 2, 3]).await
}

async fn publish_audio_and_camera(
    room: &Arc<Room>,
    user_id: &UserId,
    adapter: &RuntimeTransportAdapter,
) {
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                adapter,
            )
            .await
            .is_some()
    );
    publish_camera(room, user_id, adapter).await;
}

async fn publish_camera(room: &Arc<Room>, user_id: &UserId, adapter: &RuntimeTransportAdapter) {
    assert!(
        room.test_api()
            .media()
            .publish_track(
                user_id,
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                adapter,
            )
            .await
            .is_some()
    );
}

async fn source_media_ids(
    room: &Arc<Room>,
    user_id: &UserId,
) -> (TransportMediaId, TransportMediaId) {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist");
    };
    let Some(audio_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, StreamType::Audio)
        .await
    else {
        panic!("audio producer should expose a transport media id");
    };
    let Some(camera_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, StreamType::Camera)
        .await
    else {
        panic!("camera producer should expose a transport media id");
    };
    (audio_media_id, camera_media_id)
}

async fn assert_transport_media_mapping_is_missing(
    room: &Arc<Room>,
    transport_media_id: TransportMediaId,
) {
    assert!(
        room.test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
}

async fn assert_transport_media_owner_mapping_is_missing(
    room: &Arc<Room>,
    transport_media_id: TransportMediaId,
) {
    assert!(
        room.test_api()
            .inspect()
            .producer_owner_user_id_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert!(
        room.test_api()
            .inspect()
            .producer_owner_connection_id_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
}

async fn assert_user_has_no_producer_route_target(
    room: &Arc<Room>,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: StreamType,
) {
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(user_id, connection_id, stream_type)
            .await
    );
}

fn assert_consumer_packet_selection_update(
    events: &[FakeWebRtcEvent],
    consumer_user_id: &UserId,
    source_user_id: &UserId,
    expected_rid: &str,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerPacketGateUpdated {
                consumer_user_id: updated_consumer_user_id,
                source_user_id: updated_source_user_id,
                packet_gate: SourcePacketGate::Rid(rid),
            } if updated_consumer_user_id == consumer_user_id
                && updated_source_user_id == source_user_id
                && rid == expected_rid
        )
    }));
}

fn assert_consumer_keyframe_request(
    events: &[FakeWebRtcEvent],
    expected_consumer_user_id: &UserId,
    expected_source_user_id: &UserId,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerKeyframeRequested {
                consumer_user_id,
                source_user_id,
            } if consumer_user_id == expected_consumer_user_id
                && source_user_id == expected_source_user_id
        )
    }));
}

fn assert_consumer_activity_update(
    events: &[FakeWebRtcEvent],
    expected_consumer_user_id: &UserId,
    expected_source_user_id: &UserId,
    expected_active: bool,
) {
    assert!(events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerActivityUpdated {
                consumer_user_id,
                source_user_id,
                active,
            } if consumer_user_id == expected_consumer_user_id
                && source_user_id == expected_source_user_id
                && *active == expected_active
        )
    }));
}

async fn assert_subscription_layout(
    room: &Arc<Room>,
    adapter: &RuntimeTransportAdapter,
    consumer_user_id: &UserId,
    stream_type: StreamType,
    expected_role: DiagnosticsVideoLayoutRole,
    expected_priority: DiagnosticsVideoRoutePriority,
) {
    let diagnostics = room.diagnostics_user_views(adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.stream_type == stream_type
                && subscription.layout_role == Some(expected_role)
                && subscription.layout_priority == Some(expected_priority)
        }),
        "diagnostics should expose the subscription layout role and priority"
    );
}

#[tokio::test]
async fn dominant_speaker_camera_policy_clears_only_the_observed_speakers_gate() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    for user_id in [UserId::Integer(1), UserId::Integer(2)] {
        publish_audio_and_camera(&room, &user_id, &adapter).await;
    }

    let (first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;
    let (second_audio_media_id, _second_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(2)).await;

    let baseline_event_count = fake.snapshot_events().len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        second_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(2), UserInfo::default(), false, &adapter)
        .await;

    let events = fake.snapshot_events();
    let speaker_two_events = &events[baseline_event_count..];
    assert_consumer_packet_selection_update(
        speaker_two_events,
        &UserId::Integer(1),
        &UserId::Integer(2),
        "hi",
    );
    assert_consumer_packet_selection_update(
        speaker_two_events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "hi",
    );

    let second_baseline_event_count = events.len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
        .await;

    let events = fake.snapshot_events();
    let speaker_one_events = &events[second_baseline_event_count..];
    assert_consumer_packet_selection_update(
        speaker_one_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_packet_selection_update(
        speaker_one_events,
        &UserId::Integer(3),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_packet_selection_update(
        speaker_one_events,
        &UserId::Integer(1),
        &UserId::Integer(2),
        "lo",
    );
    assert_consumer_packet_selection_update(
        speaker_one_events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "lo",
    );
}

#[tokio::test]
async fn active_speaker_camera_policy_clears_only_the_first_five_speakers_gates() {
    let (room, adapter, fake) = setup_ready_users_with_fake(&[1, 2, 3, 4, 5, 6, 7]).await;
    for raw_user_id in 1_i64..=6 {
        publish_audio_and_camera(&room, &UserId::Integer(raw_user_id), &adapter).await;
    }

    let mut ordered_audio_media_ids = Vec::new();
    for raw_user_id in 1_i64..=6 {
        let (audio_media_id, _camera_media_id) =
            source_media_ids(&room, &UserId::Integer(raw_user_id)).await;
        ordered_audio_media_ids.push(audio_media_id);
    }

    let baseline_event_count = fake.snapshot_events().len();
    fake.set_active_speaker_source_snapshot(
        ordered_audio_media_ids
            .iter()
            .rev()
            .copied()
            .map(|transport_media_id| ActiveSpeakerSource::new(transport_media_id, Instant::now()))
            .collect(),
    );
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(6), UserInfo::default(), false, &adapter)
        .await;

    let events = fake.snapshot_events();
    let active_speaker_events = &events[baseline_event_count..];
    for raw_user_id in 2_i64..=6 {
        assert_consumer_packet_selection_update(
            active_speaker_events,
            &UserId::Integer(7),
            &UserId::Integer(raw_user_id),
            "hi",
        );
    }
}

#[tokio::test]
async fn pinned_camera_layout_overrides_active_speaker_bias_for_that_receiver() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_audio_and_camera(&room, &UserId::Integer(1), &adapter).await;
    publish_audio_and_camera(&room, &UserId::Integer(3), &adapter).await;
    let (_first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;
    let (third_audio_media_id, _third_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(3)).await;

    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        third_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(3), UserInfo::default(), false, &adapter)
        .await;

    let baseline_event_count = fake.snapshot_events().len();
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera_layout: Some(VideoLayoutIntent::Pinned),
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let layout_events = &events[baseline_event_count..];
    assert_consumer_packet_selection_update(
        layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
}

#[tokio::test]
async fn hidden_camera_layout_suppresses_active_speaker_featured_quality() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_audio_and_camera(&room, &UserId::Integer(1), &adapter).await;
    let (first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;

    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
        .await;

    let baseline_event_count = fake.snapshot_events().len();
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera_layout: Some(VideoLayoutIntent::Hidden),
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let layout_events = &events[baseline_event_count..];
    assert_consumer_packet_selection_update(
        layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
}

#[tokio::test]
async fn explicit_visible_thumbnail_camera_layout_stays_on_thumbnail_quality() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_audio_and_camera(&room, &UserId::Integer(1), &adapter).await;
    let (first_audio_media_id, _first_camera_media_id) =
        source_media_ids(&room, &UserId::Integer(1)).await;

    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    room.test_api()
        .lifecycle()
        .update_user_info_runtime(&UserId::Integer(1), UserInfo::default(), false, &adapter)
        .await;

    let baseline_event_count = fake.snapshot_events().len();
    room.test_api()
        .media()
        .update_subscription(
            &UserId::Integer(2),
            &UserId::Integer(1),
            &DownloadStates {
                camera_layout: Some(VideoLayoutIntent::VisibleThumbnail),
                ..DownloadStates::default()
            },
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let layout_events = &events[baseline_event_count..];
    assert_consumer_packet_selection_update(
        layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
}

#[tokio::test]
async fn screen_share_layout_uses_screen_specific_priority_in_diagnostics() {
    let (room, adapter, _fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Screen,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    assert_subscription_layout(
        &room,
        &adapter,
        &UserId::Integer(2),
        StreamType::Screen,
        DiagnosticsVideoLayoutRole::ScreenShare,
        DiagnosticsVideoRoutePriority::ScreenShare,
    )
    .await;
}

#[tokio::test]
async fn explicit_unpublish_preserves_state_when_transport_cleanup_fails() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_track(
                &scenario.publisher_user_id,
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                &scenario.transport_adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut scenario.subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );

    let Some(connection_id) = scenario
        .room
        .test_api()
        .inspect()
        .user_connection_id(&scenario.publisher_user_id)
        .await
    else {
        panic!("publisher connection should exist");
    };
    let Some(transport_media_id) = scenario
        .room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &scenario.publisher_user_id,
            connection_id,
            StreamType::Audio,
        )
        .await
    else {
        panic!("published audio should expose a transport media id");
    };
    let transport_user_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, connection_id);
    scenario
        .transport_adapter
        .close_session(&transport_user_key)
        .await
        .expect("closing the publisher transport should succeed");

    assert_eq!(
        scenario
            .room
            .unpublish_track(
                &scenario.publisher_user_id,
                connection_id,
                StreamType::Audio,
                &scenario.transport_adapter,
            )
            .await,
        UnpublishOutcome::TransportCleanupFailed,
        "unpublish should abort when transport cleanup fails"
    );

    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 1);
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);
    assert!(
        scenario
            .room
            .test_api()
            .inspect()
            .has_producer_route_target(
                &scenario.publisher_user_id,
                connection_id,
                StreamType::Audio,
            )
            .await
    );
    assert!(
        scenario
            .room
            .test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert!(drain_outbound(&mut scenario.subscriber_rx).is_empty());
}

#[tokio::test]
async fn publish_track_uses_negotiated_consumer_rtp_parameters() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    assert!(
        set_client_rtp_capabilities(
            &room,
            &UserId::Integer(2),
            test_client_rtp_capabilities_without_video_rtx(),
        )
        .await
        .session_present
    );

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    assert!(drain_outbound(&mut rx1).is_empty());
    let request = drain_outbound(&mut rx2)
        .into_iter()
        .find_map(|message| match message {
            UserOutbound::Request(request) => Some(*request),
            UserOutbound::Message(_)
            | UserOutbound::TrackBindingUpdate(_)
            | UserOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let RoomEventRequest::BootstrapRemoteTrack(payload) = request;
    let codecs = payload.rtp_parameters().codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].codec_name(), "VP8");
    assert_eq!(codecs[0].payload_type(), 96);
}

#[tokio::test]
async fn user_replacement_purges_stale_published_media_state() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    let producer_id = room
        .test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    assert!(producer_id.is_some());
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_)))
    );
    let published_transport_media_id = room
        .test_api()
        .inspect()
        .first_published_transport_media_id()
        .await;
    assert!(published_transport_media_id.is_some());

    assert_eq!(room.test_api().inspect().producer_count().await, 1);
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert!(
        room.test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                StreamType::Camera,
            )
            .await
    );

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(1),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    let published_transport_media_id =
        published_transport_media_id.expect("published track should have a transport id");
    assert_transport_media_mapping_is_missing(&room, published_transport_media_id).await;
    assert_transport_media_owner_mapping_is_missing(&room, published_transport_media_id).await;
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                StreamType::Camera,
            )
            .await
    );
}

#[tokio::test]
async fn user_replacement_purges_all_published_stream_mappings() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(
        room.test_api()
            .media()
            .publish_track(
                &UserId::Integer(1),
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_eq!(
        drain_outbound(&mut subscriber_rx)
            .into_iter()
            .filter(|message| matches!(message, UserOutbound::Request(_)))
            .count(),
        2,
        "subscriber should receive one bootstrap per published stream"
    );

    let camera_transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            StreamType::Camera,
        )
        .await;
    let audio_transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            StreamType::Audio,
        )
        .await;
    assert!(camera_transport_media_id.is_some());
    assert!(audio_transport_media_id.is_some());

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        room.test_api()
            .lifecycle()
            .join_user(
                UserId::Integer(1),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);
    assert_transport_media_mapping_is_missing(
        &room,
        camera_transport_media_id.expect("camera producer should expose a transport id"),
    )
    .await;
    assert_transport_media_mapping_is_missing(
        &room,
        audio_transport_media_id.expect("audio producer should expose a transport id"),
    )
    .await;
    assert_user_has_no_producer_route_target(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        StreamType::Camera,
    )
    .await;
    assert_user_has_no_producer_route_target(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        StreamType::Audio,
    )
    .await;
}

#[tokio::test]
async fn publish_track_releases_room_lock_while_waiting_on_transport_adapter() {
    let (room, _adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let (fake_transport_adapter, _) = fake_adapter();
    let fake = fake_transport_adapter
        .as_fake_adapter()
        .expect("expected fake transport adapter");
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = fake_transport_adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        room.test_api()
            .lifecycle()
            .update_user_info(
                &UserId::Integer(2),
                UserInfo {
                    is_talking: Some(true),
                    ..UserInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "user info update should not wait for publish transport declaration"
    );

    assert!(publish_task.await.unwrap().is_some());
    assert!(
        drain_outbound(&mut rx1).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut rx2).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "user should still receive the concurrent info broadcast"
    );
}

#[tokio::test]
async fn publish_track_defers_producer_commit_until_transport_publish_succeeds() {
    let (room, adapter, fake, _rx1, _rx2) = setup_two_ready_users_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(room.test_api().inspect().producer_count().await, 0);

    assert!(publish_task.await.unwrap().is_some());

    assert_eq!(room.test_api().inspect().producer_count().await, 1);
    let transport_media_id = room
        .test_api()
        .inspect()
        .first_published_transport_media_id()
        .await;
    assert!(transport_media_id.is_some());
    assert_eq!(
        room.test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(
                transport_media_id.expect("published track should have a transport id")
            )
            .await,
        Some(StreamType::Camera)
    );
    assert!(
        room.test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                StreamType::Camera,
            )
            .await
    );
}

#[tokio::test]
async fn publish_track_cleans_up_transport_media_when_user_leaves_mid_publish() {
    let (room, adapter, fake, _rx1, _rx2) = setup_two_ready_users_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.test_api()
            .lifecycle()
            .leave_user(&UserId::Integer(1), test_connection_id(0))
            .await
    );
    assert!(publish_task.await.unwrap().is_none());

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                user_id: UserId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_updates_screen_sharing_info() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Screen,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;

    // Drain bootstrap messages.
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    // Pause screen sharing.
    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Screen, false, &adapter)
        .await;

    let msgs = drain_outbound(&mut rx1);
    if let UserOutbound::Message(RoomEventMessage::UserInfoChanged(snapshot)) = &msgs[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_screen_sharing_on, Some(false));
    } else {
        panic!("expected UserInfoChanged for screen sharing");
    }
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ProducerActivityUpdated {
                user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_commits_user_state_before_transport_update_finishes() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    room.test_api()
        .media()
        .publish_track(
            &UserId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    fake.set_producer_active_delay(Some(Duration::from_millis(200)));

    let update_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = adapter.clone();
        async move {
            room.test_api()
                .media()
                .set_publication_active(&UserId::Integer(1), StreamType::Camera, false, &adapter)
                .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ProducerActivityUpdated {
                user_id: UserId::Integer(1),
                active: false,
            }
        )
    })
    .await;

    let Some((_, info)) = room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(1))
        .await
    else {
        panic!("publisher user should still be present");
    };
    assert_eq!(info.is_camera_on, Some(false));

    update_task.await.unwrap();
}

#[tokio::test]
async fn late_join_bootstrap_releases_room_lock_while_waiting_on_transport_adapter() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = transport_adapter.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        room.test_api()
            .lifecycle()
            .update_user_info(
                &UserId::Integer(1),
                UserInfo {
                    is_talking: Some(true),
                    ..UserInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "user info update should not wait for late-join consumer declaration"
    );

    bootstrap_task.await.unwrap();
    assert!(
        drain_outbound(&mut publisher_rx).iter().any(|msg| matches!(
            msg,
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|msg| matches!(
                msg,
                UserOutbound::Message(RoomEventMessage::UserInfoChanged(_))
                    | UserOutbound::Request(_)
            )),
        "late joiner should receive outbound traffic while bootstrap is running"
    );
}

#[tokio::test]
async fn late_join_bootstrap_defers_consumer_commit_until_transport_consume_succeeds() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = transport_adapter.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(room.test_api().inspect().consumer_count().await, 0);

    bootstrap_task.await.unwrap();

    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
}

#[tokio::test]
async fn late_join_bootstrap_cleans_up_transport_media_when_user_leaves_mid_consume() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = transport_adapter.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.test_api()
            .lifecycle()
            .leave_user(&UserId::Integer(2), test_connection_id(1))
            .await
    );
    bootstrap_task.await.unwrap();

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                user_id: UserId::Integer(2),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn in_flight_bootstrap_retry_does_not_duplicate_consumer_or_unpublish_cleanup() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = transport_adapter.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    StreamType::Camera,
                    MediaKind::Video,
                    test_video_rtp_parameters(),
                    &adapter,
                )
                .await
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let _ = refresh_session_consumers(&room, &UserId::Integer(2), &transport_adapter).await;

    assert!(
        publish_task
            .await
            .unwrap_or_else(|error| panic!("publish task should finish: {error}"))
            .is_some()
    );

    let consume_requests = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                FakeWebRtcEvent::ConsumeMediaRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                    media_kind: MediaKind::Video,
                }
            )
        })
        .count();
    assert_eq!(
        consume_requests, 1,
        "late-join retry must not schedule a second consumer consume while publish bootstrap is in flight"
    );
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_eq!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .filter(|message| matches!(message, UserOutbound::Request(_)))
            .count(),
        1,
        "subscriber should receive exactly one bootstrap request for the published track"
    );

    assert_eq!(
        room.unpublish_track(
            &UserId::Integer(1),
            test_connection_id(0),
            StreamType::Camera,
            &transport_adapter
        )
        .await,
        UnpublishOutcome::Unpublished
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);

    let removed_media = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeWebRtcEvent::MediaRemoved { .. }))
        .count();
    assert_eq!(
        removed_media, 2,
        "unpublish should remove exactly the publisher and subscriber transport media after a retried bootstrap"
    );
}

#[tokio::test]
async fn production_change_ignores_unknown_stream_type() {
    let (room, adapter, mut rx1, mut _rx2) = setup_two_ready_users().await;

    // No producer published for audio. PRODUCTION_CHANGE should be a no-op.
    room.test_api()
        .media()
        .set_publication_active(&UserId::Integer(1), StreamType::Audio, false, &adapter)
        .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn client_capabilities_bootstrap_late_join_when_download_connected_first() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let download_update = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    assert!(download_update.session_present);
    assert!(!download_update.became_consumer_ready);

    assert!(
        apply_client_rtp_capabilities(
            &room,
            &UserId::Integer(2),
            user_connection_id(&room, &UserId::Integer(2)).await,
            test_client_rtp_capabilities(),
            &transport_adapter,
        )
        .await
    );
    assert!(
        room.test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(2))
            .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after capabilities make it ready"
    );
    assert!(
        fake.snapshot_events().iter().all(|event| {
            !matches!(
                event,
                FakeWebRtcEvent::ConsumerKeyframeRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                }
            )
        }),
        "fresh bootstraps must not request a keyframe before the refresh answer lands"
    );
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &transport_adapter).await);
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerKeyframeRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
            }
        )
    })
    .await;
}

#[tokio::test]
async fn transport_connect_bootstrap_late_join_when_capabilities_arrive_first() {
    let (room, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let capabilities_update =
        set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
            .await;
    assert!(capabilities_update.session_present);
    assert!(!capabilities_update.became_consumer_ready);
    assert!(
        room.test_api()
            .inspect()
            .session_has_parsed_client_rtp_capabilities(&UserId::Integer(2))
            .await
    );

    assert!(
        apply_consume_transport_ready(
            &room,
            &UserId::Integer(2),
            user_connection_id(&room, &UserId::Integer(2)).await,
            &transport_adapter,
        )
        .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after download connect makes it ready"
    );
    assert!(
        fake.snapshot_events().iter().all(|event| {
            !matches!(
                event,
                FakeWebRtcEvent::ConsumerKeyframeRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                }
            )
        }),
        "fresh bootstraps must not request a keyframe before the refresh answer lands"
    );
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &transport_adapter).await);
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumerKeyframeRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
            }
        )
    })
    .await;
}

#[tokio::test]
async fn refresh_retry_bootstraps_only_missing_consumers_on_real_rtc() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_track(
                &scenario.publisher_user_id,
                StreamType::Camera,
                MediaKind::Video,
                video_rtp_parameters_with_mid("cam-refresh-retry", 22_222),
                &scenario.transport_adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        StreamType::Camera,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);

    let first_refresh_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("first subscriber refresh should stage an rtc offer");

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_track(
                &scenario.publisher_user_id,
                StreamType::Screen,
                MediaKind::Video,
                video_rtp_parameters_with_mid("screen-refresh-retry", 33_333),
                &scenario.transport_adapter,
            )
            .await
            .is_some()
    );
    assert_eq!(
        scenario.room.test_api().inspect().consumer_count().await,
        1,
        "second consumer must stay pending while the first rtc offer awaits an answer"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no second bootstrap should be emitted before the first refresh answer lands"
    );

    settle_refresh_offer(&mut scenario, first_refresh_offer).await;

    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 2);
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        StreamType::Screen,
    );

    let second_refresh_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("retry should stage the deferred rtc offer");
    settle_refresh_offer(&mut scenario, second_refresh_offer).await;

    assert_eq!(
        scenario.room.test_api().inspect().consumer_count().await,
        2,
        "retry pass must not duplicate already-committed consumers"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no new bootstrap should be emitted once every consumer already exists"
    );
}

#[tokio::test]
async fn negotiated_publish_commit_bootstraps_consumers_on_real_rtc() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;
    let Some(publisher_connection_id) = scenario
        .room
        .test_api()
        .inspect()
        .user_connection_id(&scenario.publisher_user_id)
        .await
    else {
        panic!("publisher connection should exist");
    };
    let publisher_session_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, publisher_connection_id);
    let mut publisher_remote = build_remote_rtc(55_101);
    apply_offer_answer(
        &scenario.transport_adapter,
        &publisher_session_key,
        &mut publisher_remote,
        scenario.publisher_initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = scenario
        .transport_adapter
        .publish_media(
            &publisher_session_key,
            MediaKind::Video,
            &o_sfu_router::MediaStream::new(vec![], vec![], vec![]),
        )
        .await
        .expect("protocol publish intent should stage a recv-only media line");
    let publish_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&publisher_session_key)
        .await
        .expect("protocol publish should stage a follow-up offer");
    apply_offer_answer(
        &scenario.transport_adapter,
        &publisher_session_key,
        &mut publisher_remote,
        publish_offer.into_sdp(),
    )
    .await;
    let negotiated_parameters = scenario
        .transport_adapter
        .negotiated_producer_parameters(&publisher_session_key, transport_media_id)
        .await
        .expect("answered protocol publish should expose negotiated producer parameters");

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .publish_negotiated_track(
                &scenario.publisher_user_id,
                NegotiatedPublish {
                    connection_id: publisher_connection_id,
                    stream_type: StreamType::Camera,
                    media_kind: MediaKind::Video,
                    transport_media_id,
                    consumable_rtp_parameters: negotiated_parameters,
                },
                &scenario.transport_adapter,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        StreamType::Camera,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);
}

struct RealRtcRefreshScenario {
    room: Arc<Room>,
    transport_adapter: RuntimeTransportAdapter,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
    publisher_initial_offer: SessionOffer,
    subscriber_session_key: TransportSessionKey,
    publisher_rx: mpsc::UnboundedReceiver<UserOutbound>,
    subscriber_rx: mpsc::UnboundedReceiver<UserOutbound>,
    subscriber_remote: Rtc,
}

async fn setup_real_rtc_refresh_scenario() -> RealRtcRefreshScenario {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", None, &RoomConfig::default(), None)
        .await;
    let (publisher_tx, publisher_rx) = test_sender();
    let (subscriber_tx, subscriber_rx) = test_sender();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let publisher_connection_id = room
        .test_api()
        .lifecycle()
        .join_user(
            publisher_user_id.clone(),
            None,
            UserPermissions::default(),
            publisher_tx,
        )
        .await
        .expect("publisher should join");
    let subscriber_connection_id = room
        .test_api()
        .lifecycle()
        .join_user(
            subscriber_user_id.clone(),
            None,
            UserPermissions::default(),
            subscriber_tx,
        )
        .await
        .expect("subscriber should join");
    let transport_adapter = build_real_rtc_transport_adapter();
    let publisher_session_key =
        room.transport_user_key(&publisher_user_id, publisher_connection_id);
    let subscriber_session_key =
        room.transport_user_key(&subscriber_user_id, subscriber_connection_id);

    let publisher_initial_offer =
        bootstrap_real_rtc_user(&transport_adapter, &publisher_session_key).await;
    let subscriber_initial_offer =
        bootstrap_real_rtc_user(&transport_adapter, &subscriber_session_key).await;
    let mut subscriber_remote = build_remote_rtc(55_100);
    apply_offer_answer(
        &transport_adapter,
        &subscriber_session_key,
        &mut subscriber_remote,
        subscriber_initial_offer.into_sdp(),
    )
    .await;

    assert_eq!(
        room.apply_session_negotiated(
            &publisher_user_id,
            publisher_connection_id,
            test_client_rtp_capabilities(),
            &transport_adapter,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
    assert_eq!(
        room.apply_session_negotiated(
            &subscriber_user_id,
            subscriber_connection_id,
            test_client_rtp_capabilities(),
            &transport_adapter,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );

    RealRtcRefreshScenario {
        room,
        transport_adapter,
        publisher_user_id,
        subscriber_user_id,
        publisher_initial_offer,
        subscriber_session_key,
        publisher_rx,
        subscriber_rx,
        subscriber_remote,
    }
}

async fn settle_refresh_offer(scenario: &mut RealRtcRefreshScenario, offer: SessionOffer) {
    apply_offer_answer(
        &scenario.transport_adapter,
        &scenario.subscriber_session_key,
        &mut scenario.subscriber_remote,
        offer.into_sdp(),
    )
    .await;

    scenario
        .room
        .apply_session_refreshed(
            &scenario.subscriber_user_id,
            user_connection_id(&scenario.room, &scenario.subscriber_user_id).await,
            &scenario.transport_adapter,
        )
        .await;
}

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        1
    );

    assert_eq!(
        room.rollback_staged_publish(&user_id, connection_id, StreamType::Camera, &adapter)
            .await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(drain_outbound(&mut subscriber_rx).is_empty());

    let events = fake.snapshot_events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::PublishMediaRequested { user_id: owner, .. }
                if *owner == user_id
        )),
        "staging should declare producer media on the transport"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::MediaRemoved { user_id: owner, .. }
                if *owner == user_id
        )),
        "rolling back a staged publish should remove the staged transport media"
    );
}

#[tokio::test]
async fn duplicate_staged_publish_is_ignored_before_transport_reservation() {
    let (room, adapter, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert_eq!(
        room.stage_negotiated_publish(&user_id, connection_id, StreamType::Camera, &adapter)
            .await
            .expect("first stage should not hit transport failure"),
        crate::PublishStageOutcome::Staged
    );
    assert_eq!(
        room.stage_negotiated_publish(&user_id, connection_id, StreamType::Camera, &adapter)
            .await
            .expect("duplicate stage should not hit transport failure"),
        crate::PublishStageOutcome::Duplicate
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        1
    );
    assert_eq!(
        fake.snapshot_events()
            .iter()
            .filter(|event| matches!(
                event,
                FakeWebRtcEvent::PublishMediaRequested { user_id: owner, .. }
                    if *owner == user_id
            ))
            .count(),
        1,
        "the pre-await duplicate check should avoid reserving a second transport media"
    );
    assert_eq!(
        room.rollback_staged_publish(&user_id, connection_id, StreamType::Camera, &adapter)
            .await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
}

#[tokio::test]
async fn explicit_unpublish_missing_publication_is_a_domain_noop() {
    let (room, adapter, _fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert_eq!(
        room.unpublish_track(&user_id, connection_id, StreamType::Camera, &adapter)
            .await,
        UnpublishOutcome::MissingPublication
    );
}

#[tokio::test]
async fn staged_publish_rollback_reports_cleanup_failure_without_state_ownership() {
    let (room, adapter, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    let transport_media_id =
        staged_publish_transport_media_id(&room, &user_id, connection_id, StreamType::Camera)
            .await
            .expect("staged publish should expose its transport media id");
    fake.fail_next_remove_media(transport_media_id);

    assert_eq!(
        room.rollback_staged_publish(&user_id, connection_id, StreamType::Camera, &adapter)
            .await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Failed
        }
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
}

#[tokio::test]
async fn staged_negotiated_publish_commit_moves_through_room_owned_transaction() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        1
    );

    commit_staged_publishes(&room, &user_id, connection_id, &adapter).await;

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert!(room.is_stream_published(&user_id, StreamType::Camera).await);
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_bootstrap_for_stream(&drain_outbound(&mut subscriber_rx), StreamType::Camera);
    assert!(
        !fake.snapshot_events().iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::MediaRemoved { user_id: owner, .. }
                if *owner == user_id
        )),
        "successful commit should not compensate the staged producer media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_materializes_all_negotiated_encodings() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    let transport_media_id =
        staged_publish_transport_media_id(&room, &user_id, connection_id, StreamType::Camera)
            .await
            .expect("staged publish should expose its transport media id");
    fake.set_negotiated_producer_parameters(
        transport_media_id,
        test_simulcast_video_rtp_parameters(),
    );

    commit_staged_publishes(&room, &user_id, connection_id, &adapter).await;

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert!(room.is_stream_published(&user_id, StreamType::Camera).await);
    assert_eq!(
        room.test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await
            .expect("transport media should resolve to source encodings")
            .len(),
        2
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    let subscriber_messages = drain_outbound(&mut subscriber_rx);
    assert_bootstrap_for_stream(&subscriber_messages, StreamType::Camera);
    assert!(
        subscriber_messages.iter().any(|message| matches!(
            message,
            UserOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    RoomEventRequest::BootstrapRemoteTrack(payload)
                        if payload.source_descriptor().encodings().count() == 2
                )
        )),
        "consumer bootstrap should carry the full committed source encoding graph"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_cleans_up_when_transport_parameters_are_missing() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    let transport_media_id =
        staged_publish_transport_media_id(&room, &user_id, connection_id, StreamType::Camera)
            .await
            .expect("staged publish should expose its transport media id");
    fake.clear_negotiated_producer_parameters(transport_media_id);

    commit_staged_publishes(&room, &user_id, connection_id, &adapter).await;

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert!(!room.is_stream_published(&user_id, StreamType::Camera).await);
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(drain_outbound(&mut subscriber_rx).is_empty());
    assert!(
        fake.snapshot_events().iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                user_id: owner,
                transport_media_id: removed_media_id,
            } if *owner == user_id && *removed_media_id == transport_media_id
        )),
        "commit should clean up the staged transport media when negotiated parameters are unavailable"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_cleans_up_when_user_state_rejects_it() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    assert!(
        room.test_api()
            .lifecycle()
            .leave_session_without_transport_cleanup(&user_id, connection_id, &adapter)
            .await
    );
    let _ = drain_outbound(&mut publisher_rx);
    let _ = drain_outbound(&mut subscriber_rx);

    commit_staged_publishes(&room, &user_id, connection_id, &adapter).await;

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert!(!room.test_api().inspect().has_session(&user_id).await);
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(drain_outbound(&mut subscriber_rx).is_empty());
    assert!(
        fake.snapshot_events().iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::MediaRemoved { user_id: owner, .. }
                if *owner == user_id
        )),
        "commit rejection should clean up the staged transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_rejects_replaced_connection() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    let transport_media_id =
        staged_publish_transport_media_id(&room, &user_id, connection_id, StreamType::Camera)
            .await
            .expect("staged publish should expose its transport media id");
    let (replacement_sender, _replacement_rx) = test_sender();
    let replacement_connection_id = room
        .test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            user_id.clone(),
            None,
            UserPermissions::default(),
            replacement_sender,
            &adapter,
        )
        .await
        .expect("replacement user should join");
    let _ = drain_outbound(&mut publisher_rx);
    let _ = drain_outbound(&mut subscriber_rx);

    commit_staged_publishes(&room, &user_id, connection_id, &adapter).await;

    assert_ne!(replacement_connection_id, connection_id);
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert!(!room.is_stream_published(&user_id, StreamType::Camera).await);
    assert_eq!(
        room.test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await,
        None
    );
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(drain_outbound(&mut subscriber_rx).is_empty());
    assert!(
        fake.snapshot_events().iter().any(|event| matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                user_id: owner,
                transport_media_id: removed_media_id,
            } if *owner == user_id && *removed_media_id == transport_media_id
        )),
        "stale replaced publish commit should clean up the staged transport media"
    );
}

#[tokio::test]
async fn staged_publish_connection_cleanup_rolls_back_every_staged_stream() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
    assert!(
        stage_negotiated_publish(&room, &user_id, connection_id, StreamType::Screen, &adapter,)
            .await
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        2
    );

    rollback_staged_publishes_for_connection(&room, &user_id, connection_id, &adapter).await;

    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        0
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert!(drain_outbound(&mut subscriber_rx).is_empty());
    assert_eq!(
        fake.snapshot_events()
            .iter()
            .filter(|event| matches!(
                event,
                FakeWebRtcEvent::MediaRemoved { user_id: owner, .. }
                    if *owner == user_id
            ))
            .count(),
        2,
        "connection cleanup should remove every staged publish transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_duplicate_race_keeps_one_staged_entry_and_one_cleanup() {
    let (room, adapter, fake, _publisher_rx, _subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));
    let user_id = UserId::Integer(1);
    let connection_id = room
        .test_api()
        .inspect()
        .user_connection_id(&user_id)
        .await
        .expect("publisher should have a live connection");

    let (first_stage, second_stage) = tokio::join!(
        room.stage_negotiated_publish(&user_id, connection_id, StreamType::Camera, &adapter,),
        room.stage_negotiated_publish(&user_id, connection_id, StreamType::Camera, &adapter,),
    );

    let outcomes = [
        first_stage.expect("first stage attempt should not hit transport failure"),
        second_stage.expect("second stage attempt should not hit transport failure"),
    ];
    assert!(outcomes.contains(&crate::PublishStageOutcome::Staged));
    assert!(
        outcomes.contains(&crate::PublishStageOutcome::DuplicateAfterReservation {
            cleanup: crate::TransportEffectOutcome::Applied,
        })
    );
    assert_eq!(
        staged_publish_count(&room, &user_id, connection_id).await,
        1
    );

    let events = fake.snapshot_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                FakeWebRtcEvent::PublishMediaRequested { user_id: owner, .. }
                    if *owner == user_id
            ))
            .count(),
        2,
        "both racing stage attempts should declare transport media before the post-await duplicate re-check"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                FakeWebRtcEvent::MediaRemoved { user_id: owner, .. }
                    if *owner == user_id
            ))
            .count(),
        1,
        "the duplicate staged transport media should be compensated exactly once"
    );
    assert!(
        rollback_staged_publish(&room, &user_id, connection_id, StreamType::Camera, &adapter,)
            .await
    );
}

#[allow(
    clippy::panic,
    reason = "the RTC room test fixture uses a fixed valid configuration and should fail loudly if it stops being valid"
)]
fn build_real_rtc_transport_adapter() -> RuntimeTransportAdapter {
    match RtcTransport::builder()
        .transport_config(RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(8_000_000, 10_000_000),
            video_bitrate_limits: crate::VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(46_200, 46_299),
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: crate::CodecPreferences::default(),
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(MediaTap::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
        .worker_count(1)
        .build()
    {
        Ok(transport) => RuntimeTransportAdapter::from_rtc_transport(transport),
        Err(error) => panic!("constant RTC room test transport config should be valid: {error}"),
    }
}

async fn bootstrap_real_rtc_user(
    transport_adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    transport_adapter
        .create_initial_session_offer(session_key)
        .await
        .expect("rtc user should produce an initial offer")
}

fn assert_bootstrap_for_stream(messages: &[UserOutbound], stream_type: StreamType) {
    assert!(
        messages.iter().any(|message| matches!(
            message,
            UserOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    RoomEventRequest::BootstrapRemoteTrack(payload)
                        if payload.stream_type() == stream_type
                )
        )),
        "expected a bootstrap request for {stream_type:?}"
    );
}

fn build_remote_rtc(port: u16) -> Rtc {
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    remote
}

async fn apply_offer_answer(
    adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
    remote: &mut Rtc,
    offer_sdp: String,
) {
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&offer_sdp)
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build");
    assert!(
        adapter
            .apply_session_answer(session_key, &answer.to_sdp_string())
            .await
            .is_ok()
    );
}

fn video_rtp_parameters_with_mid(mid: &str, ssrc: u32) -> MediaStream {
    router_sample_video_rtp_parameters(Some(mid), ssrc)
}
