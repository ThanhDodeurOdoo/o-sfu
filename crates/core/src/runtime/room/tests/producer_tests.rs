use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use o_sfu_router::{
    MediaKind,
    test_support::rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters,
};
use str0m::{Candidate, Rtc, change::SdpOffer};

use super::{api::NegotiatedPublish, fixtures::*};
use crate::{
    Bitrate, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        diagnostics::{
            DiagnosticsPolicyPauseReason, DiagnosticsStore, DiagnosticsVideoLayoutRole,
            DiagnosticsVideoRoutePriority,
        },
        media_transport::{
            MediaTransportDeps, RtcTransport, RtcTransportConfig, SessionOffer, TransportMediaId,
            TransportSessionKey, test_support::FakeMediaTransportEvent,
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        room::Room,
    },
};

fn assert_track_binding_activity_update(
    message: &UserOutbound,
    user_id: &UserId,
    stream_type: TestSourceKind,
    active: Option<bool>,
) {
    match message {
        UserOutbound::TrackBindingUpdate(update) => {
            assert_eq!(&update.user_id, user_id);
            assert_eq!(update.stream_id, stream_id_for_source(stream_type));
            assert_eq!(update.active, active);
        }
        other => panic!("expected TrackBindingUpdate, got {other:?}"),
    }
}

#[tokio::test]
async fn production_change_pauses_producer_and_broadcasts_track_binding() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    let bootstrap_msgs = drain_outbound(&mut rx2);
    assert!(
        bootstrap_msgs
            .iter()
            .any(|m| matches!(m, UserOutbound::Request(..))),
        "user 2 should have received a bootstrap remote track request"
    );
    assert!(drain_outbound(&mut rx1).is_empty());

    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "user 1 should get track binding update");
    assert_eq!(msgs2.len(), 1, "user 2 should get track binding update");
    assert_track_binding_activity_update(
        &msgs1[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(false),
    );
    assert_track_binding_activity_update(
        &msgs2[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(false),
    );

    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Active,
        &adapter,
    )
    .await;

    let msgs1 = drain_outbound(&mut rx1);
    assert_track_binding_activity_update(
        &msgs1[0],
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(true),
    );
}

#[tokio::test]
async fn explicit_unpublish_removes_published_track_and_consumer_routes() {
    let (room, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
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
            TestSourceKind::ScalableVideo,
        )
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert_eq!(
        room.unpublish_track(
            &UserId::Integer(1),
            test_connection_id(0),
            &stream_id_for_source(TestSourceKind::ScalableVideo),
            &adapter,
        )
        .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
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
                TestSourceKind::ScalableVideo,
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
                && update.stream_id == stream_id_for_source(TestSourceKind::ScalableVideo)
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        UserOutbound::TrackBindingUpdate(update)
            if update.user_id == UserId::Integer(1)
                && update.stream_id == stream_id_for_source(TestSourceKind::ScalableVideo)
                && update.active.is_none()
    )));
    let removed_media_events = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeMediaTransportEvent::MediaRemoved { .. }))
        .count();
    assert_eq!(removed_media_events, 2);
}

#[tokio::test]
async fn multiparty_camera_publish_installs_the_initial_simulcast_selection() {
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;

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
    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;
    publish_simulcast_camera(&room, &UserId::Integer(2), &adapter).await;

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

    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;
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

    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;
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
    let (room, adapter, fake) = setup_three_ready_users_with_fake().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;

    let baseline_event_count = fake.snapshot_events().len();
    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(3),
            test_connection_id(2),
            UserCleanup::state_only(Some(&adapter)),
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
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera(1).await;
    scenario.mark_user_active_speaker(1).await;
    scenario.refresh_policy().await;
    assert_consumer_packet_selection_update(
        &scenario.events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );

    scenario.set_receiver_budget(2, 100_000);
    let baseline_event_count = scenario.event_cursor();
    scenario.refresh_policy_times(2).await;

    let events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
    assert_consumer_keyframe_request(&events, &UserId::Integer(2), &UserId::Integer(1));
}

#[tokio::test]
async fn receiver_bandwidth_recovery_upswitches_conservatively_with_keyframe() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera(1).await;
    scenario.mark_user_active_speaker(1).await;
    scenario.set_receiver_budget(2, 100_000);
    scenario.refresh_policy_times(2).await;
    assert_consumer_packet_selection_update(
        &scenario.events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );

    scenario.set_receiver_budget(2, 2_000_000);
    let baseline_event_count = scenario.event_cursor();
    scenario.refresh_policy_times(3).await;

    let recovery_events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &recovery_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_keyframe_request(&recovery_events, &UserId::Integer(2), &UserId::Integer(1));
}

#[tokio::test]
async fn receiver_budget_pauses_visible_thumbnail_after_cheapest_layers_do_not_fit() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2, 3, 4]).await;
    scenario.publish_simulcast_cameras(&[1, 3, 4]).await;

    scenario.set_receiver_budget(2, 200_000);
    let baseline_event_count = scenario.event_cursor();
    scenario.refresh_policy_times(2).await;

    let events = scenario.events_since(baseline_event_count);
    assert_consumer_activity_update(&events, &UserId::Integer(2), &UserId::Integer(1), false);
    let diagnostics = scenario
        .room
        .diagnostics_user_views(&scenario.adapter)
        .await;
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
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2, 3, 4]).await;
    scenario.publish_simulcast_cameras(&[1, 3, 4]).await;
    scenario.set_receiver_budget(2, 200_000);
    scenario.refresh_policy_times(2).await;
    assert_consumer_activity_update(
        &scenario.events(),
        &UserId::Integer(2),
        &UserId::Integer(1),
        false,
    );

    scenario.set_receiver_budget(2, 1_000_000);
    let baseline_event_count = scenario.event_cursor();
    scenario.refresh_policy_times(3).await;

    let recovery_events = scenario.events_since(baseline_event_count);
    assert_consumer_activity_update(
        &recovery_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        true,
    );
    assert_consumer_keyframe_request(&recovery_events, &UserId::Integer(2), &UserId::Integer(1));
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
    stream_type: TestSourceKind,
) {
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(user_id, connection_id, stream_type)
            .await
    );
}

async fn assert_subscription_layout(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    stream_type: TestSourceKind,
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
            subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.layout_role == Some(expected_role)
                && subscription.layout_priority == Some(expected_priority)
        }),
        "diagnostics should expose the subscription layout role and priority"
    );
}

#[tokio::test]
async fn dominant_speaker_camera_policy_clears_only_the_observed_speakers_gate() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera_for_users(&[1, 2]).await;

    let first_audio_media_id = scenario.audio_media_id(1).await;
    let second_audio_media_id = scenario.audio_media_id(2).await;

    let baseline_event_count = scenario.event_cursor();
    scenario.mark_active_speaker(second_audio_media_id);
    scenario.refresh_policy().await;

    let speaker_two_events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &speaker_two_events,
        &UserId::Integer(1),
        &UserId::Integer(2),
        "hi",
    );
    assert_consumer_packet_selection_update(
        &speaker_two_events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "hi",
    );

    let second_baseline_event_count = scenario.event_cursor();
    scenario.mark_active_speaker(first_audio_media_id);
    scenario.refresh_policy().await;

    let speaker_one_events = scenario.events_since(second_baseline_event_count);
    assert_consumer_packet_selection_update(
        &speaker_one_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_packet_selection_update(
        &speaker_one_events,
        &UserId::Integer(3),
        &UserId::Integer(1),
        "hi",
    );
    assert_consumer_packet_selection_update(
        &speaker_one_events,
        &UserId::Integer(1),
        &UserId::Integer(2),
        "lo",
    );
    assert_consumer_packet_selection_update(
        &speaker_one_events,
        &UserId::Integer(3),
        &UserId::Integer(2),
        "lo",
    );
}

#[tokio::test]
async fn active_speaker_camera_policy_clears_only_the_first_five_speakers_gates() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2, 3, 4, 5, 6, 7]).await;
    scenario
        .publish_audio_and_camera_for_users(&[1, 2, 3, 4, 5, 6])
        .await;

    let ordered_audio_media_ids = scenario.audio_media_ids(&[1, 2, 3, 4, 5, 6]).await;

    let baseline_event_count = scenario.event_cursor();
    scenario.mark_active_speakers(ordered_audio_media_ids.iter().rev().copied());
    scenario.refresh_policy().await;

    let active_speaker_events = scenario.events_since(baseline_event_count);
    for raw_user_id in 2_i64..=6 {
        assert_consumer_packet_selection_update(
            &active_speaker_events,
            &UserId::Integer(7),
            &UserId::Integer(raw_user_id),
            "hi",
        );
    }
}

#[tokio::test]
async fn pinned_camera_layout_overrides_active_speaker_bias_for_that_receiver() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let third_audio_media_id = scenario.audio_media_id(3).await;

    scenario.mark_active_speaker(third_audio_media_id);
    scenario.refresh_policy().await;

    let baseline_event_count = scenario.event_cursor();
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Pinned)
        .await;

    let layout_events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "hi",
    );
}

#[tokio::test]
async fn hidden_camera_layout_suppresses_active_speaker_featured_quality() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio_media_id = scenario.audio_media_id(1).await;

    scenario.mark_active_speaker(first_audio_media_id);
    scenario.refresh_policy().await;

    let baseline_event_count = scenario.event_cursor();
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Hidden)
        .await;

    let layout_events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
}

#[tokio::test]
async fn explicit_visible_thumbnail_camera_layout_stays_on_thumbnail_quality() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio_media_id = scenario.audio_media_id(1).await;

    scenario.mark_active_speaker(first_audio_media_id);
    scenario.refresh_policy().await;

    let baseline_event_count = scenario.event_cursor();
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::VisibleThumbnail)
        .await;

    let layout_events = scenario.events_since(baseline_event_count);
    assert_consumer_packet_selection_update(
        &layout_events,
        &UserId::Integer(2),
        &UserId::Integer(1),
        "lo",
    );
}

#[tokio::test]
async fn screen_share_layout_uses_screen_specific_priority_in_diagnostics() {
    let (room, adapter, _fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    assert_subscription_layout(
        &room,
        &adapter,
        &UserId::Integer(2),
        TestSourceKind::ReadableVideo,
        DiagnosticsVideoLayoutRole::ReadableDetail,
        DiagnosticsVideoRoutePriority::ReadableDetail,
    )
    .await;
}

#[tokio::test]
async fn explicit_unpublish_removes_state_when_transport_cleanup_fails() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    publish_track(
        &scenario.room,
        &scenario.publisher_user_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &scenario.media_transport,
    )
    .await;
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
            TestSourceKind::AudioDetector,
        )
        .await
    else {
        panic!("published audio should expose a transport media id");
    };
    let transport_user_key = scenario
        .room
        .transport_user_key(&scenario.publisher_user_id, connection_id);
    scenario
        .media_transport
        .close_session(&transport_user_key)
        .await
        .expect("closing the publisher transport should succeed");

    assert_eq!(
        scenario
            .room
            .unpublish_track(
                &scenario.publisher_user_id,
                connection_id,
                &stream_id_for_source(TestSourceKind::AudioDetector),
                &scenario.media_transport,
            )
            .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Failed
        },
        "unpublish should commit room state and queue failed transport cleanup"
    );

    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 0);
    assert_user_has_no_producer_route_target(
        &scenario.room,
        &scenario.publisher_user_id,
        connection_id,
        TestSourceKind::AudioDetector,
    )
    .await;
    assert_transport_media_mapping_is_missing(&scenario.room, transport_media_id).await;
    assert!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count()
            > 0
    );
    assert!(
        drain_outbound(&mut scenario.publisher_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::TrackBindingUpdate(_)))
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::TrackBindingUpdate(_)))
    );
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

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
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

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
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
                TestSourceKind::ScalableVideo,
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
                TestSourceKind::ScalableVideo,
            )
            .await
    );
}

#[tokio::test]
async fn user_replacement_purges_all_published_stream_mappings() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &adapter,
    )
    .await;
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
            TestSourceKind::ScalableVideo,
        )
        .await;
    let audio_transport_media_id = room
        .test_api()
        .inspect()
        .producer_transport_media_id(
            &UserId::Integer(1),
            test_connection_id(0),
            TestSourceKind::AudioDetector,
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
        TestSourceKind::ScalableVideo,
    )
    .await;
    assert_user_has_no_producer_route_target(
        &room,
        &UserId::Integer(1),
        test_connection_id(0),
        TestSourceKind::AudioDetector,
    )
    .await;
}

#[tokio::test]
async fn publish_track_releases_room_lock_while_waiting_on_media_transport() {
    let (room, _adapter, mut rx1, mut rx2) = setup_two_ready_users().await;
    let (fake_media_transport, _) = fake_adapter();
    let fake = fake_media_transport
        .as_fake_transport()
        .expect("expected fake media transport");
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = fake_media_transport.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
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
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        let user_id = UserId::Integer(2);
        room.update_user_info(
            &user_id,
            user_connection_id(&room, &user_id).await,
            UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
            UserInfoRefresh::NotNeeded,
            &fake_media_transport,
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
                    TestSourceKind::ScalableVideo,
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
            FakeMediaTransportEvent::PublishMediaRequested {
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
        Some(TestSourceKind::ScalableVideo)
    );
    assert!(
        room.test_api()
            .inspect()
            .has_producer_route_target(
                &UserId::Integer(1),
                test_connection_id(0),
                TestSourceKind::ScalableVideo,
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
                    TestSourceKind::ScalableVideo,
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
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(1),
            test_connection_id(0),
            UserCleanup::state_only(None),
        )
        .await
    );
    assert!(publish_task.await.unwrap().is_none());

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved {
                user_id: UserId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_updates_screen_track_binding_activity() {
    let (room, adapter, mut rx1, mut rx2) = setup_two_ready_users().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;

    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ReadableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    let msgs = drain_outbound(&mut rx1);
    assert_track_binding_activity_update(
        &msgs[0],
        &UserId::Integer(1),
        TestSourceKind::ReadableVideo,
        Some(false),
    );
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (room, adapter, fake, mut rx1, mut rx2) = setup_two_ready_users_with_fake().await;

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ProducerActivityUpdated {
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

    publish_track(
        &room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
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
        let publisher_connection_id = user_connection_id(&room, &UserId::Integer(1)).await;
        async move {
            room.set_publication_active_runtime(
                &UserId::Integer(1),
                publisher_connection_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                PublicationActivity::Inactive,
                &adapter,
            )
            .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ProducerActivityUpdated {
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
async fn late_join_bootstrap_releases_room_lock_while_waiting_on_media_transport() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        let user_id = UserId::Integer(1);
        room.update_user_info(
            &user_id,
            user_connection_id(&room, &user_id).await,
            UserInfo {
                is_talking: Some(true),
                ..UserInfo::default()
            },
            UserInfoRefresh::NotNeeded,
            &media_transport,
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
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
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
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(2),
            test_connection_id(1),
            UserCleanup::state_only(None),
        )
        .await
    );
    bootstrap_task.await.unwrap();

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::MediaRemoved {
                user_id: UserId::Integer(2),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn late_join_bootstrap_queues_transport_cleanup_retry_when_commit_cleanup_fails() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let _ = set_consume_transport_ready(&room, &UserId::Integer(2)).await;
    let _ = set_client_rtp_capabilities(&room, &UserId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            let _ = refresh_session_consumers(&room, &UserId::Integer(2), &adapter).await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let consumer_transport_media_id = fake.next_transport_media_id();
    fake.fail_remove_media_until_allowed(consumer_transport_media_id);
    assert!(
        room.remove_user_with_cleanup(
            &UserId::Integer(2),
            test_connection_id(1),
            UserCleanup::state_only(None),
        )
        .await
    );
    bootstrap_task.await.unwrap();

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 1);
    assert!(!fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            user_id: UserId::Integer(2),
            transport_media_id,
        } if *transport_media_id == consumer_transport_media_id
    )));

    fake.allow_remove_media(consumer_transport_media_id);
    room.test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&media_transport)
        .await;

    assert_eq!(room.test_api().lifecycle().pending_cleanup_retry_count(), 0);
    assert!(fake.snapshot_events().iter().any(|event| matches!(
        event,
        FakeMediaTransportEvent::MediaRemoved {
            user_id: UserId::Integer(2),
            transport_media_id,
        } if *transport_media_id == consumer_transport_media_id
    )));
}

#[tokio::test]
async fn in_flight_bootstrap_retry_does_not_duplicate_consumer_or_unpublish_cleanup() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_fake().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let room = Arc::clone(&room);
        let adapter = media_transport.clone();
        async move {
            room.test_api()
                .media()
                .publish_track(
                    &UserId::Integer(1),
                    TestSourceKind::ScalableVideo,
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
            FakeMediaTransportEvent::ConsumeMediaRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let _ = refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await;

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
                FakeMediaTransportEvent::ConsumeMediaRequested {
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
            &stream_id_for_source(TestSourceKind::ScalableVideo),
            &media_transport
        )
        .await,
        UnpublishOutcome::Unpublished {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
    assert_eq!(room.test_api().inspect().producer_count().await, 0);
    assert_eq!(room.test_api().inspect().consumer_count().await, 0);

    let removed_media = fake
        .snapshot_events()
        .into_iter()
        .filter(|event| matches!(event, FakeMediaTransportEvent::MediaRemoved { .. }))
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
    let publisher_id = UserId::Integer(1);
    let publisher_connection_id = user_connection_id(&room, &publisher_id).await;
    room.set_publication_active_runtime(
        &publisher_id,
        publisher_connection_id,
        &stream_id_for_source(TestSourceKind::AudioDetector),
        PublicationActivity::Inactive,
        &adapter,
    )
    .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn client_capabilities_bootstrap_late_join_when_download_connected_first() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
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
            &media_transport,
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
            FakeMediaTransportEvent::ConsumeMediaRequested {
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
                FakeMediaTransportEvent::ConsumerKeyframeRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                }
            )
        }),
        "fresh bootstraps must not request a keyframe before the refresh answer lands"
    );
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await);
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerKeyframeRequested {
                consumer_user_id: UserId::Integer(2),
                source_user_id: UserId::Integer(1),
            }
        )
    })
    .await;
}

#[tokio::test]
async fn transport_connect_bootstrap_late_join_when_capabilities_arrive_first() {
    let (room, media_transport, fake, mut publisher_rx, mut subscriber_rx) =
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
            &media_transport,
        )
        .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumeMediaRequested {
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
                FakeMediaTransportEvent::ConsumerKeyframeRequested {
                    consumer_user_id: UserId::Integer(2),
                    source_user_id: UserId::Integer(1),
                }
            )
        }),
        "fresh bootstraps must not request a keyframe before the refresh answer lands"
    );
    assert!(refresh_session_consumers(&room, &UserId::Integer(2), &media_transport).await);
    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::ConsumerKeyframeRequested {
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
                TestSourceKind::ScalableVideo,
                MediaKind::Video,
                video_rtp_parameters_with_mid("cam-refresh-retry", 22_222),
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);

    let first_refresh_offer = scenario
        .media_transport
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
                TestSourceKind::ReadableVideo,
                MediaKind::Video,
                video_rtp_parameters_with_mid("screen-refresh-retry", 33_333),
                &scenario.media_transport,
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
        TestSourceKind::ReadableVideo,
    );

    let second_refresh_offer = scenario
        .media_transport
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
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        scenario.publisher_initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = scenario
        .media_transport
        .publish_media(
            &publisher_session_key,
            MediaKind::Video,
            &o_sfu_router::MediaStream::new(vec![], vec![], vec![]),
        )
        .await
        .expect("protocol publish intent should stage a recv-only media line");
    let publish_offer = scenario
        .media_transport
        .create_session_renegotiation_offer(&publisher_session_key)
        .await
        .expect("protocol publish should stage a follow-up offer");
    apply_offer_answer(
        &scenario.media_transport,
        &publisher_session_key,
        &mut publisher_remote,
        publish_offer.into_sdp(),
    )
    .await;
    let negotiated_parameters = scenario
        .media_transport
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
                    stream_type: TestSourceKind::ScalableVideo,
                    media_kind: MediaKind::Video,
                    transport_media_id,
                    consumable_rtp_parameters: negotiated_parameters,
                },
                &scenario.media_transport,
            )
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_eq!(scenario.room.test_api().inspect().consumer_count().await, 1);
}

struct RealRtcRefreshScenario {
    room: Arc<Room>,
    media_transport: MediaTransport,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
    publisher_initial_offer: SessionOffer,
    subscriber_session_key: TransportSessionKey,
    publisher_rx: UserOutboundReceiver,
    subscriber_rx: UserOutboundReceiver,
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
    let media_transport = build_real_rtc_media_transport();
    let publisher_session_key =
        room.transport_user_key(&publisher_user_id, publisher_connection_id);
    let subscriber_session_key =
        room.transport_user_key(&subscriber_user_id, subscriber_connection_id);

    let publisher_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &publisher_session_key).await;
    let subscriber_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &subscriber_session_key).await;
    let mut subscriber_remote = build_remote_rtc(55_100);
    apply_offer_answer(
        &media_transport,
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
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
    assert_eq!(
        room.apply_session_negotiated(
            &subscriber_user_id,
            subscriber_connection_id,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );

    RealRtcRefreshScenario {
        room,
        media_transport,
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
        &scenario.media_transport,
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
            &scenario.media_transport,
        )
        .await;
}

#[tokio::test]
async fn staged_negotiated_publish_rollback_cleans_transport_media_without_committing_state() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(scenario.staged_count().await, 1);

    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    scenario.assert_no_outbound();

    assert!(
        scenario.publish_media_requested_count() > 0,
        "staging should declare producer media on the transport"
    );
    assert!(
        scenario.removed_media_count() > 0,
        "rolling back a staged publish should remove the staged transport media"
    );
}

#[tokio::test]
async fn duplicate_staged_publish_is_ignored_before_transport_reservation() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Duplicate
    );
    assert_eq!(scenario.staged_count().await, 1);
    assert_eq!(
        scenario.publish_media_requested_count(),
        1,
        "the pre-await duplicate check should avoid reserving a second transport media"
    );
    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Applied
        }
    );
}

#[tokio::test]
async fn explicit_unpublish_missing_publication_is_a_domain_noop() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.unpublish_scalable_video().await,
        UnpublishOutcome::MissingPublication
    );
}

#[tokio::test]
async fn staged_publish_rollback_reports_cleanup_failure_without_state_ownership() {
    let scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario
        .fake
        .fail_remove_media_until_allowed(transport_media_id);

    assert_eq!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack {
            cleanup: crate::TransportEffectOutcome::Failed
        }
    );
    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert_eq!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count(),
        1
    );
    assert!(!scenario.has_removed_media(transport_media_id));

    scenario.fake.allow_remove_media(transport_media_id);
    scenario
        .room
        .test_api()
        .lifecycle()
        .force_cleanup_retry_cycle(&scenario.adapter)
        .await;

    assert_eq!(
        scenario
            .room
            .test_api()
            .lifecycle()
            .pending_cleanup_retry_count(),
        0
    );
    assert!(scenario.has_removed_media(transport_media_id));
}

#[tokio::test]
async fn staged_negotiated_publish_commit_moves_through_room_owned_transaction() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(scenario.staged_count().await, 1);

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert!(scenario.drain_publisher().is_empty());
    assert_bootstrap_for_stream(&scenario.drain_subscriber(), TestSourceKind::ScalableVideo);
    assert!(
        scenario.removed_media_count() == 0,
        "successful commit should not compensate the staged producer media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_materializes_all_negotiated_encodings() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario.fake.set_negotiated_producer_parameters(
        transport_media_id,
        test_simulcast_video_rtp_parameters(),
    );

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert_eq!(
        scenario
            .room
            .test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await
            .expect("transport media should resolve to source encodings")
            .len(),
        2
    );
    assert!(scenario.drain_publisher().is_empty());
    let subscriber_messages = scenario.drain_subscriber();
    assert_bootstrap_for_stream(&subscriber_messages, TestSourceKind::ScalableVideo);
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
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    scenario
        .fake
        .clear_negotiated_producer_parameters(transport_media_id);

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert!(
        !scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.has_removed_media(transport_media_id),
        "commit should clean up the staged transport media when negotiated parameters are unavailable"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_cleans_up_when_user_state_rejects_it() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert!(
        scenario
            .room
            .remove_user_with_cleanup(
                &scenario.user_id,
                scenario.connection_id,
                UserCleanup::state_only(Some(&scenario.adapter)),
            )
            .await
    );
    let _ = scenario.drain_publisher();
    let _ = scenario.drain_subscriber();

    scenario.commit().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert!(
        !scenario
            .room
            .test_api()
            .inspect()
            .has_session(&scenario.user_id)
            .await
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.removed_media_count() > 0,
        "commit rejection should clean up the staged transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_commit_rejects_replaced_connection() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    let transport_media_id = scenario
        .staged_transport_media_id(TestSourceKind::ScalableVideo)
        .await;
    let (replacement_sender, _replacement_rx) = test_sender();
    let replacement_connection_id = scenario
        .room
        .test_api()
        .lifecycle()
        .join_session_without_transport_cleanup(
            scenario.user_id.clone(),
            None,
            UserPermissions::default(),
            replacement_sender,
            &scenario.adapter,
        )
        .await
        .expect("replacement user should join");
    let _ = scenario.drain_publisher();
    let _ = scenario.drain_subscriber();

    scenario.commit().await;

    assert_ne!(replacement_connection_id, scenario.connection_id);
    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    assert!(
        !scenario
            .room
            .is_stream_published(
                &scenario.user_id,
                &stream_id_for_source(TestSourceKind::ScalableVideo)
            )
            .await
    );
    assert_eq!(
        scenario
            .room
            .test_api()
            .inspect()
            .source_encoding_ids_for_transport_media_id(transport_media_id)
            .await,
        None
    );
    scenario.assert_no_outbound();
    assert!(
        scenario.has_removed_media(transport_media_id),
        "stale replaced publish commit should clean up the staged transport media"
    );
}

#[tokio::test]
async fn staged_publish_connection_cleanup_rolls_back_every_staged_stream() {
    let mut scenario = StagedPublishScenario::new().await;

    assert_eq!(
        scenario.stage_scalable_video().await,
        PublishStageOutcome::Staged
    );
    assert_eq!(
        scenario.stage_source(TestSourceKind::ReadableVideo).await,
        PublishStageOutcome::Staged
    );
    assert_eq!(scenario.staged_count().await, 2);

    scenario.rollback_connection().await;

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 0);
    scenario.assert_no_outbound();
    assert_eq!(
        scenario.removed_media_count(),
        2,
        "connection cleanup should remove every staged publish transport media"
    );
}

#[tokio::test]
async fn staged_negotiated_publish_duplicate_race_keeps_one_staged_entry_and_one_cleanup() {
    let scenario = StagedPublishScenario::new().await;
    scenario
        .fake
        .set_publish_media_delay(Some(Duration::from_millis(200)));
    let (first_stage, second_stage) = tokio::join!(
        scenario.stage_scalable_video(),
        scenario.stage_scalable_video(),
    );

    let outcomes = [first_stage, second_stage];
    assert!(outcomes.contains(&PublishStageOutcome::Staged));
    assert!(
        outcomes.contains(&PublishStageOutcome::DuplicateAfterReservation {
            cleanup: crate::TransportEffectOutcome::Applied,
        })
    );
    assert_eq!(scenario.staged_count().await, 1);

    assert_eq!(
        scenario.publish_media_requested_count(),
        2,
        "both racing stage attempts should declare transport media before the post-await duplicate re-check"
    );
    assert_eq!(
        scenario.removed_media_count(),
        1,
        "the duplicate staged transport media should be compensated exactly once"
    );
    assert!(matches!(
        scenario.rollback_scalable_video().await,
        RollbackStagedPublishOutcome::RolledBack { .. }
    ));
}

#[tokio::test]
async fn cancelled_staged_publish_does_not_create_pending_owner() {
    let scenario = StagedPublishScenario::new().await;
    scenario
        .fake
        .set_publish_media_delay(Some(Duration::from_millis(200)));

    let stage_task = tokio::spawn({
        let room = Arc::clone(&scenario.room);
        let adapter = scenario.adapter.clone();
        let user_id = scenario.user_id.clone();
        let connection_id = scenario.connection_id;
        async move {
            room.stage_negotiated_publish(
                &user_id,
                connection_id,
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
        }
    });

    wait_for_fake_event(&scenario.fake, |event| {
        matches!(
            event,
            FakeMediaTransportEvent::PublishMediaRequested {
                user_id: UserId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;
    stage_task.abort();
    let join_error = stage_task
        .await
        .expect_err("aborted staged publish task should report cancellation");
    assert!(join_error.is_cancelled());

    assert_eq!(scenario.staged_count().await, 0);
    assert_eq!(
        scenario.publish_media_requested_count(),
        1,
        "the publish intent reached the transport boundary before cancellation"
    );
    assert_eq!(
        scenario.removed_media_count(),
        0,
        "cancellation before transport returns must not invent cleanup work"
    );
}

#[allow(
    clippy::panic,
    reason = "the RTC room test fixture uses a fixed valid configuration and should fail loudly if it stops being valid"
)]
fn build_real_rtc_media_transport() -> MediaTransport {
    match RtcTransport::builder()
        .transport_config(RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: crate::VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(46_200, 46_299),
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: crate::CodecPreferences::default(),
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
        .worker_count(1)
        .build()
    {
        Ok(transport) => MediaTransport::from_rtc_transport(transport),
        Err(error) => panic!("constant RTC room test transport config should be valid: {error}"),
    }
}

async fn bootstrap_real_rtc_user(
    media_transport: &MediaTransport,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    media_transport
        .create_initial_session_offer(session_key)
        .await
        .expect("rtc user should produce an initial offer")
}

fn assert_bootstrap_for_stream(messages: &[UserOutbound], stream_type: TestSourceKind) {
    assert!(
        messages.iter().any(|message| matches!(
            message,
            UserOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    RoomEventRequest::BootstrapRemoteTrack(payload)
                        if payload.stream_id() == &stream_id_for_source(stream_type)
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
    adapter: &MediaTransport,
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
