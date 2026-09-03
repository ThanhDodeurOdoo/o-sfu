use std::time::Duration;

use o_sfu_telemetry::schema::event as telemetry_event;
use serde_json::json;
use tokio::time::timeout;

use super::{super::tracing as test_tracing, support::*};
use crate::engine::{
    UserInfo,
    media_transport::{
        ActiveSpeakerSource, ReceiverBandwidthSnapshot, SourcePolicyUpdateSubscription,
        TransportBitrateSnapshot, TransportMediaId, TransportTeardown,
    },
    metrics::{MetricName, test_support::RuntimeMetricsSnapshotLookup},
    room::{
        DeactivateIntentOutcome, media_graph::ReceiverRouteActivity,
        source_policy::SourcePolicyTransaction, state::RoomState,
    },
    source_model::{
        ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId, SourceDeactivateIntent,
        SourcePolicy, SourcePublishIntent,
    },
};

fn plan_policy(
    state: &RoomState,
    speakers: &[ActiveSpeakerSource],
    bandwidth: &ReceiverBandwidthSnapshot,
) -> Option<SourcePolicyTransaction> {
    SourcePolicyTransaction::plan(
        state,
        speakers,
        bandwidth,
        &TransportBitrateSnapshot::default(),
    )
}

async fn apply_policy_observations(
    scenario: &SourcePolicyScenario,
    bandwidth: &ReceiverBandwidthSnapshot,
    observations: u8,
) {
    for _ in 0..observations {
        let tx = {
            let state = scenario.room.state.read().await;
            plan_policy(&state, &[], bandwidth)
                .expect("bandwidth observation should produce a policy update")
        };
        tx.execute(&scenario.room, &scenario.adapter).await;
    }
}

async fn assert_scalable_video_rid_for_publishers(
    scenario: &SourcePolicyScenario,
    receiver: &UserId,
    publishers: impl IntoIterator<Item = i64>,
    expected_rid: &str,
) {
    for publisher in publishers {
        assert_subscription_selected_rid(
            &scenario.room,
            &scenario.adapter,
            receiver,
            &UserId::Integer(publisher),
            TestSourceKind::ScalableVideo,
            expected_rid,
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn two_party_camera_publish_selects_the_highest_consumer_layer() {
    let (room, adapter, metrics, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_media_metrics().await;

    let transitions_before = route_transition_counts(&room);
    let capture = test_tracing::capture().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;
    assert_no_route_change_event(&room, &UserId::Integer(2));
    drop(capture);
    assert_eq!(route_transition_counts(&room), transitions_before);

    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_remote_track_snapshot_for_stream(
        &drain_outbound(&mut subscriber_rx),
        TestSourceKind::ScalableVideo,
    );
    assert_subscription_selected_rid(
        &room,
        &adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "hi",
    )
    .await;
    reset_subscription_selection_to_open(
        &room,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let keyframes_before = keyframe_request_count(&metrics);
    refresh_source_policy(&room, &adapter).await;
    assert!(keyframe_request_count(&metrics) > keyframes_before);
    assert_subscription_selected_rid(
        &room,
        &adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "hi",
    )
    .await;
    assert_receiver_bwe_target(
        &room,
        &adapter,
        &UserId::Integer(2),
        Bitrate::from_kbps(900),
    )
    .await;
}

#[tokio::test]
async fn source_bitrate_cap_limits_or_pauses_consumer_layers() {
    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    publish_capped_camera(&room, &adapter, Bitrate::from_kbps(200)).await;
    assert_subscription_selected_rid(
        &room,
        &adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "lo",
    )
    .await;
    assert_receiver_bwe_target(
        &room,
        &adapter,
        &UserId::Integer(2),
        Bitrate::from_kbps(150),
    )
    .await;
    let (_, sources) = diagnostics_room_views(&room, &adapter).await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    assert!(sources.iter().any(|source| {
        source.owner_user_id == UserId::Integer(1)
            && source.stream_id == stream_id.as_str()
            && source.video_bitrate_cap_bps == Some(200_000)
    }));

    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    publish_capped_camera(&room, &adapter, Bitrate::from_kbps(100)).await;
    assert_subscription_policy_pause_reason(
        &room,
        &adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::SourceBitrateLimit),
    )
    .await;
    assert_receiver_bwe_target(&room, &adapter, &UserId::Integer(2), Bitrate::zero()).await;
}

#[tokio::test]
async fn video_bitrate_cap_admits_video_sources_without_adaptation() {
    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    let intent = SourcePublishIntent::new(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        MediaKind::Video,
        SourcePolicy::hidden().with_video_bitrate_cap(Bitrate::from_kbps(200)),
    );
    room.test_api()
        .media()
        .publish_intent(
            &UserId::Integer(1),
            &intent,
            MediaKind::Video,
            test_simulcast_video_rtp_parameters(),
            &adapter,
        )
        .await
        .expect("capped camera publication should succeed");
    assert_subscription_selected_rid(
        &room,
        &adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "lo",
    )
    .await;
}

#[tokio::test]
async fn observed_ridless_source_above_cap_is_paused() {
    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    let intent = SourcePublishIntent::new(
        stream_id_for_source(TestSourceKind::ReadableVideo),
        MediaKind::Video,
        source_publish_intent_for_source(TestSourceKind::ReadableVideo)
            .policy()
            .with_video_bitrate_cap(Bitrate::from_kbps(200)),
    );
    room.test_api()
        .media()
        .publish_intent(
            &publisher,
            &intent,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await
        .expect("capped readable video publication should succeed");
    let source_media = source_media_id(&room, &publisher, TestSourceKind::ReadableVideo).await;
    let source_bitrate = TransportBitrateSnapshot {
        total: Bitrate::from_kbps(500),
        per_media: vec![(source_media, Bitrate::from_kbps(500))],
    };
    let tx = {
        let state = room.state.read().await;
        SourcePolicyTransaction::plan(
            &state,
            &[],
            &ReceiverBandwidthSnapshot::default(),
            &source_bitrate,
        )
        .expect("observed cap violation should produce a policy update")
    };
    tx.execute(&room, &adapter).await;

    assert_subscription_policy_pause_reason(
        &room,
        &adapter,
        &receiver,
        &publisher,
        TestSourceKind::ReadableVideo,
        Some(DiagnosticsPolicyPauseReason::SourceBitrateLimit),
    )
    .await;
}

#[tokio::test]
async fn source_bitrate_cap_pause_survives_receiver_overload() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2, 3]).await;
    publish_capped_camera(&scenario.room, &scenario.adapter, Bitrate::from_kbps(100)).await;
    publish_simulcast_camera(&scenario.room, &UserId::Integer(3), &scenario.adapter).await;

    let tx = {
        let receiver_user_id = UserId::Integer(2);
        let active_speaker_sources = scenario.adapter.active_speaker_source_snapshot().await;
        let receiver_connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;
        let receiver_session_key = scenario
            .room
            .transport_user_key(&receiver_user_id, receiver_connection_id)
            .await;
        let receiver_bandwidth_snapshot = ReceiverBandwidthSnapshot {
            per_session: vec![(receiver_session_key, Bitrate::from_kbps(100))],
        };
        let state = scenario.room.state.read().await;
        plan_policy(
            &state,
            &active_speaker_sources,
            &receiver_bandwidth_snapshot,
        )
        .expect("source policy transaction should contain overload work")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::SourceBitrateLimit),
    )
    .await;
}

#[tokio::test]
async fn multiparty_camera_publish_marks_thumbnail_routes_in_diagnostics() {
    let (room, adapter) = setup_three_ready_users_with_transport().await;

    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;

    for consumer_user_id in [UserId::Integer(2), UserId::Integer(3)] {
        assert_subscription_layout(
            &room,
            &adapter,
            &consumer_user_id,
            TestSourceKind::ScalableVideo,
            DiagnosticsVideoLayoutRole::VisibleThumbnail,
            DiagnosticsVideoRoutePriority::VisibleThumbnail,
        )
        .await;
    }
}

async fn publish_capped_camera(room: &Arc<Room>, adapter: &MediaTransport, cap: Bitrate) {
    let intent = SourcePublishIntent::new(
        stream_id_for_source(TestSourceKind::ScalableVideo),
        MediaKind::Video,
        source_publish_intent_for_source(TestSourceKind::ScalableVideo)
            .policy()
            .with_video_bitrate_cap(cap),
    );
    room.test_api()
        .media()
        .publish_intent(
            &UserId::Integer(1),
            &intent,
            MediaKind::Video,
            test_simulcast_video_rtp_parameters(),
            adapter,
        )
        .await
        .expect("capped camera publication should succeed");
}

#[tokio::test]
async fn source_policy_resets_receiver_bwe_target_after_publication_deactivation() {
    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;
    assert_receiver_bwe_target(
        &room,
        &adapter,
        &UserId::Integer(2),
        Bitrate::from_kbps(900),
    )
    .await;

    assert!(
        room.test_api()
            .media()
            .deactivate_publication(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
    );

    assert_eq!(room.test_api().inspect().producer_count().await, 1);
    assert_eq!(room.test_api().inspect().consumer_count().await, 1);
    assert_receiver_bwe_target(&room, &adapter, &UserId::Integer(2), Bitrate::zero()).await;
}

#[tokio::test]
async fn source_policy_stale_featured_update_does_not_mark_replacement_user() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let audio_media_id = scenario.audio_media_id(1).await;
    scenario.mark_active_speaker(audio_media_id).await;
    let tx = source_policy_transaction_from_transport_snapshot(&scenario).await;

    let (replacement_tx, _replacement_rx) = test_sender();
    join_user_without_transport_teardown(
        &scenario.room,
        &scenario.adapter,
        UserId::Integer(1),
        replacement_tx,
    )
    .await;
    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_featured(&scenario, 1, false).await;
}

#[tokio::test]
async fn source_policy_ignores_receiver_bandwidth_from_replaced_connection() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    let receiver_user_id = UserId::Integer(2);
    publish_simulcast_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;
    let old_connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;
    let old_session_key = scenario
        .room
        .transport_user_key(&receiver_user_id, old_connection_id)
        .await;

    let (replacement_tx, _replacement_rx) = test_sender();
    join_user_without_transport_teardown(
        &scenario.room,
        &scenario.adapter,
        receiver_user_id.clone(),
        replacement_tx,
    )
    .await;
    make_session_ready_with_transport(&scenario.room, &receiver_user_id, &scenario.adapter).await;
    let receiver_bandwidth_snapshot = ReceiverBandwidthSnapshot {
        per_session: vec![(old_session_key, Bitrate::from_kbps(100))],
    };
    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &[], &receiver_bandwidth_snapshot)
            .expect("source policy transaction should contain current bandwidth work")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    let (diagnostics, _) = diagnostics_room_views(&scenario.room, &scenario.adapter).await;
    let subscription = diagnostics
        .iter()
        .find(|view| view.user_id == receiver_user_id)
        .and_then(|view| {
            view.subscriptions.iter().find(|subscription| {
                subscription.producer_user_id == UserId::Integer(1)
                    && subscription.stream_id
                        == stream_id_for_source(TestSourceKind::ScalableVideo).as_str()
            })
        })
        .expect("replacement receiver should have a current video route");
    assert_eq!(
        subscription
            .selection
            .latest_receiver_bandwidth_estimate_bps,
        None
    );
}

#[tokio::test]
async fn active_speaker_camera_policy_selects_the_observed_speaker() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera_for_users(&[1, 2]).await;
    let second_audio_media_id = scenario.audio_media_id(2).await;

    scenario.mark_active_speaker(second_audio_media_id).await;
    scenario.refresh_policy_until_upgrades_settle().await;

    assert_subscription_selected_rid(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(1),
        &UserId::Integer(2),
        TestSourceKind::ScalableVideo,
        "hi",
    )
    .await;
}

#[tokio::test]
async fn active_speaker_camera_policy_tracks_camera_activity() {
    let (room, adapter, _owner_rx, mut observer_rx) = setup_two_ready_users().await;
    let scenario = SourcePolicyScenario { room, adapter };
    let owner_id = UserId::Integer(1);
    publish_track(
        &scenario.room,
        &owner_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        &scenario.adapter,
    )
    .await;
    let audio_media_id = scenario.audio_media_id(1).await;

    scenario.mark_active_speaker(audio_media_id).await;
    scenario.refresh_policy().await;
    assert_featured(&scenario, 1, false).await;
    drain_outbound(&mut observer_rx);

    let camera = source_publish_intent_for_source(TestSourceKind::ScalableVideo).with_presence(
        Some(UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::default()
        }),
    );
    scenario
        .room
        .test_api()
        .media()
        .publish_intent(
            &owner_id,
            &camera,
            MediaKind::Video,
            test_simulcast_video_rtp_parameters(),
            &scenario.adapter,
        )
        .await
        .expect("camera publication should succeed");
    assert_camera_feature_fanout(&mut observer_rx, &owner_id, true);

    let connection_id = user_connection_id(&scenario.room, &owner_id).await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    let pause = SourceDeactivateIntent::new(stream_id).with_presence(Some(UserInfo {
        is_camera_on: Some(false),
        ..UserInfo::default()
    }));
    assert_eq!(
        scenario
            .room
            .user_operation(&owner_id, connection_id, &scenario.adapter)
            .deactivate_publication(&pause)
            .await,
        DeactivateIntentOutcome::Deactivated
    );

    assert_eq!(scenario.room.test_api().inspect().producer_count().await, 2);
    assert_camera_feature_fanout(&mut observer_rx, &owner_id, false);
    assert!(matches!(
        scenario
            .room
            .user_operation(&owner_id, connection_id, &scenario.adapter)
            .start_publish(&camera, true)
            .await,
        Ok(PublishIntentOutcome::Activated)
    ));

    assert_camera_feature_fanout(&mut observer_rx, &owner_id, true);
}

#[tokio::test]
async fn active_speaker_camera_policy_prefers_louder_same_observation_speaker() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let first_audio_media_id = scenario.audio_media_id(1).await;
    let third_audio_media_id = scenario.audio_media_id(3).await;

    scenario
        .mark_active_speakers_with_levels([
            (first_audio_media_id, -30),
            (third_audio_media_id, -10),
        ])
        .await;
    scenario.refresh_policy_until_upgrades_settle().await;

    assert_featured(&scenario, 1, false).await;
    assert_featured(&scenario, 3, true).await;

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
}

#[tokio::test]
async fn audio_speaker_limit_ignores_foreign_and_inactive_sources() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(1, 10).unwrap(),
    )
    .await;
    for user_id in [UserId::Integer(1), UserId::Integer(3)] {
        publish_track(
            &scenario.room,
            &user_id,
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &scenario.adapter,
        )
        .await;
    }
    let first_audio_media_id = scenario.audio_media_id(1).await;
    let third_audio_media_id = scenario.audio_media_id(3).await;
    let observed_at = Instant::now();
    let speakers = [
        ActiveSpeakerSource::new(
            TransportMediaId::new(u64::MAX),
            observed_at + Duration::from_millis(2),
        ),
        ActiveSpeakerSource::new(first_audio_media_id, observed_at + Duration::from_millis(1)),
        ActiveSpeakerSource::new(third_audio_media_id, observed_at),
    ];
    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &speakers, &ReceiverBandwidthSnapshot::default())
            .expect("audio speaker limit should update the overflow route")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;
    scenario
        .mark_active_speakers_with_levels([
            (first_audio_media_id, -20),
            (third_audio_media_id, -30),
        ])
        .await;
    scenario.refresh_policy().await;

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::AudioDetector,
        Some(DiagnosticsPolicyPauseReason::AudioSpeakerLimit),
    )
    .await;

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .deactivate_publication(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::AudioDetector),
                &scenario.adapter,
            )
            .await
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
}

#[tokio::test]
async fn audio_speaker_limit_prioritizes_screen_sharers() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        // Restrict to 1 active speaker so that all sources are dropped except the highest-priority one.
        RoomMediaLimits::try_new(1, 10).unwrap(),
    )
    .await;
    let screen_sharer_id = UserId::Integer(1);
    let receiver_id = UserId::Integer(2);
    let non_screen_sharer_id = UserId::Integer(3);
    for user_id in [&screen_sharer_id, &non_screen_sharer_id] {
        publish_track(
            &scenario.room,
            user_id,
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &scenario.adapter,
        )
        .await;
    }
    let screen_sharing_intent = source_publish_intent_for_source(TestSourceKind::ReadableVideo)
        .with_presence(Some(UserInfo {
            is_screen_sharing_on: Some(true),
            ..UserInfo::default()
        }));
    scenario
        .room
        .test_api()
        .media()
        .publish_intent(
            &screen_sharer_id,
            &screen_sharing_intent,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &scenario.adapter,
        )
        .await
        .expect("screen share publication should succeed");
    let screen_sharer_audio = source_media_id(
        &scenario.room,
        &screen_sharer_id,
        TestSourceKind::AudioDetector,
    )
    .await;
    let non_screen_sharer_audio = source_media_id(
        &scenario.room,
        &non_screen_sharer_id,
        TestSourceKind::AudioDetector,
    )
    .await;
    // Make the non-screen sharer louder so they rank higher.
    // This verifies that the screen sharer's audio is preserved despite having a lower volume ranking.
    scenario
        .mark_active_speakers_with_levels([
            (non_screen_sharer_audio, -10), // loudest
            (screen_sharer_audio, -30),     // quietest
        ])
        .await;
    scenario.refresh_policy().await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver_id,
        &screen_sharer_id,
        TestSourceKind::AudioDetector,
        // No pause reason means the subscription remains active and the screen sharer's source is preserved.
        None,
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver_id,
        &non_screen_sharer_id,
        TestSourceKind::AudioDetector,
        Some(DiagnosticsPolicyPauseReason::AudioSpeakerLimit),
    )
    .await;
}

#[tokio::test]
async fn deafening_a_receiver_pauses_its_audio_and_keeps_video() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario
        .publish_audio_and_camera_for_users(&[1, 2, 3])
        .await;
    let first_audio = scenario.audio_media_id(1).await;
    scenario.refresh_policy().await;
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(2), UserId::Integer(3)]
    );

    scenario.set_deaf(2, true).await;

    // User 1's audio now reaches user 3 only, so the deafened receiver is the
    // single route that stopped being forwarded.
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(3)]
    );
    for publisher_user_id in [UserId::Integer(1), UserId::Integer(3)] {
        assert_subscription_policy_pause_reason(
            &scenario.room,
            &scenario.adapter,
            &UserId::Integer(2),
            &publisher_user_id,
            TestSourceKind::AudioDetector,
            Some(DiagnosticsPolicyPauseReason::ReceiverDeafened),
        )
        .await;
    }
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(3),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
    // A receiver-side audio decision must not touch video delivery.
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
}

#[tokio::test]
async fn undeafening_restores_audio_on_the_negotiated_route() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio = scenario.audio_media_id(1).await;
    scenario.refresh_policy().await;
    let destination_before =
        consumer_destination_identity(&scenario.adapter, first_audio, &UserId::Integer(2)).await;

    scenario.set_deaf(2, true).await;
    scenario.set_deaf(2, false).await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(2)]
    );
    assert_eq!(
        consumer_destination_identity(&scenario.adapter, first_audio, &UserId::Integer(2)).await,
        destination_before
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
}

#[tokio::test]
async fn undeafening_recomputes_the_audio_speaker_limit() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(1, 10).unwrap(),
    )
    .await;
    for user_id in [UserId::Integer(1), UserId::Integer(3)] {
        publish_track(
            &scenario.room,
            &user_id,
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &scenario.adapter,
        )
        .await;
    }
    let first_audio = scenario.audio_media_id(1).await;
    let third_audio = scenario.audio_media_id(3).await;
    // User 1 is the louder, more recent speaker, so it wins the single slot and
    // user 3 is the route the speaker cap withholds.
    scenario
        .mark_active_speakers_with_levels([(third_audio, -30), (first_audio, -20)])
        .await;
    scenario.refresh_policy().await;
    scenario.set_deaf(2, true).await;
    for publisher_user_id in [UserId::Integer(1), UserId::Integer(3)] {
        assert_subscription_policy_pause_reason(
            &scenario.room,
            &scenario.adapter,
            &UserId::Integer(2),
            &publisher_user_id,
            TestSourceKind::AudioDetector,
            Some(DiagnosticsPolicyPauseReason::ReceiverDeafened),
        )
        .await;
    }

    scenario.set_deaf(2, false).await;

    // Undeafening restores only the admitted speaker; the capped route keeps its
    // own pause reason instead of being blindly resumed. User 3 receives the
    // admitted speaker throughout because it never deafened.
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(2), UserId::Integer(3)]
    );
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [third_audio]).await,
        Vec::<UserId>::new()
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::AudioDetector,
        Some(DiagnosticsPolicyPauseReason::AudioSpeakerLimit),
    )
    .await;
}

#[tokio::test]
async fn audio_published_while_the_receiver_is_deaf_starts_paused() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.set_deaf(2, true).await;

    scenario.publish_audio_and_camera(1).await;

    let first_audio = scenario.audio_media_id(1).await;
    // The route is set up but never forwarded, so no audio leaks between the
    // publish and the next policy turn.
    let destination_at_publish =
        consumer_destination_identity(&scenario.adapter, first_audio, &UserId::Integer(2)).await;
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        Vec::<UserId>::new()
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        Some(DiagnosticsPolicyPauseReason::ReceiverDeafened),
    )
    .await;

    scenario.set_deaf(2, false).await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(2)]
    );
    assert_eq!(
        consumer_destination_identity(&scenario.adapter, first_audio, &UserId::Integer(2)).await,
        destination_at_publish
    );
}

#[tokio::test]
async fn resubscribing_while_deaf_keeps_transport_and_diagnostics_agreed() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio = scenario.audio_media_id(1).await;
    scenario.set_deaf(2, true).await;

    // Receiver intent only moves the subscription flag. It must not reopen the
    // transport destination behind a policy pause, or delivery and diagnostics
    // would disagree until some later turn happened to change the pause reason.
    scenario.subscribe_audio(2, 1, true).await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        Vec::<UserId>::new()
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        Some(DiagnosticsPolicyPauseReason::ReceiverDeafened),
    )
    .await;

    scenario.set_deaf(2, false).await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![UserId::Integer(2)]
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;
}

#[tokio::test]
async fn a_deaf_receiver_keeps_its_video_subscription_deliverable() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let first_camera = source_media_id(
        &scenario.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    scenario.set_deaf(2, true).await;

    // Deafening is audio only. Stamping ReceiverDeafened onto a video route would
    // freeze it permanently, because audio policy iterates audio routes and so
    // could never clear it again.
    scenario.subscribe_scalable_video(2, 1, true).await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_camera]).await,
        vec![UserId::Integer(2)]
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
}

#[tokio::test]
async fn deafening_releases_the_receivers_audio_budget_reserve() {
    let tuning = VideoAdaptationTuning::try_new(3, 2, 2, 3, 0, Bitrate::from_kbps(40))
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2, 3], tuning).await;
    for user_id in [UserId::Integer(1), UserId::Integer(3)] {
        publish_track(
            &scenario.room,
            &user_id,
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &scenario.adapter,
        )
        .await;
    }
    let first_audio = scenario.audio_media_id(1).await;
    let third_audio = scenario.audio_media_id(3).await;
    scenario
        .mark_active_speakers([first_audio, third_audio])
        .await;
    scenario.refresh_policy().await;

    // Receiver 2 has no video, so its whole BWE demand is the audio reserve for
    // the two admitted speakers it consumes.
    let receiver_user_id = UserId::Integer(2);
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        Bitrate::from_kbps(80),
    )
    .await;

    scenario.set_deaf(2, true).await;

    // A deafened receiver consumes no audio, so it must stop reserving video
    // budget for audio it will never get.
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        Bitrate::zero(),
    )
    .await;

    scenario.set_deaf(2, false).await;

    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        Bitrate::from_kbps(80),
    )
    .await;
}

#[tokio::test]
async fn reactivating_an_inactive_subscription_while_deaf_plans_no_active_destination() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let receiver_user_id = UserId::Integer(2);
    let publisher_user_id = UserId::Integer(1);
    // The route is inactive when the deafen turn runs, so the policy snapshot
    // skips it and it never gets stamped with a pause reason.
    scenario.subscribe_audio(2, 1, false).await;
    scenario.set_deaf(2, true).await;

    // Transport applies before the policy turn that would correct it, so the
    // planned activity itself must already be inactive; otherwise the
    // destination opens and queued RTP reaches a deafened receiver.
    let connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;
    let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
        audio_detector: Some(true),
        ..TestSubscriptionStates::default()
    });
    let work = {
        let mut state = scenario.room.state.write().await;
        state.plan_receiver_route_work(
            &receiver_user_id,
            connection_id,
            &publisher_user_id,
            &intents,
        )
    };

    assert_eq!(
        work.activities
            .iter()
            .map(ReceiverRouteActivity::active)
            .collect::<Vec<_>>(),
        vec![false]
    );
}

#[tokio::test]
async fn each_deafen_toggle_moves_audio_delivery() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio = scenario.audio_media_id(1).await;
    let receiver_user_id = UserId::Integer(2);

    for (toggle, is_deaf) in [true, false, true, false, true].into_iter().enumerate() {
        scenario.set_deaf(2, is_deaf).await;

        let expected_receivers = if is_deaf {
            Vec::new()
        } else {
            vec![receiver_user_id.clone()]
        };
        assert_eq!(
            active_destination_receivers(&scenario.adapter, [first_audio]).await,
            expected_receivers,
            "toggle {toggle} to is_deaf={is_deaf} should move audio delivery"
        );
        assert_subscription_policy_pause_reason(
            &scenario.room,
            &scenario.adapter,
            &receiver_user_id,
            &UserId::Integer(1),
            TestSourceKind::AudioDetector,
            is_deaf.then_some(DiagnosticsPolicyPauseReason::ReceiverDeafened),
        )
        .await;
    }
}

#[tokio::test]
async fn deafen_from_a_stale_connection_keeps_audio_flowing() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    scenario.publish_audio_and_camera(1).await;
    let first_audio = scenario.audio_media_id(1).await;
    let receiver_user_id = UserId::Integer(2);
    let current_connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;

    scenario
        .set_deaf_for_connection(&receiver_user_id, test_connection_id(u64::MAX), true)
        .await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        vec![receiver_user_id.clone()]
    );
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        &UserId::Integer(1),
        TestSourceKind::AudioDetector,
        None,
    )
    .await;

    scenario
        .set_deaf_for_connection(&receiver_user_id, current_connection_id, true)
        .await;

    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_audio]).await,
        Vec::<UserId>::new()
    );
}

#[tokio::test]
async fn per_receiver_audio_reserve_excludes_own_and_counts_only_consumed_audio() {
    // Reserve 40 kbps of video budget per admitted audio speaker the receiver
    // actually consumes; no headroom so the arithmetic is exact.
    let tuning = VideoAdaptationTuning::try_new(3, 2, 2, 3, 0, Bitrate::from_kbps(40))
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2, 3], tuning).await;
    scenario
        .publish_audio_and_camera_for_users(&[1, 2, 3])
        .await;

    let first_audio = scenario.audio_media_id(1).await;
    let second_audio = scenario.audio_media_id(2).await;
    let third_audio = scenario.audio_media_id(3).await;
    let observed_at = Instant::now();
    let speakers = [
        ActiveSpeakerSource::new(first_audio, observed_at + Duration::from_millis(2)),
        ActiveSpeakerSource::new(second_audio, observed_at + Duration::from_millis(1)),
        ActiveSpeakerSource::new(third_audio, observed_at),
    ];

    // Receiver user 2 has 900 kbps and consumes admitted audio from users 1 and 3
    // (not its own), so the video budget loses 2 * 40 = 80 kbps -> 820 kbps.
    let receiver_user_id = UserId::Integer(2);
    let receiver_connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;
    let receiver_session_key = scenario
        .room
        .transport_user_key(&receiver_user_id, receiver_connection_id)
        .await;
    let receiver_bandwidth_snapshot = ReceiverBandwidthSnapshot {
        per_session: vec![(receiver_session_key, Bitrate::from_kbps(900))],
    };

    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &speakers, &receiver_bandwidth_snapshot)
            .expect("policy pass should produce budget updates")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_subscription_selected_video_budget(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(820),
    )
    .await;

    // Pressure hysteresis still routes both 900 kbps selectors this turn. The
    // desired bitrate must cover that 1.8 Mbps plus the 80 kbps audio reserve.
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        Bitrate::from_kbps(1_880),
    )
    .await;
}

#[tokio::test]
async fn audio_only_receiver_reports_its_audio_reserve_as_bwe_demand() {
    let tuning = VideoAdaptationTuning::try_new(3, 2, 2, 3, 0, Bitrate::from_kbps(40))
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2, 3], tuning).await;
    // Only audio is published, so receiver user 2 has audio routes but no video
    // routes — the case where the last video route was dropped while audio keeps
    // flowing.
    for user_id in [UserId::Integer(1), UserId::Integer(3)] {
        publish_track(
            &scenario.room,
            &user_id,
            TestSourceKind::AudioDetector,
            MediaKind::Audio,
            test_audio_rtp_parameters(),
            &scenario.adapter,
        )
        .await;
    }
    let first_audio = scenario.audio_media_id(1).await;
    let third_audio = scenario.audio_media_id(3).await;
    let observed_at = Instant::now();
    let speakers = [
        ActiveSpeakerSource::new(first_audio, observed_at + Duration::from_millis(1)),
        ActiveSpeakerSource::new(third_audio, observed_at),
    ];

    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &speakers, &ReceiverBandwidthSnapshot::default())
            .expect("audio-only receiver should still report BWE demand")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    // User 2 consumes admitted audio from users 1 and 3 and has no video, so its
    // desired bitrate is the audio reserve alone (2 * 40 kbps) rather than zero.
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        Bitrate::from_kbps(80),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn overload_steps_thumbnail_down_one_layer_and_keeps_it_deliverable() {
    // A high multiparty threshold forces each route to start at its top layer, so
    // the aggregate overload loop — not per-route selection — does the stepping.
    let tuning = VideoAdaptationTuning::try_new(99, 2, 2, 3, 0, Bitrate::zero())
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2], tuning).await;
    // Three layers (lo=150, mid=450, hi=900 kbps) so one down-step lands on the
    // middle layer rather than the cheapest.
    publish_three_layer_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;

    // Receiver user 2 has 500 kbps: the top layer (900) is over budget but one
    // step down to the middle layer (450) fits, so the loop stops there.
    let receiver_user_id = UserId::Integer(2);
    let receiver_connection_id = user_connection_id(&scenario.room, &receiver_user_id).await;
    let receiver_session_key = scenario
        .room
        .transport_user_key(&receiver_user_id, receiver_connection_id)
        .await;
    let receiver_bandwidth_snapshot = ReceiverBandwidthSnapshot {
        per_session: vec![(receiver_session_key.clone(), Bitrate::from_kbps(500))],
    };

    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &[], &receiver_bandwidth_snapshot)
            .expect("overload should step the thumbnail down")
    };
    let transitions_before = route_transition_counts(&scenario.room);
    let selection_updates_before = source_selection_update_count(&scenario.room, "encoding");
    let source_media = source_media_id(
        &scenario.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let capture = test_tracing::capture().await;
    tx.execute(&scenario.room, &scenario.adapter).await;

    // The route survives at the middle layer and stays deliverable (no pause),
    // rather than being dropped to the cheapest layer or paused.
    assert_subscription_selected_rid(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "mid",
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver_user_id,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
    assert_eq!(
        route_transition_counts(&scenario.room),
        RouteTransitionCounts {
            degraded: transitions_before.degraded + 1,
            ..transitions_before
        }
    );
    assert_eq!(
        source_selection_update_count(&scenario.room, "encoding"),
        selection_updates_before + 1
    );
    assert_route_change_event(
        &scenario,
        &receiver_user_id,
        &UserId::Integer(1),
        &receiver_session_key,
        source_media,
        ExpectedRouteChange {
            outcome: "degraded",
            reason: None,
            receiver_bandwidth: Bitrate::from_kbps(500),
            video_budget: Bitrate::from_kbps(500),
            active_route_count: 1,
            selected_video_bitrate: Bitrate::from_kbps(450),
            selected_estimated_bitrate: Bitrate::from_kbps(450),
        },
    )
    .await;
    drop(capture);
}

#[tokio::test(flavor = "current_thread")]
async fn inactive_route_does_not_report_an_in_flight_degradation() {
    let tuning = VideoAdaptationTuning::try_new(99, 2, 2, 3, 0, Bitrate::zero())
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2], tuning).await;
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    publish_three_layer_camera(&scenario.room, &publisher, &scenario.adapter).await;
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let session_key = scenario
        .room
        .transport_user_key(&receiver, connection_id)
        .await;
    let bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key, Bitrate::from_kbps(500))],
    };
    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &[], &bandwidth).expect("overload should plan one route degradation")
    };
    update_subscription_selection(
        &scenario.room,
        &receiver,
        &publisher,
        TestSourceKind::ScalableVideo,
        |selection| selection.set_active(false),
    )
    .await;
    let transitions_before = route_transition_counts(&scenario.room);
    let capture = test_tracing::capture().await;

    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_no_route_change_event(&scenario.room, &receiver);
    drop(capture);
    assert_eq!(route_transition_counts(&scenario.room), transitions_before);
    assert_receiver_video_allocation(
        &scenario,
        &receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(500),
        1,
        0,
        Bitrate::zero(),
    )
    .await;
}

#[tokio::test]
async fn overload_steps_hidden_route_before_visible_thumbnail() {
    let tuning = VideoAdaptationTuning::try_new(99, 2, 2, 3, 0, Bitrate::zero())
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2, 3], tuning).await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(3), &scenario.adapter).await;
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Hidden)
        .await;
    let receiver = UserId::Integer(2);
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let session_key = scenario
        .room
        .transport_user_key(&receiver, connection_id)
        .await;
    let bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key, Bitrate::from_kbps(1_350))],
    };
    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &[], &bandwidth).expect("overload should step the hidden route down")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_subscription_selected_rid(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        "mid",
    )
    .await;
    assert_subscription_selected_rid(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
        "hi",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_route_control_reconciles_sibling_budget_to_committed_selection() {
    let tuning = VideoAdaptationTuning::try_new(99, 2, 2, 3, 0, Bitrate::zero())
        .expect("valid tuning should build");
    let scenario = SourcePolicyScenario::with_ready_users_and_tuning(&[1, 2, 3], tuning).await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(3), &scenario.adapter).await;
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Hidden)
        .await;
    let receiver = UserId::Integer(2);
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let session_key = scenario
        .room
        .transport_user_key(&receiver, connection_id)
        .await;
    let first_source_media = source_media_id(
        &scenario.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let third_source_media = source_media_id(
        &scenario.room,
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let first_consumer_media =
        consumer_destination_identity(&scenario.adapter, first_source_media, &receiver)
            .await
            .0;
    let bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key.clone(), Bitrate::from_kbps(900))],
    };
    let tx = {
        let state = scenario.room.state.read().await;
        plan_policy(&state, &[], &bandwidth).expect("overload should plan route degradation")
    };
    scenario
        .adapter
        .teardown([TransportTeardown::RemoveMedia {
            session_key: session_key.clone(),
            transport_media_id: first_consumer_media,
        }])
        .await;
    let transitions_before = route_transition_counts(&scenario.room);
    let capture = test_tracing::capture().await;

    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_scalable_video_rid_for_publishers(&scenario, &receiver, [1], "hi").await;
    assert_scalable_video_rid_for_publishers(&scenario, &receiver, [3], "mid").await;
    assert_route_change_event(
        &scenario,
        &receiver,
        &UserId::Integer(3),
        &session_key,
        third_source_media,
        ExpectedRouteChange {
            outcome: "degraded",
            reason: None,
            receiver_bandwidth: Bitrate::from_kbps(900),
            video_budget: Bitrate::from_kbps(900),
            active_route_count: 2,
            selected_video_bitrate: Bitrate::from_kbps(600),
            selected_estimated_bitrate: Bitrate::from_kbps(450),
        },
    )
    .await;
    drop(capture);
    assert_eq!(
        route_transition_counts(&scenario.room),
        RouteTransitionCounts {
            degraded: transitions_before.degraded + 1,
            ..transitions_before
        }
    );
    assert_receiver_video_allocation(
        &scenario,
        &receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(900),
        2,
        2,
        Bitrate::from_kbps(1_350),
    )
    .await;
}

#[tokio::test]
async fn video_download_limit_pauses_lowest_ranked_receiver_routes() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let third_audio_media_id = scenario.audio_media_id(3).await;
    let first_camera_media_id = source_media_id(
        &scenario.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let third_camera_media_id = source_media_id(
        &scenario.room,
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
    )
    .await;

    scenario.mark_active_speaker(third_audio_media_id).await;
    scenario.refresh_policy_until_upgrades_settle().await;

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
    )
    .await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
    // Receiver 2 is over its one-video cap, so it keeps user 3's camera and drops
    // user 1's, while receivers 1 and 3 stay within the cap and keep theirs.
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_camera_media_id]).await,
        vec![UserId::Integer(3)]
    );
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [third_camera_media_id]).await,
        vec![UserId::Integer(1), UserId::Integer(2)]
    );
}

#[tokio::test]
async fn video_download_limit_pauses_every_route_beyond_the_limit() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3, 4],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario
        .publish_audio_and_camera_for_users(&[1, 3, 4])
        .await;
    scenario
        .mark_active_speaker(scenario.audio_media_id(4).await)
        .await;
    scenario.refresh_policy_until_upgrades_settle().await;

    for owner in [1, 3] {
        assert_subscription_policy_pause_reason(
            &scenario.room,
            &scenario.adapter,
            &UserId::Integer(2),
            &UserId::Integer(owner),
            TestSourceKind::ScalableVideo,
            Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
        )
        .await;
    }
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(4),
        TestSourceKind::ScalableVideo,
        None,
    )
    .await;
    let receiver = UserId::Integer(2);
    let selected_video =
        receiver_selected_video_bitrate(&scenario.room, &scenario.adapter, &receiver).await;
    assert_receiver_bwe_target(&scenario.room, &scenario.adapter, &receiver, selected_video).await;
}

#[tokio::test(flavor = "current_thread")]
async fn replacing_a_pause_reason_is_not_a_new_pause_transition() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    scenario
        .set_scalable_video_layout(2, 3, VideoLayoutIntent::Pinned)
        .await;
    scenario.refresh_policy_until_upgrades_settle().await;
    let receiver = UserId::Integer(2);
    let publisher = UserId::Integer(1);
    update_subscription_selection(
        &scenario.room,
        &receiver,
        &publisher,
        TestSourceKind::ScalableVideo,
        |selection| {
            selection.set_policy_pause_reason(Some(PolicyPauseReason::BudgetPressure));
        },
    )
    .await;
    let transitions_before = route_transition_counts(&scenario.room);
    let capture = test_tracing::capture().await;

    scenario.refresh_policy().await;

    test_tracing::assert_no_event(
        telemetry_event::SOURCE_POLICY_ROUTE_CHANGED,
        &[
            ("room_id", json!(scenario.room.uuid())),
            ("user_id", json!("2")),
            ("producer_user_id", json!("1")),
        ],
    );
    drop(capture);
    assert_eq!(route_transition_counts(&scenario.room), transitions_before);
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        &publisher,
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn constrained_bandwidth_pauses_then_recovers_and_upgrades_a_pinned_route() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;
    scenario.subscribe_scalable_video(3, 1, false).await;
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Pinned)
        .await;
    let receiver = UserId::Integer(2);
    assert_scalable_video_rid_for_publishers(&scenario, &receiver, [1], "hi").await;
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let route = SingleVideoRoute {
        receiver,
        session_key: scenario
            .room
            .transport_user_key(&UserId::Integer(2), connection_id)
            .await,
        source_media: source_media_id(
            &scenario.room,
            &UserId::Integer(1),
            TestSourceKind::ScalableVideo,
        )
        .await,
    };
    let policy_updates = scenario.adapter.source_policy_subscription();
    let _ = policy_updates.take_pending_updates();
    let transitions_before = route_transition_counts(&scenario.room);
    assert_pressure_hysteresis_hold(&scenario, &route, transitions_before).await;
    let paused = assert_budget_pause(&scenario, &route, &policy_updates, transitions_before).await;
    assert_repeated_pause_is_silent(&scenario, &route, paused).await;
    let resumed = assert_recovery_hysteresis_and_resume(&scenario, &route, paused).await;
    assert_upgrade_is_not_degradation(&scenario, &route, resumed).await;
}

#[tokio::test]
async fn constrained_bandwidth_preserves_demand_for_two_paused_routes() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(1), &scenario.adapter).await;
    publish_three_layer_camera(&scenario.room, &UserId::Integer(3), &scenario.adapter).await;
    let receiver = UserId::Integer(2);
    assert_scalable_video_rid_for_publishers(&scenario, &receiver, [1, 3], "hi").await;
    let first_source_media = source_media_id(
        &scenario.room,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let third_source_media = source_media_id(
        &scenario.room,
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
    )
    .await;
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let session_key = scenario
        .room
        .transport_user_key(&receiver, connection_id)
        .await;
    let constrained_bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key.clone(), Bitrate::from_kbps(100))],
    };
    apply_policy_observations(
        &scenario,
        &constrained_bandwidth,
        VideoAdaptationTuning::DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS,
    )
    .await;

    for publisher in [UserId::Integer(1), UserId::Integer(3)] {
        assert_subscription_policy_pause_reason(
            &scenario.room,
            &scenario.adapter,
            &receiver,
            &publisher,
            TestSourceKind::ScalableVideo,
            Some(DiagnosticsPolicyPauseReason::BudgetPressure),
        )
        .await;
    }
    assert_eq!(
        receiver_selected_video_bitrate(&scenario.room, &scenario.adapter, &receiver).await,
        Bitrate::zero()
    );
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        Bitrate::from_kbps(1_800),
    )
    .await;
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_source_media]).await,
        vec![UserId::Integer(3)]
    );
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [third_source_media]).await,
        vec![UserId::Integer(1)]
    );

    let recovered_bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key, Bitrate::from_kbps(300))],
    };
    apply_policy_observations(
        &scenario,
        &recovered_bandwidth,
        VideoAdaptationTuning::DEFAULT_UPSWITCH_STABLE_OBSERVATIONS,
    )
    .await;

    assert_scalable_video_rid_for_publishers(&scenario, &receiver, [1, 3], "lo").await;
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        Bitrate::from_kbps(1_800),
    )
    .await;
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [first_source_media]).await,
        vec![UserId::Integer(2), UserId::Integer(3)]
    );
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [third_source_media]).await,
        vec![UserId::Integer(1), UserId::Integer(2)]
    );
}

#[tokio::test]
async fn zero_budget_pauses_observed_ridless_readable_video() {
    let (room, adapter, _publisher_rx, _subscriber_rx) = setup_two_ready_users().await;
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    publish_track(
        &room,
        &publisher,
        TestSourceKind::ReadableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &adapter,
    )
    .await;
    let source_media = source_media_id(&room, &publisher, TestSourceKind::ReadableVideo).await;
    let connection_id = user_connection_id(&room, &receiver).await;
    let session_key = room.transport_user_key(&receiver, connection_id).await;
    let receiver_bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key, Bitrate::zero())],
    };
    let source_bitrate = TransportBitrateSnapshot {
        total: Bitrate::from_kbps(500),
        per_media: vec![(source_media, Bitrate::from_kbps(500))],
    };
    for _ in 0..VideoAdaptationTuning::DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS {
        let tx = {
            let state = room.state.read().await;
            SourcePolicyTransaction::plan(&state, &[], &receiver_bandwidth, &source_bitrate)
                .expect("observed source bitrate should produce a budget update")
        };
        tx.execute(&room, &adapter).await;
    }

    assert_subscription_policy_pause_reason(
        &room,
        &adapter,
        &receiver,
        &publisher,
        TestSourceKind::ReadableVideo,
        Some(DiagnosticsPolicyPauseReason::BudgetPressure),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_ridless_pause_preserves_observed_committed_bitrate() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    publish_track(
        &scenario.room,
        &publisher,
        TestSourceKind::ReadableVideo,
        MediaKind::Video,
        test_video_rtp_parameters(),
        &scenario.adapter,
    )
    .await;
    // Publishing can consume the first pressure observation. Reset the route so
    // the explicit snapshots straddle the hysteresis threshold.
    reset_subscription_selection_to_open(
        &scenario.room,
        &receiver,
        &publisher,
        TestSourceKind::ReadableVideo,
    )
    .await;
    let source_media =
        source_media_id(&scenario.room, &publisher, TestSourceKind::ReadableVideo).await;
    let connection_id = user_connection_id(&scenario.room, &receiver).await;
    let session_key = scenario
        .room
        .transport_user_key(&receiver, connection_id)
        .await;
    let receiver_bandwidth = ReceiverBandwidthSnapshot {
        per_session: vec![(session_key.clone(), Bitrate::zero())],
    };
    let source_bitrate = TransportBitrateSnapshot {
        total: Bitrate::from_kbps(500),
        per_media: vec![(source_media, Bitrate::from_kbps(500))],
    };
    for _ in 0..VideoAdaptationTuning::DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS - 1 {
        let tx = {
            let state = scenario.room.state.read().await;
            SourcePolicyTransaction::plan(&state, &[], &receiver_bandwidth, &source_bitrate)
                .expect("pressure observation should produce a policy update")
        };
        tx.execute(&scenario.room, &scenario.adapter).await;
    }
    let tx = {
        let state = scenario.room.state.read().await;
        SourcePolicyTransaction::plan(&state, &[], &receiver_bandwidth, &source_bitrate)
            .expect("final pressure observation should plan a pause")
    };
    scenario
        .adapter
        .teardown([TransportTeardown::CloseSession { session_key }])
        .await;
    let transitions_before = route_transition_counts(&scenario.room);
    let capture = test_tracing::capture().await;

    tx.execute(&scenario.room, &scenario.adapter).await;

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &receiver,
        &publisher,
        TestSourceKind::ReadableVideo,
        None,
    )
    .await;
    assert_no_route_change_event(&scenario.room, &receiver);
    drop(capture);
    assert_eq!(route_transition_counts(&scenario.room), transitions_before);
    assert_receiver_video_allocation(
        &scenario,
        &receiver,
        TestSourceKind::ReadableVideo,
        Bitrate::zero(),
        1,
        1,
        Bitrate::from_kbps(500),
    )
    .await;
}

#[tokio::test]
async fn pinned_camera_layout_overrides_active_speaker_bias_for_that_receiver() {
    let scenario = SourcePolicyScenario::three_ready_users().await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let third_audio_media_id = scenario.audio_media_id(3).await;

    scenario.mark_active_speaker(third_audio_media_id).await;
    scenario.refresh_policy().await;
    scenario
        .set_scalable_video_layout(2, 1, VideoLayoutIntent::Pinned)
        .await;

    assert_subscription_layout(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        TestSourceKind::ScalableVideo,
        DiagnosticsVideoLayoutRole::Pinned,
        DiagnosticsVideoRoutePriority::PinnedOrFeatured,
    )
    .await;
}

#[tokio::test]
async fn screen_share_layout_uses_screen_specific_priority_in_diagnostics() {
    let (room, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_users().await;

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

#[tokio::test(flavor = "current_thread")]
async fn source_policy_replaced_route_does_not_commit_stale_selector_update() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let (tx, third_camera_source_id) = third_camera_policy_transaction(&scenario).await;
    let receiver = UserId::Integer(2);
    let (replacement_tx, _replacement_rx) = test_sender();
    join_user_without_transport_teardown(
        &scenario.room,
        &scenario.adapter,
        receiver.clone(),
        replacement_tx,
    )
    .await;
    make_session_ready_with_transport(&scenario.room, &receiver, &scenario.adapter).await;
    let replacement_selection = {
        let state = scenario.room.state.read().await;
        state
            .topology
            .source_selection_for_test(&receiver, third_camera_source_id)
            .expect("replacement route should select the current publication")
    };
    let transitions_before = route_transition_counts(&scenario.room);
    let selection_updates_before = source_selection_update_count(&scenario.room, "encoding");
    let capture = test_tracing::capture().await;
    tx.execute(&scenario.room, &scenario.adapter).await;
    assert_no_route_change_event(&scenario.room, &receiver);
    drop(capture);
    assert_eq!(route_transition_counts(&scenario.room), transitions_before);
    assert_eq!(
        source_selection_update_count(&scenario.room, "encoding"),
        selection_updates_before
    );
    let current_selection = {
        let state = scenario.room.state.read().await;
        state
            .topology
            .source_selection_for_test(&receiver, third_camera_source_id)
    };
    assert_eq!(current_selection, Some(replacement_selection));
}

#[tokio::test(flavor = "current_thread")]
async fn source_policy_rejected_transport_gate_does_not_commit_selector_update() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let (tx, _) = third_camera_policy_transaction(&scenario).await;
    let receiver_connection_id = user_connection_id(&scenario.room, &UserId::Integer(2)).await;
    let receiver_session_key = scenario
        .room
        .transport_user_key(&UserId::Integer(2), receiver_connection_id)
        .await;

    scenario
        .adapter
        .teardown([TransportTeardown::CloseSession {
            session_key: receiver_session_key,
        }])
        .await;
    let transitions_before = route_transition_counts(&scenario.room);
    let selection_updates_before = source_selection_update_count(&scenario.room, "encoding");
    let capture = test_tracing::capture().await;
    tx.execute(&scenario.room, &scenario.adapter).await;
    assert_no_route_change_event(&scenario.room, &UserId::Integer(2));
    drop(capture);
    assert_eq!(route_transition_counts(&scenario.room), transitions_before);
    assert_eq!(
        source_selection_update_count(&scenario.room, "encoding"),
        selection_updates_before
    );

    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
    )
    .await;
}

async fn third_camera_policy_transaction(
    scenario: &SourcePolicyScenario,
) -> (SourcePolicyTransaction, PublishedSourceId) {
    let third_audio_media_id = scenario.audio_media_id(3).await;
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &UserId::Integer(2),
        &UserId::Integer(3),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::VideoDownloadLimit),
    )
    .await;
    scenario.mark_active_speaker(third_audio_media_id).await;
    let third_camera_source_id = scenario
        .room
        .test_api()
        .inspect()
        .source_id_for_owner_stream(&UserId::Integer(3), TestSourceKind::ScalableVideo)
        .await
        .expect("third camera should have a source id before stale source policy work");
    let tx = source_policy_transaction_from_transport_snapshot(scenario).await;
    (tx, third_camera_source_id)
}

async fn source_policy_transaction_from_transport_snapshot(
    scenario: &SourcePolicyScenario,
) -> SourcePolicyTransaction {
    let active_speaker_sources = scenario.adapter.active_speaker_source_snapshot().await;
    let session_keys = {
        let state = scenario.room.state.read().await;
        state
            .transport_user_entries()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let receiver_bandwidth_snapshot = scenario.adapter.receiver_bandwidth_snapshot(&session_keys);
    let state = scenario.room.state.read().await;
    plan_policy(
        &state,
        &active_speaker_sources,
        &receiver_bandwidth_snapshot,
    )
    .expect("source policy transaction should contain work before execution")
}

async fn assert_featured(scenario: &SourcePolicyScenario, user_id: i64, expected: bool) {
    let info = scenario
        .room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(user_id))
        .await
        .expect("user should still be present")
        .1;
    assert_eq!(info.is_featured, Some(expected));
}

fn assert_camera_feature_fanout(rx: &mut UserOutboundReceiver, user: &UserId, expected: bool) {
    let info = drain_outbound(rx)
        .into_iter()
        .rev()
        .find_map(|message| match message {
            UserOutbound::Message(RoomEventMessage::UserInfoChanged(mut snapshot)) => {
                snapshot.remove(user)
            }
            UserOutbound::Message(_) | UserOutbound::RemoteTracks(_) | UserOutbound::Close(_) => {
                None
            }
        })
        .expect("user info fanout should contain the target user");
    assert_eq!(info.is_camera_on, Some(expected));
    assert_eq!(info.is_featured, Some(expected));
}

async fn reset_subscription_selection_to_open(
    room: &Room,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
) {
    update_subscription_selection(
        room,
        consumer_user_id,
        producer_user_id,
        stream_type,
        |selection| *selection = ConsumerSourceSelection::open(true),
    )
    .await;
}

async fn update_subscription_selection(
    room: &Room,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
    update: impl FnOnce(&mut ConsumerSourceSelection),
) {
    let stream_id = stream_id_for_source(stream_type);
    let state = room.state.read().await;
    let route = state
        .topology
        .committed_consumer_routes()
        .find(|route| {
            route.key.receiver == *consumer_user_id
                && route.source.descriptor.owner().user_id() == producer_user_id
                && route.source.descriptor.stream_id() == &stream_id
        })
        .expect("test should have a live subscription route");
    let key = route.key.clone();
    let transport_route = route.route.clone();
    let source_id = route.source.descriptor.source_id();
    drop(state);

    let mut state = room.state.write().await;
    let updated =
        state
            .topology
            .update_consumer_source_selection(&key, source_id, &transport_route, update);
    drop(state);
    assert!(updated);
}

fn keyframe_request_count(metrics: &RuntimeMetrics) -> u64 {
    let snapshot = metrics.snapshot();
    snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RouteTransitionCounts {
    degraded: u64,
    paused: u64,
    resumed: u64,
}

fn route_transition_counts(room: &Room) -> RouteTransitionCounts {
    let snapshot = room.metrics.snapshot();
    let outcome_count = |value| {
        snapshot.counter_value(MetricName::BudgetSolverOutcomesTotal, &[("outcome", value)])
    };
    RouteTransitionCounts {
        degraded: outcome_count("degraded"),
        paused: outcome_count("paused"),
        resumed: outcome_count("resumed"),
    }
}

fn source_selection_update_count(room: &Room, selector: &str) -> u64 {
    room.metrics.snapshot().counter_value(
        MetricName::SourceSelectionUpdatesTotal,
        &[("selector", selector)],
    )
}

struct SingleVideoRoute {
    receiver: UserId,
    session_key: TransportSessionKey,
    source_media: TransportMediaId,
}

impl SingleVideoRoute {
    fn bandwidth(&self, kbps: u64) -> ReceiverBandwidthSnapshot {
        ReceiverBandwidthSnapshot {
            per_session: vec![(self.session_key.clone(), Bitrate::from_kbps(kbps))],
        }
    }
}

async fn assert_pressure_hysteresis_hold(
    scenario: &SourcePolicyScenario,
    route: &SingleVideoRoute,
    expected_transitions: RouteTransitionCounts,
) {
    let capture = test_tracing::capture().await;
    apply_policy_observations(
        scenario,
        &route.bandwidth(100),
        VideoAdaptationTuning::DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS - 1,
    )
    .await;
    assert_no_route_change_event(&scenario.room, &route.receiver);
    drop(capture);
    assert_scalable_video_rid_for_publishers(scenario, &route.receiver, [1], "hi").await;
    assert_receiver_video_allocation(
        scenario,
        &route.receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(100),
        1,
        1,
        Bitrate::from_kbps(900),
    )
    .await;
    assert_eq!(
        route_transition_counts(&scenario.room),
        expected_transitions
    );
}

async fn assert_budget_pause(
    scenario: &SourcePolicyScenario,
    route: &SingleVideoRoute,
    policy_updates: &SourcePolicyUpdateSubscription,
    previous_transitions: RouteTransitionCounts,
) -> RouteTransitionCounts {
    let capture = test_tracing::capture().await;
    apply_policy_observations(scenario, &route.bandwidth(100), 1).await;
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &route.receiver,
        Bitrate::from_kbps(900),
    )
    .await;
    let follow_up = timeout(Duration::from_secs(1), policy_updates.wait_for_update())
        .await
        .expect("unresolved hysteresis should schedule another policy pass");
    assert!(follow_up.contains(&scenario.room.instance_id()));
    assert_subscription_policy_pause_reason(
        &scenario.room,
        &scenario.adapter,
        &route.receiver,
        &UserId::Integer(1),
        TestSourceKind::ScalableVideo,
        Some(DiagnosticsPolicyPauseReason::BudgetPressure),
    )
    .await;
    assert_receiver_video_allocation(
        scenario,
        &route.receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(100),
        1,
        0,
        Bitrate::zero(),
    )
    .await;
    let transitions = RouteTransitionCounts {
        paused: previous_transitions.paused + 1,
        ..previous_transitions
    };
    assert_eq!(route_transition_counts(&scenario.room), transitions);
    assert_route_change_event(
        scenario,
        &route.receiver,
        &UserId::Integer(1),
        &route.session_key,
        route.source_media,
        ExpectedRouteChange {
            outcome: "paused",
            reason: Some("budget_pressure"),
            receiver_bandwidth: Bitrate::from_kbps(100),
            video_budget: Bitrate::from_kbps(100),
            active_route_count: 0,
            selected_video_bitrate: Bitrate::zero(),
            selected_estimated_bitrate: Bitrate::from_kbps(900),
        },
    )
    .await;
    drop(capture);
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [route.source_media]).await,
        Vec::<UserId>::new()
    );
    transitions
}

async fn assert_repeated_pause_is_silent(
    scenario: &SourcePolicyScenario,
    route: &SingleVideoRoute,
    expected_transitions: RouteTransitionCounts,
) {
    let capture = test_tracing::capture().await;
    apply_policy_observations(scenario, &route.bandwidth(90), 1).await;
    assert_no_route_change_event(&scenario.room, &route.receiver);
    drop(capture);
    assert_receiver_video_allocation(
        scenario,
        &route.receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(90),
        1,
        0,
        Bitrate::zero(),
    )
    .await;
    assert_eq!(
        route_transition_counts(&scenario.room),
        expected_transitions
    );
}

async fn assert_recovery_hysteresis_and_resume(
    scenario: &SourcePolicyScenario,
    route: &SingleVideoRoute,
    paused_transitions: RouteTransitionCounts,
) -> RouteTransitionCounts {
    let recovered_bandwidth = route.bandwidth(200);
    let held_capture = test_tracing::capture().await;
    apply_policy_observations(
        scenario,
        &recovered_bandwidth,
        VideoAdaptationTuning::DEFAULT_UPSWITCH_STABLE_OBSERVATIONS - 1,
    )
    .await;
    assert_no_route_change_event(&scenario.room, &route.receiver);
    drop(held_capture);
    assert_eq!(route_transition_counts(&scenario.room), paused_transitions);
    assert_receiver_video_allocation(
        scenario,
        &route.receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(200),
        1,
        0,
        Bitrate::zero(),
    )
    .await;

    let resume_capture = test_tracing::capture().await;
    apply_policy_observations(scenario, &recovered_bandwidth, 1).await;
    assert_scalable_video_rid_for_publishers(scenario, &route.receiver, [1], "lo").await;
    let resumed_transitions = RouteTransitionCounts {
        resumed: paused_transitions.resumed + 1,
        ..paused_transitions
    };
    assert_eq!(route_transition_counts(&scenario.room), resumed_transitions);
    assert_receiver_video_allocation(
        scenario,
        &route.receiver,
        TestSourceKind::ScalableVideo,
        Bitrate::from_kbps(200),
        1,
        1,
        Bitrate::from_kbps(150),
    )
    .await;
    assert_route_change_event(
        scenario,
        &route.receiver,
        &UserId::Integer(1),
        &route.session_key,
        route.source_media,
        ExpectedRouteChange {
            outcome: "resumed",
            reason: Some("budget_pressure"),
            receiver_bandwidth: Bitrate::from_kbps(200),
            video_budget: Bitrate::from_kbps(200),
            active_route_count: 1,
            selected_video_bitrate: Bitrate::from_kbps(150),
            selected_estimated_bitrate: Bitrate::from_kbps(150),
        },
    )
    .await;
    drop(resume_capture);
    assert_eq!(
        active_destination_receivers(&scenario.adapter, [route.source_media]).await,
        vec![route.receiver.clone()]
    );
    resumed_transitions
}

async fn assert_upgrade_is_not_degradation(
    scenario: &SourcePolicyScenario,
    route: &SingleVideoRoute,
    expected_transitions: RouteTransitionCounts,
) {
    let capture = test_tracing::capture().await;
    apply_policy_observations(
        scenario,
        &route.bandwidth(450),
        VideoAdaptationTuning::DEFAULT_UPSWITCH_STABLE_OBSERVATIONS,
    )
    .await;
    assert_no_route_change_event(&scenario.room, &route.receiver);
    drop(capture);
    assert_scalable_video_rid_for_publishers(scenario, &route.receiver, [1], "mid").await;
    assert_eq!(
        route_transition_counts(&scenario.room),
        expected_transitions
    );
    assert_receiver_bwe_target(
        &scenario.room,
        &scenario.adapter,
        &route.receiver,
        Bitrate::from_kbps(900),
    )
    .await;
}

fn assert_no_route_change_event(room: &Room, receiver: &UserId) {
    test_tracing::assert_no_event(
        telemetry_event::SOURCE_POLICY_ROUTE_CHANGED,
        &[
            ("room_id", json!(room.uuid())),
            ("user_id", json!(receiver.path_segment())),
        ],
    );
}

async fn assert_receiver_video_allocation(
    scenario: &SourcePolicyScenario,
    receiver: &UserId,
    stream_type: TestSourceKind,
    video_budget: Bitrate,
    subscription_count: usize,
    active_route_count: usize,
    selected_video_bitrate: Bitrate,
) {
    let (users, _) = diagnostics_room_views(&scenario.room, &scenario.adapter).await;
    let stream_id = stream_id_for_source(stream_type);
    let subscriptions = users
        .iter()
        .find(|user| &user.user_id == receiver)
        .expect("diagnostics should include the receiver")
        .subscriptions
        .iter()
        .filter(|subscription| subscription.stream_id == stream_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(subscriptions.len(), subscription_count);
    for subscription in subscriptions {
        assert_eq!(
            subscription
                .selection
                .latest_receiver_bandwidth_estimate_bps,
            Some(video_budget.as_bps())
        );
        assert_eq!(
            subscription.selection.selected_video_budget_bps,
            Some(video_budget.as_bps())
        );
        assert_eq!(
            subscription.selection.active_video_route_count,
            active_route_count
        );
        assert_eq!(
            subscription.selection.selected_video_bitrate_bps,
            selected_video_bitrate.as_bps()
        );
    }
}

struct ExpectedRouteChange<'a> {
    outcome: &'a str,
    reason: Option<&'a str>,
    receiver_bandwidth: Bitrate,
    video_budget: Bitrate,
    active_route_count: usize,
    selected_video_bitrate: Bitrate,
    selected_estimated_bitrate: Bitrate,
}

async fn assert_route_change_event(
    scenario: &SourcePolicyScenario,
    receiver: &UserId,
    publisher: &UserId,
    session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    expected: ExpectedRouteChange<'_>,
) {
    let (users, _) = diagnostics_room_views(&scenario.room, &scenario.adapter).await;
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
    let subscription = users
        .iter()
        .find(|user| &user.user_id == receiver)
        .and_then(|user| {
            user.subscriptions.iter().find(|subscription| {
                subscription.producer_user_id == *publisher
                    && subscription.stream_id == stream_id.as_str()
            })
        })
        .expect("diagnostics should include the subscription");
    let selection = &subscription.selection;
    let receiver_id = receiver.path_segment();
    let consumer_media_id = subscription
        .consumer_transport_media_id
        .expect("committed subscription should have a transport media id");
    let encoding_id = selection
        .selected_encoding_id
        .expect("scalable route should select an encoding");
    let mut fields = vec![
        ("transport_media_id", json!(consumer_media_id)),
        ("producer_user_id", json!(publisher.path_segment())),
        ("source_transport_media_id", json!(source_media_id.as_u64())),
        ("stream_id", json!(stream_id)),
        ("outcome", json!(expected.outcome)),
        (
            "latest_receiver_bandwidth_estimate_bps",
            json!(expected.receiver_bandwidth.as_bps()),
        ),
        (
            "selected_video_budget_bps",
            json!(expected.video_budget.as_bps()),
        ),
        (
            "planned_active_video_route_count",
            json!(expected.active_route_count),
        ),
        (
            "planned_selected_video_bitrate_bps",
            json!(expected.selected_video_bitrate.as_bps()),
        ),
        ("selector", json!("encoding")),
        ("selected_encoding_id", json!(encoding_id)),
        (
            "selected_estimated_bitrate_bps",
            json!(expected.selected_estimated_bitrate.as_bps()),
        ),
    ];
    if let Some(reason) = expected.reason {
        fields.push(("reason", json!(reason)));
    }
    test_tracing::assert_user_exact(
        telemetry_event::SOURCE_POLICY_ROUTE_CHANGED,
        scenario.room.uuid(),
        receiver_id.as_ref(),
        session_key.connection_id().as_u64(),
        session_key.media_worker_id().as_usize(),
        &fields,
    );
}
