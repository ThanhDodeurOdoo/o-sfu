use std::time::Duration;

use super::support::*;
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, ReceiverBandwidthSnapshot, TransportMediaId, TransportTeardown,
    },
    room::source_policy::SourcePolicyTransaction,
    source_model::{ConsumerSourceSelection, PublishedSourceId, SourcePolicy, SourcePublishIntent},
};

#[tokio::test]
async fn two_party_camera_publish_selects_the_highest_consumer_layer() {
    let (room, adapter, metrics, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users_with_media_metrics().await;

    publish_simulcast_camera(&room, &UserId::Integer(1), &adapter).await;

    assert!(drain_outbound(&mut publisher_rx).is_empty());
    assert_remote_source_snapshot_for_stream(
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
    let sources = room.diagnostics_sources(&adapter).await;
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
        SourcePolicyTransaction::plan_from_state(
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
async fn source_policy_resets_receiver_bwe_target_after_last_video_route_removal() {
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
            .unpublish_track(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &adapter,
            )
            .await
    );
    refresh_source_policy(&room, &adapter).await;

    assert_receiver_bwe_target(&room, &adapter, &UserId::Integer(2), Bitrate::zero()).await;
}

#[tokio::test]
async fn source_policy_stale_featured_update_does_not_mark_replacement_user() {
    let scenario = SourcePolicyScenario::with_ready_users(&[1, 2]).await;
    let featured_user_id = UserId::Integer(1);
    scenario.publish_audio_and_camera(1).await;
    let audio_media_id = scenario.audio_media_id(1).await;
    scenario.mark_active_speaker(audio_media_id).await;
    let tx = source_policy_transaction_from_transport_snapshot(&scenario).await;

    let (replacement_tx, _replacement_rx) = test_sender();
    join_user_without_transport_teardown(
        &scenario.room,
        &scenario.adapter,
        featured_user_id.clone(),
        replacement_tx,
    )
    .await;
    tx.execute(&scenario.room, &scenario.adapter).await;

    let info = scenario
        .room
        .test_api()
        .inspect()
        .user_info_snapshot(&featured_user_id)
        .await
        .expect("replacement user should still be present")
        .1;
    assert_eq!(info.is_featured, Some(false));
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
        SourcePolicyTransaction::plan_from_state(&state, &[], &receiver_bandwidth_snapshot)
            .expect("source policy transaction should contain current bandwidth work")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

    let diagnostics = scenario
        .room
        .diagnostics_user_views(&scenario.adapter)
        .await;
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

    let first_info = scenario
        .room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(1))
        .await
        .unwrap()
        .1;
    let third_info = scenario
        .room
        .test_api()
        .inspect()
        .user_info_snapshot(&UserId::Integer(3))
        .await
        .unwrap()
        .1;
    assert_eq!(first_info.is_featured, Some(false));
    assert_eq!(third_info.is_featured, Some(true));

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
async fn audio_only_speaker_limit_ignores_foreign_sources() {
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
        SourcePolicyTransaction::plan_from_state(
            &state,
            &speakers,
            &ReceiverBandwidthSnapshot::default(),
        )
        .expect("audio speaker limit should update the overflow route")
    };
    tx.execute(&scenario.room, &scenario.adapter).await;

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
    assert_eq!(
        active_destination_count_for_receiver(
            &scenario.adapter,
            [first_camera_media_id, third_camera_media_id],
            &UserId::Integer(2),
        )
        .await,
        1
    );
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

#[tokio::test]
async fn source_policy_removed_route_does_not_commit_stale_selector_update() {
    let scenario = SourcePolicyScenario::with_ready_users_and_media_limits(
        &[1, 2, 3],
        RoomMediaLimits::try_new(4, 1).unwrap(),
    )
    .await;
    scenario.publish_audio_and_camera_for_users(&[1, 3]).await;
    let (tx, third_camera_source_id) = third_camera_policy_transaction(&scenario).await;

    assert!(
        scenario
            .room
            .test_api()
            .media()
            .unpublish_track(
                &UserId::Integer(3),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &scenario.adapter,
            )
            .await
    );
    tx.execute(&scenario.room, &scenario.adapter).await;

    assert!(
        !scenario
            .room
            .test_api()
            .inspect()
            .contains_consumer_source_selection(&UserId::Integer(2), third_camera_source_id)
            .await
    );
}

#[tokio::test]
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
    tx.execute(&scenario.room, &scenario.adapter).await;

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
            .into_iter()
            .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let receiver_bandwidth_snapshot = scenario.adapter.receiver_bandwidth_snapshot(&session_keys);
    let state = scenario.room.state.read().await;
    SourcePolicyTransaction::plan_from_state(
        &state,
        &active_speaker_sources,
        &receiver_bandwidth_snapshot,
    )
    .expect("source policy transaction should contain work before execution")
}

async fn reset_subscription_selection_to_open(
    room: &Room,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
) {
    let stream_id = stream_id_for_source(stream_type);
    let state = room.state.read().await;
    let route = state
        .topology
        .live_consumer_routes()
        .find(|route| {
            route.consumer_user_id == *consumer_user_id
                && route.source.descriptor.owner().user_id() == producer_user_id
                && route.source.descriptor.stream_id() == &stream_id
        })
        .expect("test should have a live subscription route");
    let transport_ref = route.transport_ref();
    let source_id = route.source.descriptor.source_id();
    drop(state);

    let mut state = room.state.write().await;
    let updated =
        state
            .topology
            .update_consumer_source_selection(&transport_ref, source_id, |selection| {
                *selection = ConsumerSourceSelection::open(true);
            });
    drop(state);
    assert!(updated);
}

fn keyframe_request_count(metrics: &RuntimeMetrics) -> u64 {
    let snapshot = metrics.snapshot();
    snapshot.rtc_keyframe_requests_forwarded() + snapshot.rtc_keyframe_requests_absorbed()
}
