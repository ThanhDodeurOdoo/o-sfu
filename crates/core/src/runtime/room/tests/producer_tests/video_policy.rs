use super::support::*;

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
