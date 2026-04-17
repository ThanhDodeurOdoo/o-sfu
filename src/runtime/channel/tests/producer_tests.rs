use super::fixtures::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::channel::{Channel, NegotiatedPublish};
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;
use crate::runtime::test_rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters;
use crate::runtime::transport_adapter::{
    FakeWebRtcEvent, RtcTransportAdapterShardSetConfig, SourcePacketGate, TransportMediaId,
    TransportSessionKey,
};
use o_sfu_router::MediaKind;
use str0m::{Candidate, Rtc, change::SdpOffer};

#[tokio::test]
async fn production_change_pauses_producer_and_broadcasts_info() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    // Session 1 publishes a camera track.
    let producer_id = channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    assert!(producer_id.is_some());

    // Drain the INIT_CONSUMER bootstrap that went to session 2.
    let bootstrap_msgs = drain_outbound(&mut rx2);
    assert!(
        bootstrap_msgs
            .iter()
            .any(|m| matches!(m, SessionOutbound::Request(..))),
        "session 2 should have received a bootstrap remote track request"
    );
    // Session 1 shouldn't get its own consumer.
    assert!(drain_outbound(&mut rx1).is_empty());

    // Now session 1 sends PRODUCTION_CHANGE: camera off (pause).
    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // Both sessions should receive a session info broadcast with isCameraOn = false.
    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "session 1 should get info broadcast");
    assert_eq!(msgs2.len(), 1, "session 2 should get info broadcast");

    // Verify the broadcast contains isCameraOn = false.
    let info_msg = &msgs1[0];
    if let SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(snapshot)) = info_msg {
        let info = snapshot
            .values()
            .next()
            .expect("snapshot should have one entry");
        assert_eq!(info.is_camera_on, Some(false));
    } else {
        panic!("expected SessionInfoChanged, got {info_msg:?}");
    }

    // Resume: session 1 sends PRODUCTION_CHANGE: camera on.
    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Camera, true, &adapter)
        .await;

    let msgs1 = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(snapshot)) = &msgs1[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_camera_on, Some(true));
    } else {
        panic!("expected SessionInfoChanged after resume");
    }
}

#[tokio::test]
async fn explicit_unpublish_removes_published_track_and_consumer_routes() {
    let (channel, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_sessions_with_fake().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
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
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );
    let Some(transport_media_id) = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Camera)
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert!(
        channel
            .unpublish_track(&SessionId::Integer(1), 0, StreamType::Camera, &adapter)
            .await
    );

    assert_eq!(channel.producer_count().await, 0);
    assert_eq!(channel.consumer_count().await, 0);
    assert!(
        !channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Camera)
            .await
    );
    assert!(
        channel
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );

    let publisher_messages = drain_outbound(&mut publisher_rx);
    let subscriber_messages = drain_outbound(&mut subscriber_rx);
    assert!(publisher_messages.iter().any(|message| matches!(
        message,
        SessionOutbound::TrackBindingUpdate(update)
            if update.session_id == SessionId::Integer(1)
                && update.stream_type == StreamType::Camera
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        SessionOutbound::TrackBindingUpdate(update)
            if update.session_id == SessionId::Integer(1)
                && update.stream_type == StreamType::Camera
                && update.active.is_none()
    )));
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(snapshot))
            if snapshot.values().next().is_some_and(|info| info.is_camera_on.is_none())
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
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for raw_session_id in [1_i64, 2, 3] {
        let (sender, _receiver) = test_sender();
        let session_id = SessionId::Integer(raw_session_id);
        channel
            .join_session(
                session_id.clone(),
                None,
                SessionPermissions::default(),
                sender,
            )
            .await
            .expect("session should join");
        channel.set_publish_transport_ready(&session_id).await;
        channel.set_consume_transport_ready(&session_id).await;
        channel
            .set_client_rtp_capabilities(&session_id, test_client_rtp_capabilities())
            .await;
    }

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );

    let Some(transport_media_id) = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Camera)
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert!(fake.snapshot_events().iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id: updated_media_id,
                packet_gate: Some(SourcePacketGate::Rid(rid)),
            } if *session_id == SessionId::Integer(1)
                && *updated_media_id == transport_media_id
                && rid == "lo"
        )
    }));
}

#[tokio::test]
async fn two_party_camera_publish_keeps_the_initial_simulcast_selection_unset() {
    let (channel, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_sessions_with_fake().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
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
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );
    assert!(
        !fake
            .snapshot_events()
            .iter()
            .any(|event| matches!(event, FakeWebRtcEvent::SourcePacketGateUpdated { .. })),
        "two-party camera publish should not force a shared source layer yet"
    );
}

#[tokio::test]
async fn joining_a_third_session_applies_the_shared_camera_source_selection() {
    let (channel, adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_sessions_with_fake().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
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
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );

    let Some(transport_media_id) = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Camera)
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    let (sender, _receiver) = test_sender();
    channel
        .join_session_runtime(
            SessionId::Integer(3),
            None,
            SessionPermissions::default(),
            sender,
            &adapter,
            super::super::SessionCleanupPolicy::StateOnly,
        )
        .await
        .expect("third session should join");

    assert!(fake.snapshot_events().iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id: updated_media_id,
                packet_gate: Some(SourcePacketGate::Rid(rid)),
            } if *session_id == SessionId::Integer(1)
                && *updated_media_id == transport_media_id
                && rid == "lo"
        )
    }));
}

#[tokio::test]
async fn leaving_a_multiparty_room_clears_the_shared_camera_source_selection() {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for raw_session_id in [1_i64, 2, 3] {
        let (sender, _receiver) = test_sender();
        let session_id = SessionId::Integer(raw_session_id);
        channel
            .join_session_runtime(
                session_id.clone(),
                None,
                SessionPermissions::default(),
                sender,
                &adapter,
                super::super::SessionCleanupPolicy::StateOnly,
            )
            .await
            .expect("session should join");
        channel.set_publish_transport_ready(&session_id).await;
        channel.set_consume_transport_ready(&session_id).await;
        channel
            .set_client_rtp_capabilities(&session_id, test_client_rtp_capabilities())
            .await;
    }

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_simulcast_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );

    let Some(transport_media_id) = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Camera)
        .await
    else {
        panic!("published camera should expose a transport media id");
    };

    assert!(
        channel
            .leave_session_runtime(
                &SessionId::Integer(3),
                2,
                &adapter,
                super::super::SessionCleanupPolicy::StateOnly,
            )
            .await
    );

    assert!(fake.snapshot_events().iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id: updated_media_id,
                packet_gate: None,
            } if *session_id == SessionId::Integer(1)
                && *updated_media_id == transport_media_id
        )
    }));
}

async fn setup_ready_sessions_with_fake(
    session_ids: &[i64],
) -> (
    Arc<Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
) {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (adapter, fake) = fake_adapter();
    for &raw_session_id in session_ids {
        let (sender, _receiver) = test_sender();
        let session_id = SessionId::Integer(raw_session_id);
        channel
            .join_session_runtime(
                session_id.clone(),
                None,
                SessionPermissions::default(),
                sender,
                &adapter,
                super::super::SessionCleanupPolicy::StateOnly,
            )
            .await
            .expect("session should join");
        channel.set_publish_transport_ready(&session_id).await;
        channel.set_consume_transport_ready(&session_id).await;
        channel
            .set_client_rtp_capabilities(&session_id, test_client_rtp_capabilities())
            .await;
    }
    (channel, adapter, fake)
}

async fn setup_three_ready_sessions_with_fake() -> (
    Arc<Channel>,
    RuntimeTransportAdapter,
    Arc<FakeWebRtcAdapter>,
) {
    setup_ready_sessions_with_fake(&[1, 2, 3]).await
}

async fn publish_audio_and_camera(
    channel: &Arc<Channel>,
    session_id: &SessionId,
    adapter: &RuntimeTransportAdapter,
) {
    assert!(
        channel
            .publish_track(
                session_id,
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                adapter,
            )
            .await
            .is_some()
    );
    assert!(
        channel
            .publish_track(
                session_id,
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
    channel: &Arc<Channel>,
    session_id: &SessionId,
) -> (TransportMediaId, TransportMediaId) {
    let Some(connection_id) = channel.session_connection_id(session_id).await else {
        panic!("session should exist");
    };
    let Some(audio_media_id) = channel
        .producer_transport_media_id(session_id, connection_id, StreamType::Audio)
        .await
    else {
        panic!("audio producer should expose a transport media id");
    };
    let Some(camera_media_id) = channel
        .producer_transport_media_id(session_id, connection_id, StreamType::Camera)
        .await
    else {
        panic!("camera producer should expose a transport media id");
    };
    (audio_media_id, camera_media_id)
}

fn assert_source_packet_selection_update(
    events: &[FakeWebRtcEvent],
    session_id: &SessionId,
    transport_media_id: TransportMediaId,
    selection: Option<&str>,
) {
    assert!(events.iter().any(|event| match (event, selection) {
        (
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id: updated_session_id,
                transport_media_id: updated_media_id,
                packet_gate: None,
            },
            None,
        ) => updated_session_id == session_id && *updated_media_id == transport_media_id,
        (
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id: updated_session_id,
                transport_media_id: updated_media_id,
                packet_gate: Some(SourcePacketGate::Rid(rid)),
            },
            Some(expected_rid),
        ) => {
            updated_session_id == session_id
                && *updated_media_id == transport_media_id
                && rid == expected_rid
        }
        _ => false,
    }));
}

#[tokio::test]
async fn dominant_speaker_camera_policy_clears_only_the_observed_speakers_gate() {
    let (channel, adapter, fake) = setup_three_ready_sessions_with_fake().await;
    for session_id in [SessionId::Integer(1), SessionId::Integer(2)] {
        publish_audio_and_camera(&channel, &session_id, &adapter).await;
    }

    let (first_audio_media_id, first_camera_media_id) =
        source_media_ids(&channel, &SessionId::Integer(1)).await;
    let (second_audio_media_id, second_camera_media_id) =
        source_media_ids(&channel, &SessionId::Integer(2)).await;

    let baseline_event_count = fake.snapshot_events().len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        second_audio_media_id,
        Instant::now(),
    )]);
    channel
        .update_session_info_runtime(
            &SessionId::Integer(2),
            SessionInfo::default(),
            false,
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let speaker_two_events = &events[baseline_event_count..];
    assert_source_packet_selection_update(
        speaker_two_events,
        &SessionId::Integer(2),
        second_camera_media_id,
        None,
    );
    assert!(!speaker_two_events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id,
                packet_gate: None,
            } if *session_id == SessionId::Integer(1)
                && *transport_media_id == first_camera_media_id
        )
    }));

    let second_baseline_event_count = events.len();
    fake.set_active_speaker_source_snapshot(vec![ActiveSpeakerSource::new(
        first_audio_media_id,
        Instant::now(),
    )]);
    channel
        .update_session_info_runtime(
            &SessionId::Integer(1),
            SessionInfo::default(),
            false,
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let speaker_one_events = &events[second_baseline_event_count..];
    assert_source_packet_selection_update(
        speaker_one_events,
        &SessionId::Integer(1),
        first_camera_media_id,
        None,
    );
    assert_source_packet_selection_update(
        speaker_one_events,
        &SessionId::Integer(2),
        second_camera_media_id,
        Some("lo"),
    );
}

#[tokio::test]
async fn active_speaker_camera_policy_clears_only_the_first_five_speakers_gates() {
    let (channel, adapter, fake) = setup_ready_sessions_with_fake(&[1, 2, 3, 4, 5, 6, 7]).await;
    for raw_session_id in 1_i64..=6 {
        publish_audio_and_camera(&channel, &SessionId::Integer(raw_session_id), &adapter).await;
    }

    let mut ordered_audio_media_ids = Vec::new();
    let mut ordered_camera_media_ids = Vec::new();
    for raw_session_id in 1_i64..=6 {
        let (audio_media_id, camera_media_id) =
            source_media_ids(&channel, &SessionId::Integer(raw_session_id)).await;
        ordered_audio_media_ids.push(audio_media_id);
        ordered_camera_media_ids.push(camera_media_id);
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
    channel
        .update_session_info_runtime(
            &SessionId::Integer(6),
            SessionInfo::default(),
            false,
            &adapter,
        )
        .await;

    let events = fake.snapshot_events();
    let active_speaker_events = &events[baseline_event_count..];
    for (camera_idx, raw_session_id) in (2_i64..=6).enumerate() {
        assert_source_packet_selection_update(
            active_speaker_events,
            &SessionId::Integer(raw_session_id),
            ordered_camera_media_ids[camera_idx + 1],
            None,
        );
    }
    assert!(!active_speaker_events.iter().any(|event| {
        matches!(
            event,
            FakeWebRtcEvent::SourcePacketGateUpdated {
                session_id,
                transport_media_id,
                packet_gate: None,
            } if *session_id == SessionId::Integer(1)
                && *transport_media_id == ordered_camera_media_ids[0]
        )
    }));
}

#[tokio::test]
async fn explicit_unpublish_preserves_state_when_transport_cleanup_fails() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    assert!(
        scenario
            .channel
            .publish_track(
                &scenario.publisher_session_id,
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
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );

    let Some(connection_id) = scenario
        .channel
        .session_connection_id(&scenario.publisher_session_id)
        .await
    else {
        panic!("publisher connection should exist");
    };
    let Some(transport_media_id) = scenario
        .channel
        .producer_transport_media_id(
            &scenario.publisher_session_id,
            connection_id,
            StreamType::Audio,
        )
        .await
    else {
        panic!("published audio should expose a transport media id");
    };
    let transport_session_key = scenario
        .channel
        .transport_session_key(&scenario.publisher_session_id, connection_id);
    scenario
        .transport_adapter
        .close_session(&transport_session_key)
        .await
        .expect("closing the publisher transport should succeed");

    assert!(
        !scenario
            .channel
            .unpublish_track(
                &scenario.publisher_session_id,
                connection_id,
                StreamType::Audio,
                &scenario.transport_adapter,
            )
            .await,
        "unpublish should abort when transport cleanup fails"
    );

    assert_eq!(scenario.channel.producer_count().await, 1);
    assert_eq!(scenario.channel.consumer_count().await, 1);
    assert!(
        scenario
            .channel
            .has_producer_route_target(
                &scenario.publisher_session_id,
                connection_id,
                StreamType::Audio,
            )
            .await
    );
    assert!(
        scenario
            .channel
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_some()
    );
    assert!(drain_outbound(&mut scenario.publisher_rx).is_empty());
    assert!(drain_outbound(&mut scenario.subscriber_rx).is_empty());
}

#[tokio::test]
async fn publish_track_uses_negotiated_consumer_rtp_parameters() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;
    assert!(
        channel
            .set_client_rtp_capabilities(
                &SessionId::Integer(2),
                test_client_rtp_capabilities_without_video_rtx(),
            )
            .await
            .session_present
    );

    channel
        .publish_track(
            &SessionId::Integer(1),
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
            SessionOutbound::Request(request) => Some(*request),
            SessionOutbound::Message(_)
            | SessionOutbound::TrackBindingUpdate(_)
            | SessionOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let ChannelEventRequest::BootstrapRemoteTrack(payload) = request;
    let codecs = payload.rtp_parameters().codecs().collect::<Vec<_>>();
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].codec_name(), "VP8");
    assert_eq!(codecs[0].payload_type(), 96);
}

#[tokio::test]
async fn session_replacement_purges_stale_published_media_state() {
    let (channel, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_sessions().await;

    let producer_id = channel
        .publish_track(
            &SessionId::Integer(1),
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
            .any(|message| matches!(message, SessionOutbound::Request(_)))
    );
    let published_transport_media_id = channel.first_published_transport_media_id().await;
    assert!(published_transport_media_id.is_some());

    assert_eq!(channel.producer_count().await, 1);
    assert_eq!(channel.consumer_count().await, 1);
    assert!(
        channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Camera)
            .await
    );

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(channel.producer_count().await, 0);
    assert_eq!(channel.consumer_count().await, 0);
    assert!(
        channel
            .producer_stream_type_for_transport_media_id(
                published_transport_media_id.expect("published track should have a transport id")
            )
            .await
            .is_none()
    );
    assert!(
        !channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Camera)
            .await
    );
}

#[tokio::test]
async fn session_replacement_purges_all_published_stream_mappings() {
    let (channel, adapter, mut publisher_rx, mut subscriber_rx) = setup_two_ready_sessions().await;

    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await
            .is_some()
    );
    assert!(
        channel
            .publish_track(
                &SessionId::Integer(1),
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
            .filter(|message| matches!(message, SessionOutbound::Request(_)))
            .count(),
        2,
        "subscriber should receive one bootstrap per published stream"
    );

    let camera_transport_media_id = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Camera)
        .await;
    let audio_transport_media_id = channel
        .producer_transport_media_id(&SessionId::Integer(1), 0, StreamType::Audio)
        .await;
    assert!(camera_transport_media_id.is_some());
    assert!(audio_transport_media_id.is_some());

    let (replacement_tx, _replacement_rx) = test_sender();
    assert!(
        channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                replacement_tx,
            )
            .await
            .is_ok()
    );

    assert_eq!(channel.producer_count().await, 0);
    assert_eq!(channel.consumer_count().await, 0);
    assert!(
        channel
            .producer_stream_type_for_transport_media_id(
                camera_transport_media_id.expect("camera producer should expose a transport id")
            )
            .await
            .is_none()
    );
    assert!(
        channel
            .producer_stream_type_for_transport_media_id(
                audio_transport_media_id.expect("audio producer should expose a transport id")
            )
            .await
            .is_none()
    );
    assert!(
        !channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Camera)
            .await
    );
    assert!(
        !channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Audio)
            .await
    );
}

#[tokio::test]
async fn publish_track_releases_channel_lock_while_waiting_on_transport_adapter() {
    let (channel, _adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;
    let (fake_transport_adapter, _) = fake_adapter();
    let RuntimeTransportAdapter::Fake(fake) = &fake_transport_adapter else {
        panic!("expected fake transport adapter");
    };
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = fake_transport_adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
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
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        channel
            .update_session_info(
                &SessionId::Integer(2),
                SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "session info update should not wait for publish transport declaration"
    );

    assert!(publish_task.await.unwrap().is_some());
    assert!(
        drain_outbound(&mut rx1).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut rx2).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(_))
        )),
        "peer should still receive the concurrent info broadcast"
    );
}

#[tokio::test]
async fn publish_track_defers_producer_commit_until_transport_publish_succeeds() {
    let (channel, adapter, fake, _rx1, _rx2) = setup_two_ready_sessions_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
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
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(channel.producer_count().await, 0);

    assert!(publish_task.await.unwrap().is_some());

    assert_eq!(channel.producer_count().await, 1);
    let transport_media_id = channel.first_published_transport_media_id().await;
    assert!(transport_media_id.is_some());
    assert_eq!(
        channel
            .producer_stream_type_for_transport_media_id(
                transport_media_id.expect("published track should have a transport id")
            )
            .await,
        Some(StreamType::Camera)
    );
    assert!(
        channel
            .has_producer_route_target(&SessionId::Integer(1), 0, StreamType::Camera)
            .await
    );
}

#[tokio::test]
async fn publish_track_cleans_up_transport_media_when_session_leaves_mid_publish() {
    let (channel, adapter, fake, _rx1, _rx2) = setup_two_ready_sessions_with_fake().await;
    fake.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = adapter.clone();
        async move {
            channel
                .publish_track(
                    &SessionId::Integer(1),
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
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(1), 0).await);
    assert!(publish_task.await.unwrap().is_none());

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                session_id: SessionId::Integer(1),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_updates_screen_sharing_info() {
    let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
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
    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Screen, false, &adapter)
        .await;

    let msgs = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(snapshot)) = &msgs[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_screen_sharing_on, Some(false));
    } else {
        panic!("expected SessionInfoChanged for screen sharing");
    }
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (channel, adapter, fake, mut rx1, mut rx2) = setup_two_ready_sessions_with_fake().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
            StreamType::Camera,
            MediaKind::Video,
            test_video_rtp_parameters(),
            &adapter,
        )
        .await;
    drain_outbound(&mut rx1);
    drain_outbound(&mut rx2);

    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ProducerActivityUpdated {
                session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_commits_session_state_before_transport_update_finishes() {
    let (channel, adapter, fake, mut rx1, mut rx2) = setup_two_ready_sessions_with_fake().await;

    channel
        .publish_track(
            &SessionId::Integer(1),
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
        let channel = Arc::clone(&channel);
        let adapter = adapter.clone();
        async move {
            channel
                .set_publication_active(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
                .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ProducerActivityUpdated {
                session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;

    let Some((_, info)) = channel.session_info_snapshot(&SessionId::Integer(1)).await else {
        panic!("publisher session should still be present");
    };
    assert_eq!(info.is_camera_on, Some(false));

    update_task.await.unwrap();
}

#[tokio::test]
async fn late_join_bootstrap_releases_channel_lock_while_waiting_on_transport_adapter() {
    let (channel, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_consume_transport_ready(&SessionId::Integer(2))
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    let update_result = timeout(Duration::from_millis(50), async {
        channel
            .update_session_info(
                &SessionId::Integer(1),
                SessionInfo {
                    is_talking: Some(true),
                    ..SessionInfo::default()
                },
                false,
            )
            .await;
    })
    .await;
    assert!(
        update_result.is_ok(),
        "session info update should not wait for late-join consumer declaration"
    );

    bootstrap_task.await.unwrap();
    assert!(
        drain_outbound(&mut publisher_rx).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|msg| matches!(
                msg,
                SessionOutbound::Message(ChannelEventMessage::SessionInfoChanged(_))
                    | SessionOutbound::Request(_)
            )),
        "late joiner should receive outbound traffic while bootstrap is running"
    );
}

#[tokio::test]
async fn late_join_bootstrap_defers_consumer_commit_until_transport_consume_succeeds() {
    let (channel, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_consume_transport_ready(&SessionId::Integer(2))
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert_eq!(channel.consumer_count().await, 0);

    bootstrap_task.await.unwrap();

    assert_eq!(channel.consumer_count().await, 1);
}

#[tokio::test]
async fn late_join_bootstrap_cleans_up_transport_media_when_session_leaves_mid_consume() {
    let (channel, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_consume_transport_ready(&SessionId::Integer(2))
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    fake.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(2), 1).await);
    bootstrap_task.await.unwrap();

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::MediaRemoved {
                session_id: SessionId::Integer(2),
                ..
            }
        )
    })
    .await;
}

#[tokio::test]
async fn production_change_ignores_unknown_stream_type() {
    let (channel, adapter, mut rx1, mut _rx2) = setup_two_ready_sessions().await;

    // No producer published for audio. PRODUCTION_CHANGE should be a no-op.
    channel
        .set_publication_active(&SessionId::Integer(1), StreamType::Audio, false, &adapter)
        .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn client_capabilities_bootstrap_late_join_when_download_connected_first() {
    let (channel, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let download_update = channel
        .set_consume_transport_ready(&SessionId::Integer(2))
        .await;
    assert!(download_update.session_present);
    assert!(!download_update.became_consumer_ready);

    assert!(
        channel
            .apply_client_rtp_capabilities(
                &SessionId::Integer(2),
                channel
                    .session_connection_id(&SessionId::Integer(2))
                    .await
                    .unwrap_or(u64::MAX),
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(2))
            .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, SessionOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after capabilities make it ready"
    );
}

#[tokio::test]
async fn transport_connect_bootstrap_late_join_when_capabilities_arrive_first() {
    let (channel, transport_adapter, fake, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let capabilities_update = channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    assert!(capabilities_update.session_present);
    assert!(!capabilities_update.became_consumer_ready);
    assert!(
        channel
            .session_has_parsed_client_rtp_capabilities(&SessionId::Integer(2))
            .await
    );

    assert!(
        channel
            .apply_consume_transport_ready(
                &SessionId::Integer(2),
                channel
                    .session_connection_id(&SessionId::Integer(2))
                    .await
                    .unwrap_or(u64::MAX),
                &transport_adapter,
            )
            .await
    );

    wait_for_fake_event(&fake, |event| {
        matches!(
            event,
            FakeWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, SessionOutbound::Request(_))),
        "subscriber should receive a consumer bootstrap after download connect makes it ready"
    );
}

#[tokio::test]
async fn refresh_retry_bootstraps_only_missing_consumers_on_real_rtc() {
    let mut scenario = setup_real_rtc_refresh_scenario().await;

    assert!(
        scenario
            .channel
            .publish_track(
                &scenario.publisher_session_id,
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
    assert_eq!(scenario.channel.consumer_count().await, 1);

    let first_refresh_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("first subscriber refresh should stage an rtc offer");

    assert!(
        scenario
            .channel
            .publish_track(
                &scenario.publisher_session_id,
                StreamType::Screen,
                MediaKind::Video,
                video_rtp_parameters_with_mid("screen-refresh-retry", 33_333),
                &scenario.transport_adapter,
            )
            .await
            .is_some()
    );
    assert_eq!(
        scenario.channel.consumer_count().await,
        1,
        "second consumer must stay pending while the first rtc offer awaits an answer"
    );
    assert!(
        drain_outbound(&mut scenario.subscriber_rx).is_empty(),
        "no second bootstrap should be emitted before the first refresh answer lands"
    );

    apply_offer_answer(
        &scenario.transport_adapter,
        &scenario.subscriber_session_key,
        &mut scenario.subscriber_remote,
        first_refresh_offer.into_sdp(),
    )
    .await;

    scenario
        .channel
        .bootstrap_missing_consumers(&scenario.subscriber_session_id, &scenario.transport_adapter)
        .await;

    assert_eq!(scenario.channel.consumer_count().await, 2);
    assert_bootstrap_for_stream(
        &drain_outbound(&mut scenario.subscriber_rx),
        StreamType::Screen,
    );

    let second_refresh_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&scenario.subscriber_session_key)
        .await
        .expect("retry should stage the deferred rtc offer");
    apply_offer_answer(
        &scenario.transport_adapter,
        &scenario.subscriber_session_key,
        &mut scenario.subscriber_remote,
        second_refresh_offer.into_sdp(),
    )
    .await;

    scenario
        .channel
        .bootstrap_missing_consumers(&scenario.subscriber_session_id, &scenario.transport_adapter)
        .await;

    assert_eq!(
        scenario.channel.consumer_count().await,
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
        .channel
        .session_connection_id(&scenario.publisher_session_id)
        .await
    else {
        panic!("publisher connection should exist");
    };
    let publisher_session_key = scenario
        .channel
        .transport_session_key(&scenario.publisher_session_id, publisher_connection_id);
    let mut publisher_remote = build_remote_rtc(55_101);
    let initial_offer = scenario
        .transport_adapter
        .create_initial_session_offer(&publisher_session_key)
        .await
        .expect("publisher should get an initial rtc offer");
    apply_offer_answer(
        &scenario.transport_adapter,
        &publisher_session_key,
        &mut publisher_remote,
        initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = scenario
        .transport_adapter
        .publish_media(
            &publisher_session_key,
            MediaKind::Video,
            &o_sfu_router::RtpParameters::new(vec![], vec![], vec![]),
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
            .channel
            .publish_negotiated_track(
                &scenario.publisher_session_id,
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
    assert_eq!(scenario.channel.consumer_count().await, 1);
}

struct RealRtcRefreshScenario {
    channel: Arc<Channel>,
    transport_adapter: RuntimeTransportAdapter,
    publisher_session_id: SessionId,
    subscriber_session_id: SessionId,
    subscriber_session_key: TransportSessionKey,
    publisher_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    subscriber_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    subscriber_remote: Rtc,
}

async fn setup_real_rtc_refresh_scenario() -> RealRtcRefreshScenario {
    let manager = ChannelManager::for_test();
    let channel = manager
        .create_or_get("issuer-a", None, &ChannelConfig::default(), None)
        .await;
    let (publisher_tx, publisher_rx) = test_sender();
    let (subscriber_tx, subscriber_rx) = test_sender();
    let publisher_session_id = SessionId::Integer(1);
    let subscriber_session_id = SessionId::Integer(2);
    let publisher_connection_id = channel
        .join_session(
            publisher_session_id.clone(),
            None,
            SessionPermissions::default(),
            publisher_tx,
        )
        .await
        .expect("publisher should join");
    let subscriber_connection_id = channel
        .join_session(
            subscriber_session_id.clone(),
            None,
            SessionPermissions::default(),
            subscriber_tx,
        )
        .await
        .expect("subscriber should join");
    let transport_adapter = build_real_rtc_transport_adapter();
    let publisher_session_key =
        channel.transport_session_key(&publisher_session_id, publisher_connection_id);
    let subscriber_session_key =
        channel.transport_session_key(&subscriber_session_id, subscriber_connection_id);

    bootstrap_real_rtc_session(&transport_adapter, &publisher_session_key).await;
    bootstrap_real_rtc_session(&transport_adapter, &subscriber_session_key).await;
    let mut subscriber_remote = build_remote_rtc(55_100);
    let initial_offer = transport_adapter
        .create_initial_session_offer(&subscriber_session_key)
        .await
        .expect("subscriber should get an initial rtc offer");
    apply_offer_answer(
        &transport_adapter,
        &subscriber_session_key,
        &mut subscriber_remote,
        initial_offer.into_sdp(),
    )
    .await;

    assert!(
        channel
            .apply_session_negotiated(
                &publisher_session_id,
                publisher_connection_id,
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );
    assert!(
        channel
            .apply_session_negotiated(
                &subscriber_session_id,
                subscriber_connection_id,
                test_client_rtp_capabilities(),
                &transport_adapter,
            )
            .await
    );

    RealRtcRefreshScenario {
        channel,
        transport_adapter,
        publisher_session_id,
        subscriber_session_id,
        subscriber_session_key,
        publisher_rx,
        subscriber_rx,
        subscriber_remote,
    }
}

fn build_real_rtc_transport_adapter() -> RuntimeTransportAdapter {
    RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        RtcPortRange::new(46_200, 46_299),
        1,
        MediaCodecFlags::default(),
        Arc::new(MediaTap::default()),
        Arc::new(RuntimeMetrics::default()),
    ))
}

async fn bootstrap_real_rtc_session(
    transport_adapter: &RuntimeTransportAdapter,
    session_key: &TransportSessionKey,
) {
    assert!(
        transport_adapter
            .transport_bootstrap_payload(
                session_key,
                &o_sfu_router::RtpCapabilities::new(vec![], vec![])
            )
            .await
            .is_ok()
    );
}

fn assert_bootstrap_for_stream(messages: &[SessionOutbound], stream_type: StreamType) {
    assert!(
        messages.iter().any(|message| matches!(
            message,
            SessionOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    ChannelEventRequest::BootstrapRemoteTrack(payload)
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
    assert_eq!(
        adapter
            .apply_session_answer(session_key, &answer.to_sdp_string())
            .await,
        Ok(())
    );
}

fn video_rtp_parameters_with_mid(mid: &str, ssrc: u32) -> RtpParameters {
    router_sample_video_rtp_parameters(Some(mid), ssrc)
}
