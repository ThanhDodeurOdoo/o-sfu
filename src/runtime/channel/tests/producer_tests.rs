use super::fixtures::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::channel::{Channel, NegotiatedPublish};
use crate::runtime::recording::MediaTap;
use crate::runtime::transport_adapter::{RtcTransportAdapterShardSetConfig, TransportSessionKey};
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
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    // Both sessions should receive a session info broadcast with isCameraOn = false.
    let msgs1 = drain_outbound(&mut rx1);
    let msgs2 = drain_outbound(&mut rx2);
    assert_eq!(msgs1.len(), 1, "session 1 should get info broadcast");
    assert_eq!(msgs2.len(), 1, "session 2 should get info broadcast");

    // Verify the broadcast contains isCameraOn = false.
    let info_msg = &msgs1[0];
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = info_msg {
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
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, true, &adapter)
        .await;

    let msgs1 = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = &msgs1[0]
    {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_camera_on, Some(true));
    } else {
        panic!("expected SessionInfoChanged after resume");
    }
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
            SessionOutbound::Message(_) | SessionOutbound::Close(_) => None,
        })
        .expect("subscriber should receive INIT_CONSUMER");
    let CurrentServerRequest::BootstrapRemoteTrack(payload) = request else {
        panic!("expected INIT_CONSUMER request");
    };
    let codecs = payload
        .rtp_parameters
        .0
        .get("codecs")
        .and_then(serde_json::Value::as_array)
        .expect("consumer bootstrap should include codecs");
    assert_eq!(codecs.len(), 1);
    assert_eq!(codecs[0].get("mimeType"), Some(&json!("video/VP8")));
    assert_eq!(codecs[0].get("payloadType"), Some(&json!(96)));
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
    let (stub_adapter, _) = stub_adapter();
    let RuntimeTransportAdapter::Stub(stub) = &stub_adapter else {
        panic!("expected stub transport adapter");
    };
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

    let publish_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = stub_adapter.clone();
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

    wait_for_stub_event(stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
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
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut rx2).iter().any(|msg| matches!(
            msg,
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "peer should still receive the concurrent info broadcast"
    );
}

#[tokio::test]
async fn publish_track_defers_producer_commit_until_transport_publish_succeeds() {
    let (channel, adapter, stub, _rx1, _rx2) = setup_two_ready_sessions_with_stub().await;
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

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

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
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
    let (channel, adapter, stub, _rx1, _rx2) = setup_two_ready_sessions_with_stub().await;
    stub.set_publish_media_delay(Some(Duration::from_millis(200)));

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

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::PublishMediaRequested {
                session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(1), 0).await);
    assert!(publish_task.await.unwrap().is_none());

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved {
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
        .update_upload_state(&SessionId::Integer(1), StreamType::Screen, false, &adapter)
        .await;

    let msgs = drain_outbound(&mut rx1);
    if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) = &msgs[0] {
        let info = snapshot.values().next().unwrap();
        assert_eq!(info.is_screen_sharing_on, Some(false));
    } else {
        panic!("expected SessionInfoChanged for screen sharing");
    }
}

#[tokio::test]
async fn production_change_updates_transport_route_activity() {
    let (channel, adapter, stub, mut rx1, mut rx2) = setup_two_ready_sessions_with_stub().await;

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
        .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false, &adapter)
        .await;

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ProducerActivityUpdated {
                session_id: SessionId::Integer(1),
                active: false,
            }
        )
    })
    .await;
}

#[tokio::test]
async fn late_join_bootstrap_releases_channel_lock_while_waiting_on_transport_adapter() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
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
            SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
        )),
        "publisher should still receive the concurrent info broadcast"
    );
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|msg| matches!(
                msg,
                SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
                    | SessionOutbound::Request(_)
            )),
        "late joiner should receive outbound traffic while bootstrap is running"
    );
}

#[tokio::test]
async fn late_join_bootstrap_defers_consumer_commit_until_transport_consume_succeeds() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
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
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
        .await;
    channel
        .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
        .await;
    stub.set_consume_media_delay(Some(Duration::from_millis(200)));

    let bootstrap_task = tokio::spawn({
        let channel = Arc::clone(&channel);
        let adapter = transport_adapter.clone();
        async move {
            channel
                .bootstrap_missing_consumers(&SessionId::Integer(2), &adapter)
                .await;
        }
    });

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
                consumer_session_id: SessionId::Integer(2),
                source_session_id: SessionId::Integer(1),
                media_kind: MediaKind::Video,
            }
        )
    })
    .await;

    assert!(channel.leave_session(&SessionId::Integer(2), 1).await);
    bootstrap_task.await.unwrap();

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::MediaRemoved {
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
        .update_upload_state(&SessionId::Integer(1), StreamType::Audio, false, &adapter)
        .await;

    assert!(
        drain_outbound(&mut rx1).is_empty(),
        "no broadcast expected when no producer exists for the stream type"
    );
}

#[tokio::test]
async fn client_capabilities_bootstrap_late_join_when_download_connected_first() {
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
        setup_late_join_bootstrap_scenario().await;
    drain_outbound(&mut publisher_rx);
    drain_outbound(&mut subscriber_rx);

    let download_update = channel
        .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
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

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
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
    let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
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
            .apply_transport_connected(
                &SessionId::Integer(2),
                channel
                    .session_connection_id(&SessionId::Integer(2))
                    .await
                    .unwrap_or(u64::MAX),
                TransportConnectDirection::Download,
                &transport_adapter,
            )
            .await
    );

    wait_for_stub_event(&stub, |event| {
        matches!(
            event,
            StubWebRtcEvent::ConsumeMediaRequested {
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
        .expect("native publish intent should stage a recv-only media line");
    let publish_offer = scenario
        .transport_adapter
        .create_session_renegotiation_offer(&publisher_session_key)
        .await
        .expect("native publish should stage a follow-up offer");
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
        .expect("answered native publish should expose negotiated producer parameters");

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
    RuntimeTransportAdapter::builder()
        .rtc(RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(46_200, 46_299),
            1,
            MediaCodecFlags::default(),
            Arc::new(MediaTap::default()),
        ))
        .build()
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
                    CurrentServerRequest::BootstrapRemoteTrack(payload)
                        if payload.stream_type == stream_type
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
    RtpParameters(json!({
        "mid": mid,
        "codecs": [
            {
                "mimeType": "video/VP8",
                "payloadType": 96,
                "clockRate": 90000,
                "parameters": {},
                "rtcpFeedback": [
                    { "type": "nack" },
                    { "type": "nack", "parameter": "pli" },
                    { "type": "ccm", "parameter": "fir" },
                    { "type": "goog-remb" },
                    { "type": "transport-cc" }
                ]
            },
            {
                "mimeType": "video/rtx",
                "payloadType": 97,
                "clockRate": 90000,
                "parameters": { "apt": "96" },
                "rtcpFeedback": []
            }
        ],
        "headerExtensions": [
            { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
            { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
            { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
        ],
        "encodings": [{ "ssrc": ssrc }]
    }))
}
