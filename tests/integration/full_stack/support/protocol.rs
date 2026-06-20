use super::*;

pub(crate) async fn publish_camera_track(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) -> Option<()> {
    let source = FakeMediaSource::camera();
    publisher.publish_track(&source).await?;
    publisher.complete_next_negotiation().await?;
    assert_track_snapshot(subscriber, UserId::Integer(10), StreamType::Camera, true).await;
    assert_peer_info_update(
        subscriber,
        UserId::Integer(10),
        UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::snapshot_defaults()
        },
    )
    .await;
    Some(())
}

pub(crate) async fn assert_consumer_download_toggle_round_trip_protocol(
    subscriber: &mut ProtocolFakePeer,
) {
    for camera in [Some(false), Some(true)] {
        assert!(
            subscriber
                .update_subscription(
                    UserId::Integer(10),
                    super::DownloadStates {
                        camera,
                        ..super::DownloadStates::default()
                    },
                )
                .await
                .is_some()
        );
    }
}

pub(crate) async fn assert_camera_unpublish_updates_snapshot_and_info(
    publisher: &mut ProtocolFakePeer,
    subscriber: &mut ProtocolFakePeer,
) {
    assert!(
        publisher
            .set_publication_active(StreamType::Camera, false)
            .await
            .is_some()
    );
    assert!(publisher.complete_next_negotiation().await.is_some());
    let messages = [
        next_server_message(subscriber, "first camera unpublish update").await,
        next_server_message(subscriber, "second camera unpublish update").await,
        next_server_message(subscriber, "third camera unpublish update").await,
    ];
    let Some(track_snapshot) = messages.iter().find_map(|message| match message {
        ServerMessage::Tracks(snapshot) => Some(snapshot),
        _ => None,
    }) else {
        panic!("expected track snapshot after camera unpublish");
    };
    assert!(
        track_snapshot.is_empty(),
        "protocol unpublish should clear the authoritative camera track snapshot"
    );

    assert!(
        messages.iter().any(
            |message| matches!(message, ServerMessage::Sources(snapshot) if snapshot.is_empty())
        ),
        "protocol unpublish should clear the authoritative camera source snapshot"
    );

    let Some(peer_info) = messages.iter().find_map(|message| match message {
        ServerMessage::PeerInfo(info) => Some(info),
        _ => None,
    }) else {
        panic!("expected peer info update after camera unpublish");
    };
    assert_eq!(peer_info.user_id, UserId::Integer(10));
    assert_eq!(
        peer_info.info,
        UserInfo {
            is_camera_on: Some(false),
            ..UserInfo::snapshot_defaults()
        }
    );
}

pub(crate) async fn connect_late_subscriber(
    server: &TestServer,
    room: &str,
) -> Option<ProtocolFakePeer> {
    super::connect_fake_peer(server, room, UserId::Integer(30), TEST_ROOM_KEY).await
}

pub(crate) async fn assert_late_join_has_no_track_snapshot(late_subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            late_subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

pub(crate) async fn assert_departure_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
) {
    for _ in 0..3 {
        match next_server_message(subscriber, "protocol peer departure notification").await {
            ServerMessage::PeerLeft(departure) => {
                assert_eq!(departure.user_id, user_id);
                return;
            }
            ServerMessage::Sources(snapshot) if snapshot.is_empty() => {}
            ServerMessage::Tracks(snapshot) if snapshot.is_empty() => {}
            message => panic!("expected protocol peer departure notification, got {message:?}"),
        }
    }
    panic!("expected protocol peer departure notification");
}

pub(crate) async fn assert_peer_joined_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
) {
    let ServerMessage::PeerJoined(joined) =
        next_server_message(subscriber, "protocol peer joined notification").await
    else {
        panic!("expected protocol peer joined notification");
    };
    assert_eq!(joined.user_id, user_id);
}

pub(crate) async fn assert_track_snapshot(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> TrackBinding {
    let ServerMessage::Tracks(track_bindings) =
        next_server_message(subscriber, "protocol track snapshot").await
    else {
        panic!("expected protocol track snapshot");
    };
    let [track_binding] = track_bindings.as_slice() else {
        panic!("expected one protocol track binding");
    };
    assert_eq!(track_binding.user_id, user_id);
    assert_eq!(track_binding.stream_type, stream_type);
    assert_eq!(track_binding.active, active);
    assert_source_snapshot(subscriber, Some(track_binding)).await;
    track_binding.clone()
}

pub(crate) async fn assert_empty_track_snapshot(subscriber: &mut ProtocolFakePeer) {
    let ServerMessage::Tracks(track_bindings) =
        next_server_message(subscriber, "empty protocol track snapshot").await
    else {
        panic!("expected protocol track snapshot");
    };
    assert!(track_bindings.is_empty());
    assert_source_snapshot(subscriber, None).await;
}

pub(crate) async fn assert_peer_info_update(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    expected_info: UserInfo,
) {
    let ServerMessage::PeerInfo(peer_info) =
        next_server_message(subscriber, "protocol peer info update").await
    else {
        panic!("expected protocol peer info update");
    };
    assert_eq!(peer_info.user_id, user_id);
    assert_eq!(peer_info.info, expected_info);
}

pub(crate) async fn assert_no_server_message_protocol(subscriber: &mut ProtocolFakePeer) {
    assert!(
        timeout(
            Duration::from_millis(200),
            subscriber.read_next_server_message()
        )
        .await
        .is_err()
    );
}

async fn next_server_message(subscriber: &mut ProtocolFakePeer, expected: &str) -> ServerMessage {
    let Some(message) = subscriber
        .read_server_message_with_timeout(Duration::from_secs(1))
        .await
    else {
        panic!("expected {expected}");
    };
    message
}

async fn assert_source_snapshot(
    subscriber: &mut ProtocolFakePeer,
    track_binding: Option<&TrackBinding>,
) {
    let ServerMessage::Sources(sources) =
        next_server_message(subscriber, "protocol source snapshot").await
    else {
        panic!("expected protocol source snapshot");
    };
    let Some(track_binding) = track_binding else {
        assert!(sources.is_empty());
        return;
    };
    let [source] = sources.as_slice() else {
        panic!("expected one protocol source descriptor");
    };
    assert_eq!(source.user_id, track_binding.user_id);
    assert_eq!(source.stream_type, track_binding.stream_type);
    assert_eq!(source.active, track_binding.active);
    assert!(source.mid.as_deref().is_some_and(|mid| !mid.is_empty()));
    assert!(!source.source_id.is_empty());
}
