use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationSnapshot {
    pub(crate) track: TrackBinding,
    pub(crate) source: SourceDescriptor,
}

pub(crate) fn assert_retained_publication(
    initial: &PublicationSnapshot,
    current: &PublicationSnapshot,
    active: bool,
) {
    let mut expected = initial.clone();
    expected.track.active = active;
    expected.source.active = active;
    assert_eq!(current, &expected);
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
    assert_publication_snapshot(subscriber, user_id, stream_type, active)
        .await
        .track
}

pub(crate) async fn assert_publication_snapshot(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> PublicationSnapshot {
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
    let source = assert_source_snapshot(subscriber, track_binding).await;
    PublicationSnapshot {
        track: track_binding.clone(),
        source,
    }
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
    track_binding: &TrackBinding,
) -> SourceDescriptor {
    let ServerMessage::Sources(sources) =
        next_server_message(subscriber, "protocol source snapshot").await
    else {
        panic!("expected protocol source snapshot");
    };
    let [source] = sources.as_slice() else {
        panic!("expected one protocol source descriptor");
    };
    assert_eq!(source.user_id, track_binding.user_id);
    assert_eq!(source.stream_type, track_binding.stream_type);
    assert_eq!(source.active, track_binding.active);
    assert_eq!(source.mid.as_deref(), Some(track_binding.mid.as_str()));
    assert!(!source.source_id.is_empty());
    source.clone()
}
