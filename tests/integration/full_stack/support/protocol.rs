use super::*;

pub(crate) fn assert_retained_publication(
    initial: &TrackBinding,
    current: &TrackBinding,
    active: bool,
) {
    let mut expected = initial.clone();
    expected.active = active;
    assert_eq!(current, &expected);
}

pub(crate) async fn assert_departure_message_protocol(
    subscriber: &mut ProtocolFakePeer,
    user_id: UserId,
) {
    for _ in 0..2 {
        match next_server_message(subscriber, "protocol peer departure notification").await {
            ServerMessage::PeerLeft(departure) => {
                assert_eq!(departure.user_id, user_id);
                return;
            }
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
    track_binding.clone()
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
