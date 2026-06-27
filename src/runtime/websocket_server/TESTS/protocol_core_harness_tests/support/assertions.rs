use super::*;

pub(crate) async fn consume_peer_joined_update(
    peer: &mut ProtocolHarnessPeer,
    user_id: ProtocolSessionId,
) -> Option<()> {
    consume_session_info_update(
        peer,
        BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&user_id),
            ProtocolSessionInfo::default().snapshot_complete(),
        )])),
        "peer join should project into the post-auth user-info update surface",
    )
    .await
}

pub(crate) async fn consume_peer_info_update(
    peer: &mut ProtocolHarnessPeer,
    user_id: ProtocolSessionId,
    info: ProtocolSessionInfo,
) -> Option<()> {
    consume_session_info_update(
        peer,
        BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&user_id),
            info,
        )])),
        "peer info should project into the post-auth user-info update surface",
    )
    .await
}

async fn consume_session_info_update(
    peer: &mut ProtocolHarnessPeer,
    expected: BundleUpdate,
    message: &str,
) -> Option<()> {
    let start = peer.updates.len();
    for _ in 0..4 {
        peer.read_server_frame().await?;
        if peer
            .updates
            .get(start..)
            .is_some_and(|updates| updates.contains(&expected))
        {
            return Some(());
        }
    }
    assert_eq!(peer.updates.last(), Some(&expected), "{message}");
    None
}

pub(crate) fn assert_track_snapshot_contains(
    track_bindings: &[TrackBinding],
    user_id: &ProtocolSessionId,
    stream_type: ProtocolStreamType,
) {
    assert!(
        track_bindings.iter().any(|binding| {
            binding.user_id == *user_id && binding.stream_type == stream_type && binding.active
        }),
        "expected an active track binding for user {user_id:?} and stream {stream_type:?}, got {track_bindings:?}"
    );
}

pub(crate) fn peer_reached_state(peer: &ProtocolHarnessPeer, state: BundleConnectionState) -> bool {
    peer.state_changes
        .iter()
        .any(|change| change.state == state && change.cause.is_none())
}
