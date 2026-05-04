use super::*;

pub(crate) async fn consume_peer_joined_update(
    peer: &mut ProtocolHarnessPeer,
    user_id: ProtocolSessionId,
) -> Option<()> {
    peer.read_server_frame().await?;
    assert_eq!(
        peer.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&user_id),
            ProtocolSessionInfo::snapshot_defaults(),
        )]))),
        "peer join should project into the post-auth user-info update surface"
    );
    Some(())
}

pub(crate) async fn consume_peer_info_update(
    peer: &mut ProtocolHarnessPeer,
    user_id: ProtocolSessionId,
    info: ProtocolSessionInfo,
) -> Option<()> {
    peer.read_server_frame().await?;
    assert_eq!(
        peer.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&user_id),
            info,
        )]))),
        "peer info should project into the post-auth user-info update surface"
    );
    Some(())
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
        "expected an active track binding for user {user_id:?} and stream {stream_type:?}"
    );
}

pub(crate) fn peer_reached_state(peer: &ProtocolHarnessPeer, state: BundleConnectionState) -> bool {
    peer.state_changes
        .iter()
        .any(|change| change.state == state && change.cause.is_none())
}
