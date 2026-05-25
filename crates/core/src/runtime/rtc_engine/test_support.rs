#[cfg(any(test, feature = "testing-transport"))]
#[path = "test_support/probe.rs"]
mod probe;
#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "test_support/route_graph.rs"]
mod route_graph;

#[cfg(test)]
use std::time::Instant;

#[cfg(any(test, feature = "testing-transport"))]
pub use probe::DebugRouteEntry;
#[cfg(test)]
pub(super) use probe::{
    ActiveRelayTargetCountProbe, HasAnyRemoteAddrSessionProbe, RecordIncomingMediaProbe,
    RelayTargetCountProbe, RememberRemoteAddrProbe, RemoteAddrOwnerProbe, ResolveMidProbe,
    SessionMaxBitrateInProbe, SessionMaxBitrateOutProbe, SessionStreamRxSsrcProbe,
    SessionStreamTxSsrcProbe,
};
#[cfg(test)]
pub use probe::{DebugPacketGate, DebugRouteDestination};
#[cfg(any(test, feature = "testing-transport"))]
pub(super) use probe::{
    DebugProbe, DebugProbeRequest, RouteEntryByConsumerMidProbe, RtcWorkerDebugChannels,
    RtcWorkerDebugHandle, SetSessionTransportHealthProbe, handle_debug_probe,
};
#[cfg(any(test, feature = "testing-transport"))]
pub(super) use probe::{ObserveAudioActivityProbe, RouteEntryByMediaIdProbe, RouteEntryProbe};
#[cfg(any(test, feature = "internal-benchmarks"))]
pub(in crate::runtime::rtc_engine) use route_graph::MediaWorkerScenario;

#[cfg(any(test, feature = "internal-benchmarks"))]
pub use super::forwarded_packet::test_support::sample_forwarded_packet;
#[cfg(any(test, feature = "internal-benchmarks"))]
pub(in crate::runtime::rtc_engine) use super::forwarded_packet::test_support::sample_forwarded_packet_without_mid;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::runtime::rtc_engine) use super::forwarded_packet::test_support::{
    reset_packet_resolution, sample_forwarded_packet_with_rid_and_audio_activity,
};
#[cfg(test)]
pub(in crate::runtime::rtc_engine) use super::forwarded_packet::test_support::{
    sample_already_relayed_packet, sample_forwarded_packet_with_audio_activity,
    sample_forwarded_packet_with_frame_mark, sample_forwarded_packet_with_rid,
    sample_local_forwarded_packet,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::runtime::{ConnectionId, RoomInstanceId, UserId, media_transport::TransportSessionKey};

#[cfg(test)]
pub(in crate::runtime::rtc_engine) fn collect_ready_session_keys(
    state: &mut super::state::PacketLoopState,
    now: Instant,
) -> Vec<TransportSessionKey> {
    let mut ready_handles = Vec::new();
    state.collect_ready_sessions(now, &mut ready_handles);
    ready_handles
        .into_iter()
        .filter_map(|session_handle| state.users.key_for_handle(session_handle).cloned())
        .collect()
}

#[cfg(any(test, feature = "internal-benchmarks"))]
#[must_use]
pub fn test_transport_session_key(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        media_worker_id,
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}
