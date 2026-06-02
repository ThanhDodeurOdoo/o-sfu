//! Remote-source relay control for worker-local route state.

use super::routes::ensure_local_producer_mid;
use crate::engine::media_transport::{
    TransportAdapterError, TransportSourceKey,
    rtc::{
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        state::PacketLoopState,
    },
};

pub(super) fn set_remote_src_pkt_gate(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let src_media = source.transport_media_id();
    if ensure_local_producer_mid(state, source.session_key(), src_media).is_err() {
        return;
    }
    state
        .routes
        .set_relay_pkt_gate(src_media, target_id, packet_gate);
}

pub(super) fn worker_add_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    target: RelayPacketMailbox,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    ensure_local_producer_mid(state, source.session_key(), src_media)?;
    state.routes.add_relay_target(src_media, target_id, target);
    Ok(())
}

pub(super) fn worker_set_relay_target_active(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    if active {
        ensure_local_producer_mid(state, source.session_key(), src_media)?;
    }
    state
        .routes
        .set_relay_target_active(src_media, target_id, active);
    Ok(())
}
