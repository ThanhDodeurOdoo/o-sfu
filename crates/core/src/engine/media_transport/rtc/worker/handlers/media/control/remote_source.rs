//! Remote-source relay control for worker-local route state.

use super::routes::ensure_owned_local_producer_mid;
use crate::engine::media_transport::{
    TransportAdapterError, TransportSourceKey,
    rtc::{
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        state::PacketLoopState,
    },
};

pub(super) fn set_remote_source_packet_gate(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let source_transport_media_id = source.transport_media_id();
    if ensure_owned_local_producer_mid(state, source.session_key(), source_transport_media_id)
        .is_err()
    {
        return;
    }
    state
        .routes
        .set_relay_packet_gate(source_transport_media_id, target_id, packet_gate);
}

pub(super) fn worker_add_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    target: RelayPacketMailbox,
) -> Result<(), TransportAdapterError> {
    let source_transport_media_id = source.transport_media_id();
    ensure_owned_local_producer_mid(state, source.session_key(), source_transport_media_id)?;
    state
        .routes
        .add_relay_target(source_transport_media_id, target_id, target);
    Ok(())
}

pub(super) fn worker_set_relay_target_active(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let source_transport_media_id = source.transport_media_id();
    if active {
        ensure_owned_local_producer_mid(state, source.session_key(), source_transport_media_id)?;
    }
    state
        .routes
        .set_relay_target_active(source_transport_media_id, target_id, active);
    Ok(())
}
