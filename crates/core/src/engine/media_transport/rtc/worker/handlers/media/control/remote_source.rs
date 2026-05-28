//! Remote-source relay control for worker-local route state.

use super::routes::ensure_owned_local_producer_mid;
use crate::engine::media_transport::{
    TransportAdapterError, TransportMediaId, TransportSourceKey,
    rtc::{
        demux::MediaRouteEntry,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
        state::PacketLoopState,
    },
};

pub(super) fn remove_relay_target(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
) {
    state.remove_relay_target(source_transport_media_id, target_id);
}

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
        .route_control
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
    state.add_relay_target(source_transport_media_id, target_id, target);
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
    state.set_relay_target_active(source_transport_media_id, target_id, active);
    Ok(())
}

/// Derive the gate sent to the producer worker for a remote source route.
///
/// RID and operating-point gates are enforced on the consumer worker. The
/// producer worker must keep the relay open for those routes because consumer
/// workers need to observe non-selected RID packets for bootstrap fallback and
/// stale-layer recovery. A local `Block` is forwarded only when it is a real
/// block, not when it is the temporary state used while a selected RID is still
/// pending.
pub(super) fn remote_source_packet_gate_for_route(
    route_entry: Option<&MediaRouteEntry>,
    local_packet_gate: Option<PacketLayerGate>,
) -> PacketLayerGate {
    match (route_entry, local_packet_gate) {
        (
            Some(_),
            Some(
                PacketLayerGate::Open
                | PacketLayerGate::Rid(_)
                | PacketLayerGate::OperatingPoint(_),
            ),
        ) => PacketLayerGate::Open,
        (Some(route_entry), Some(PacketLayerGate::Block))
            if route_entry
                .destinations
                .iter()
                .any(|destination| destination.pending_packet_gate.is_some()) =>
        {
            PacketLayerGate::Open
        }
        (_route_entry, Some(packet_gate)) => packet_gate,
        (None | Some(_), None) => PacketLayerGate::Block,
    }
}
