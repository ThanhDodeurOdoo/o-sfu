use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::super::super::{
    relay_registry::RelayRegistry, route_control::PacketLayerGate, state::RtcBootstrapState,
};
use super::ownership::owned_local_producer_mid;

pub(crate) fn respond_set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    set_remote_source_route_active(
        state,
        relay_registry,
        source_session_key,
        source_transport_media_id,
        target_id,
        active,
    );
}

pub(crate) fn respond_set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    set_remote_source_packet_gate(
        state,
        source_session_key,
        source_transport_media_id,
        target_id,
        packet_gate,
    );
}

fn set_remote_source_route_active(
    state: &RtcBootstrapState,
    relay_registry: &RelayRegistry,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    active: bool,
) {
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_none() {
        return;
    }
    relay_registry.set_source_target_active(source_transport_media_id, target_id, active);
}

fn set_remote_source_packet_gate(
    state: &mut RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: super::super::super::relay_registry::RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    if owned_local_producer_mid(state, source_session_key, source_transport_media_id).is_none() {
        return;
    }
    state
        .route_control
        .set_relay_packet_gate(source_transport_media_id, target_id, packet_gate);
}
