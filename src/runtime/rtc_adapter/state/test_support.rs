use crate::runtime::transport_adapter::TransportMediaId;

use super::super::route_control::PacketLayerGate;
use super::RtcBootstrapState;

impl RtcBootstrapState {
    pub(in crate::runtime::rtc_adapter) fn set_source_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.route_control
            .set_packet_gate(source_transport_media_id, packet_gate);
    }
}
