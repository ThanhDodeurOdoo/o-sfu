use super::{super::route_control::PacketLayerGate, RtcBootstrapState};
use crate::runtime::media_transport::TransportMediaId;

impl RtcBootstrapState {
    pub(in crate::runtime::rtc_engine) fn set_local_packet_gate(
        &mut self,
        source_transport_media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.route_control
            .set_packet_gate(source_transport_media_id, packet_gate);
    }
}
