use crate::runtime::transport_adapter::{
    TransportConnectDirection, TransportMediaId, TransportSessionKey,
};

use super::super::route_control::PacketLayerGate;
use super::RtcBootstrapState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_adapter) enum TransportLifecycleState {
    BootstrapSent,
    Connected,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::runtime::rtc_adapter) struct TransportStateKey {
    pub(in crate::runtime::rtc_adapter) session_key: TransportSessionKey,
    pub(in crate::runtime::rtc_adapter) direction: TransportConnectDirection,
}

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
