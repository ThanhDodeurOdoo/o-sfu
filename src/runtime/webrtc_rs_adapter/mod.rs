use super::{
    stub_bus::StubWebRtcAdapter,
    transport_adapter::{TransportAdapter, TransportAdapterError, TransportConnectDirection},
};
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload, shared::SessionId, webrtc::DtlsParameters,
};

#[allow(
    dead_code,
    reason = "Phase-7 SDP parsing scaffolding is prepared before transport wiring starts using it."
)]
mod sdp;

/// Placeholder transport adapter for the selected phase-7 backend (`webrtc-rs`).
///
/// During the library-selection phase this delegates to the deterministic stub
/// transport behavior so signaling and channel lifecycle flows remain stable
/// while SDP/ICE/DTLS integration is added incrementally.
#[derive(Debug, Default)]
pub(super) struct WebRtcRsTransportAdapter {
    fallback: StubWebRtcAdapter,
}

impl TransportAdapter for WebRtcRsTransportAdapter {
    fn transport_bootstrap_payload(
        &self,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        self.fallback
            .transport_bootstrap_payload(router_capabilities)
    }

    fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
    ) -> Result<(), TransportAdapterError> {
        self.fallback
            .connect_transport(session_id, direction, dtls_parameters)
    }
}
