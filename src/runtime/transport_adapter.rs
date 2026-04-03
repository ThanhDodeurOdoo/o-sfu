use std::fmt::Debug;

use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload, shared::SessionId, webrtc::DtlsParameters,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportConnectDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportAdapterError {
    TransportUnavailable,
    InvalidInput,
    UnsupportedFeature,
}

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
///
/// Implementations provide transport bootstrap payloads and transport connection handling
/// without leaking concrete WebRTC library details into the signaling flow.
pub(crate) trait TransportAdapter: Debug + Send + Sync {
    /// Build the `INIT_TRANSPORTS` payload for a newly authenticated session.
    fn transport_bootstrap_payload(
        &self,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError>;

    /// Connect one direction transport with client DTLS parameters.
    fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
    ) -> Result<(), TransportAdapterError>;
}
