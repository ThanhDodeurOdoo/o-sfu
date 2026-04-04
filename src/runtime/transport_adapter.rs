use std::{fmt::Debug, net::IpAddr, sync::Arc};

use super::{rtc_adapter::RtcTransportAdapter, stub_bus::StubWebRtcAdapter};

use crate::config::RtcPortRange;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload, shared::SessionId, webrtc::DtlsParameters,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    Stub(Arc<StubWebRtcAdapter>),
    Rtc(Arc<RtcTransportAdapter>),
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn stub() -> Self {
        Self::Stub(Arc::new(StubWebRtcAdapter::default()))
    }

    #[must_use]
    pub(crate) fn rtc(public_ip: IpAddr, rtc_port_range: RtcPortRange) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapter::new(
            public_ip,
            rtc_port_range,
        )))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_stub_adapter(adapter: Arc<StubWebRtcAdapter>) -> Self {
        Self::Stub(adapter)
    }

    /// Build the `INIT_TRANSPORTS` payload for a newly authenticated session.
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_id: &SessionId,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_id, router_capabilities)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_id, router_capabilities)
                    .await
            }
        }
    }

    /// Connect one direction transport with client DTLS parameters.
    pub(crate) async fn connect_transport(
        &self,
        session_id: &SessionId,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .connect_transport(session_id, direction, dtls_parameters, sdp_offer)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .connect_transport(session_id, direction, dtls_parameters, sdp_offer)
                    .await
            }
        }
    }

    /// Release transport-adapter state for a disconnected session.
    pub(crate) async fn close_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.close_session(session_id).await,
            Self::Rtc(adapter) => adapter.close_session(session_id).await,
        }
    }
}
