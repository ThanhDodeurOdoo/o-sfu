use std::{sync::Arc, time::Instant};

#[cfg(test)]
use crate::runtime::transport_connect::{
    TransportConnectDtlsParameters, TransportConnectIceParameters,
};
use crate::signaling::shared::SessionId;

/// Channel-scoped transport-adapter session identity.
///
/// A `SessionId` alone is not unique across the server: the same id can appear
/// in different channels simultaneously. This composite key allows one session
/// to be uniquely identified by the owning channel runtime, media worker,
/// signaling connection, and session id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransportSessionKey {
    channel_runtime: u64,
    media_worker: usize,
    connection: u64,
    session: Arc<SessionId>,
}

impl TransportSessionKey {
    #[must_use]
    pub(crate) fn new(
        channel_runtime_id: u64,
        media_worker_id: usize,
        connection_id: u64,
        session_id: SessionId,
    ) -> Self {
        Self {
            channel_runtime: channel_runtime_id,
            media_worker: media_worker_id,
            connection: connection_id,
            session: Arc::new(session_id),
        }
    }

    #[must_use]
    pub(crate) fn channel_runtime_id(&self) -> u64 {
        self.channel_runtime
    }

    #[must_use]
    pub(crate) fn media_worker_id(&self) -> usize {
        self.media_worker
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> &SessionId {
        self.session.as_ref()
    }
}

/// Direction of a WebRTC transport from the client's perspective.
#[cfg(test)]
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

/// Named request for connecting one transport direction with client auth data.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransportConnectRequest<'a> {
    direction: TransportConnectDirection,
    dtls_parameters: &'a TransportConnectDtlsParameters,
    ice_parameters: Option<&'a TransportConnectIceParameters>,
    sdp_offer: Option<&'a str>,
}

#[cfg(test)]
impl<'a> TransportConnectRequest<'a> {
    #[must_use]
    pub(crate) fn new(
        direction: TransportConnectDirection,
        dtls_parameters: &'a TransportConnectDtlsParameters,
    ) -> Self {
        Self {
            direction,
            dtls_parameters,
            ice_parameters: None,
            sdp_offer: None,
        }
    }

    #[must_use]
    pub(crate) fn with_ice_parameters(
        mut self,
        ice_parameters: &'a TransportConnectIceParameters,
    ) -> Self {
        self.ice_parameters = Some(ice_parameters);
        self
    }

    #[must_use]
    pub(crate) fn with_sdp_offer(mut self, sdp_offer: &'a str) -> Self {
        self.sdp_offer = Some(sdp_offer);
        self
    }

    #[must_use]
    pub(crate) const fn direction(self) -> TransportConnectDirection {
        self.direction
    }

    #[must_use]
    pub(crate) const fn dtls_parameters(self) -> &'a TransportConnectDtlsParameters {
        self.dtls_parameters
    }

    #[must_use]
    pub(crate) const fn ice_parameters(self) -> Option<&'a TransportConnectIceParameters> {
        self.ice_parameters
    }

    #[must_use]
    pub(crate) const fn sdp_offer(self) -> Option<&'a str> {
        self.sdp_offer
    }
}

/// Point-in-time bitrate measurement aggregated across one or more transport sessions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransportBitrateSnapshot {
    pub(crate) total: u64,
    pub(crate) per_media: Vec<(TransportMediaId, u64)>,
}

/// Opaque identifier for a media line allocated by the transport adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub(crate) struct TransportMediaId(u64);

impl TransportMediaId {
    pub(crate) fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Transport-observed active speaker keyed by the producing media source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveSpeakerSource {
    transport_media_id: TransportMediaId,
    observed_at: Instant,
}

impl ActiveSpeakerSource {
    #[must_use]
    pub(crate) const fn new(transport_media_id: TransportMediaId, observed_at: Instant) -> Self {
        Self {
            transport_media_id,
            observed_at,
        }
    }

    #[must_use]
    pub(crate) const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub(crate) const fn observed_at(self) -> Instant {
        self.observed_at
    }
}

/// Generic transport-owned packet gate applied to one published source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourcePacketGate {
    Rid(String),
}

/// Transitional server-authored SDP offer returned by the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionOffer {
    sdp: String,
}

impl SessionOffer {
    #[must_use]
    pub(crate) fn new(sdp: String) -> Self {
        Self { sdp }
    }

    #[must_use]
    pub(crate) fn into_sdp(self) -> String {
        self.sdp
    }
}
