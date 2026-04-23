use std::{sync::Arc, time::Instant};

use o_sfu_protocol::shared::SessionId;
use thiserror::Error;

use crate::runtime::{ChannelInstanceId, ConnectionId};

/// Channel-scoped transport-adapter session identity.
///
/// A `SessionId` alone is not unique across the server: the same id can appear
/// in different channels simultaneously. This composite key allows one session
/// to be uniquely identified by the owning channel instance, media worker,
/// signaling connection, and session id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSessionKey {
    channel_instance: ChannelInstanceId,
    media_worker: usize,
    connection: ConnectionId,
    session: Arc<SessionId>,
}

impl TransportSessionKey {
    #[must_use]
    pub(crate) fn new(
        channel_instance_id: ChannelInstanceId,
        media_worker_id: usize,
        connection_id: ConnectionId,
        session_id: SessionId,
    ) -> Self {
        Self {
            channel_instance: channel_instance_id,
            media_worker: media_worker_id,
            connection: connection_id,
            session: Arc::new(session_id),
        }
    }

    #[must_use]
    pub(crate) const fn channel_instance_id(&self) -> ChannelInstanceId {
        self.channel_instance
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

pub(crate) type TransportResult<T> = Result<T, TransportAdapterError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum TransportAdapterError {
    #[error("transport unavailable")]
    TransportUnavailable,
    #[error("invalid transport input")]
    InvalidInput,
    #[error("unsupported transport feature")]
    UnsupportedFeature,
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
