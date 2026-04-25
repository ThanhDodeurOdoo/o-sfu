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

/// Latest receiver-side bandwidth estimates keyed by transport session.
///
/// These values are produced by the WebRTC egress BWE path and consumed by
/// room-owned media policy. They are cold-path control-plane facts, not packet
/// loop routing state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReceiverBandwidthSnapshot {
    pub(crate) per_session: Vec<(TransportSessionKey, u64)>,
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

/// Transport-owned packet gate applied to one published source.
///
/// Room policy decides in source-domain terms. The transport boundary receives
/// only the packet-facing projection that the worker can apply without knowing
/// room layout, source identity, or future relay placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourcePacketGate {
    Open,
    Rid(String),
    OperatingPoint(SourcePacketOperatingPoint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerPacketGateUpdate {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    packet_gate: SourcePacketGate,
}

impl ConsumerPacketGateUpdate {
    #[must_use]
    pub(crate) fn new(
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Self {
        Self {
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            packet_gate,
        }
    }

    #[must_use]
    pub(crate) fn consumer_session_key(&self) -> &TransportSessionKey {
        &self.consumer_session_key
    }

    #[must_use]
    pub(crate) const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    #[must_use]
    pub(crate) fn source_session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    #[must_use]
    pub(crate) const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    #[must_use]
    pub(crate) fn packet_gate(&self) -> &SourcePacketGate {
        &self.packet_gate
    }
}

/// Packet-facing layered operating point selected for one source route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourcePacketOperatingPoint {
    rid: Option<String>,
    max_temporal_layer_id: u8,
}

impl SourcePacketOperatingPoint {
    #[must_use]
    pub(crate) fn new(rid: Option<String>, max_temporal_layer_id: u8) -> Self {
        Self {
            rid,
            max_temporal_layer_id,
        }
    }

    #[must_use]
    pub(crate) fn rid(&self) -> Option<&str> {
        self.rid.as_deref()
    }

    #[must_use]
    pub(crate) const fn max_temporal_layer_id(&self) -> u8 {
        self.max_temporal_layer_id
    }
}

/// Transitional server-authored SDP offer returned by the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionOffer {
    sdp: String,
    upload_slots: Vec<SessionUploadSlot>,
}

impl SessionOffer {
    #[must_use]
    pub(crate) fn new(sdp: String) -> Self {
        Self {
            sdp,
            upload_slots: Vec::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_upload_slots(mut self, upload_slots: Vec<SessionUploadSlot>) -> Self {
        self.upload_slots = upload_slots;
        self
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn into_sdp(self) -> String {
        self.sdp
    }

    #[must_use]
    pub(crate) fn into_parts(self) -> (String, Vec<SessionUploadSlot>) {
        (self.sdp, self.upload_slots)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionUploadKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionUploadSlot {
    pub(crate) mid: String,
    pub(crate) kind: SessionUploadKind,
    pub(crate) codecs: Vec<String>,
    pub(crate) simulcast_encodings: Vec<SessionUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionUploadEncoding {
    pub(crate) rid: String,
    pub(crate) max_bitrate: Option<u64>,
}
