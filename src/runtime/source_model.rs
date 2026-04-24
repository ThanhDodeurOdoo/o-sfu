//! Runtime-native source and encoding vocabulary for published media.
//!
//! # !!!! TEMPORARY DOC, THE IMPLEMENTATION MAY CHANGE AS WE DEVELOP SIMULCAST/SVC. !!!!
//!
//! # Boundary role
//!
//! This module defines the room-domain source identity that chanel state,
//! transport projection, diagnostics and recording metadata are expected to
//! share. It is the vocabulary above SDP, browser APIs and worker-local media
//! handles: those layers may attach facts to a source, but they must not define
//! the source identity.
//!
//! A published camera, screen share or audio track is modeled as one
//! [`PublishedSourceId`] plus one or more [`SourceEncodingId`] values. `Mid`,
//! `Rid`, `Ssrc` and [`TransportMediaId`] stay as negotiated or transport-facing
//! attachment points. Keeping those identities separate lets later same-room
//! spillover and recording consume the same source inventory without redefining
//! it around local worker placement (recording and spillover are not implemented
//! yet, just built ahead with them in mind, todo: remove comment when it is implemented)
//!
//! # Performance
//!
//! The types here are cold-path metadata used while planning or describing
//! publications. Packet loops should consume already-projected transport gates
//! instead of walking these descriptors per packet.

#![allow(
    dead_code,
    reason = "non-default selector variants are reserved for the next quality-policy slices"
)]

use std::fmt::{self, Display, Formatter};

use o_sfu_protocol::shared::{SessionId, StreamType};
use o_sfu_router::{MediaFormat, MediaKind, Mid, Rid, Ssrc};
use thiserror::Error;

use crate::runtime::{ConnectionId, transport_adapter::TransportMediaId};

/// Stable room-domain identity for one logical published source.
///
/// A source id identifies the publication itself, not the SDP media section,
/// negotiated RID, RTP SSRC or transport media handle currently realizing it.
/// Channel state should allocate it when a publish becomes a room-domain source
/// and keep using it across renegotiation or transport reattachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PublishedSourceId(u64);

impl PublishedSourceId {
    #[must_use]
    pub(crate) fn allocate(next_source_id: &mut u64) -> Self {
        let source_id = Self(*next_source_id);
        *next_source_id = next_source_id.saturating_add(1);
        source_id
    }

    #[must_use]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for PublishedSourceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "source-{}", self.0)
    }
}

/// Stable room-domain identity for one advertised source encoding
///
/// The id names an operating point of a source such as a simulcast `lo` or
/// `hi` layer. It intentionally does not encode a RID string or worker-local
/// route so selectors can keep pointing at the same encoding after transport
/// details are refreshed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SourceEncodingId(u64);

impl SourceEncodingId {
    #[must_use]
    pub(crate) fn allocate(next_encoding_id: &mut u64) -> Self {
        let encoding_id = Self(*next_encoding_id);
        *next_encoding_id = next_encoding_id.saturating_add(1);
        encoding_id
    }

    #[must_use]
    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for SourceEncodingId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "encoding-{}", self.0)
    }
}

/// Publishing session authority attached to a source descriptor.
///
/// The session identifies the logical owner visible to room policy. The
/// connection id keeps stale async publish work from a replaced websocket from
/// being mistaken for the live owner during later registry commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedSourceOwner {
    session_id: SessionId,
    connection_id: ConnectionId,
}

impl PublishedSourceOwner {
    #[must_use]
    pub(crate) fn new(session_id: SessionId, connection_id: ConnectionId) -> Self {
        Self {
            session_id,
            connection_id,
        }
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    #[must_use]
    pub(crate) const fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }
}

/// Transport realization currently attached to a source or encoding.
///
/// This is an attachment point, not an identity. Several encodings may share
/// one transport media handle while a later topology may attach mirrored or
/// relayed handles to the same source graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceTransportBinding {
    transport_media_id: TransportMediaId,
}

impl SourceTransportBinding {
    #[must_use]
    pub(crate) const fn new(transport_media_id: TransportMediaId) -> Self {
        Self { transport_media_id }
    }

    #[must_use]
    pub(crate) const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }
}

/// Consumer-side routing intent for a source.
///
/// Selectors live above packet gates. Channel policy can express whether a
/// consumer wants the source open, pinned to a concrete source encoding or left
/// to a room policy bucket. Transport code should receive a projected
/// transport-native gate after this intent is resolved
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SourceSelector {
    /// No source-level limit has been requested.
    #[default]
    Open,
    /// Select one source encoding by runtime identity.
    Encoding(SourceEncodingId),
    /// Defer the concrete encoding choice to room-level policy.
    RoomPolicy(SourceRoomPolicySelector),
}

impl SourceSelector {
    #[must_use]
    pub(crate) const fn selected_encoding(self) -> Option<SourceEncodingId> {
        match self {
            Self::Encoding(encoding_id) => Some(encoding_id),
            Self::Open | Self::RoomPolicy(_) => None,
        }
    }
}

/// Room policy bucket used before policy resolves to a concrete encoding.
///
/// This keeps layout intent out of the transport layer. The channel can decide
/// later that a thumbnail should map to a lower simulcast encoding while a
/// featured source stays unconstrained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceRoomPolicySelector {
    /// Source is important for the current room layout.
    Featured,
    /// Source is consumed as a small or background view.
    Thumbnail,
}

/// Consumer-side desired state for one published source.
///
/// The active flag is the compatibility-level subscription decision. The
/// selector is the source-level quality intent that later adaptation and
/// layout policy can resolve into a transport-native gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConsumerSourceSelection {
    active: bool,
    selector: SourceSelector,
}

impl ConsumerSourceSelection {
    #[must_use]
    pub(crate) const fn open(active: bool) -> Self {
        Self {
            active,
            selector: SourceSelector::Open,
        }
    }

    #[must_use]
    pub(crate) const fn active(self) -> bool {
        self.active
    }

    #[must_use]
    pub(crate) const fn selector(self) -> SourceSelector {
        self.selector
    }

    pub(crate) const fn set_active(&mut self, active: bool) {
        self.active = active;
    }
}

/// Authoritative room-domain description of one published source.
///
/// The descriptor groups the stable source id, owner, compatibility stream
/// label, media kind and negotiated source facts that recording, diagnostics
/// and transport projection need to agree on. It does not own router producer
/// state, socket state or packet-loop routing tables
///
/// # Invariants
///
/// A descriptor must contain at least one encoding and every encoding must point
/// back to this descriptor's source id. [`Self::new`] validates both rules so
/// callers do not keep a parallel identity model by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedSourceDescriptor {
    /// Runtime source identity shared by every view of this publication.
    source_id: PublishedSourceId,
    /// Live publishing authority used to reject stale commit or cleanup work.
    owner: PublishedSourceOwner,
    /// Odoo-facing compatibility label kept as source metadata, not identity.
    stream_type: StreamType,
    /// Router-facing media family used by negotiation and route planning.
    media_kind: MediaKind,
    /// Negotiated media-section identity when the RTC edge has one.
    mid: Option<Mid>,
    /// Advertised encodings owned by this logical source.
    encodings: Vec<SourceEncodingDescriptor>,
}

impl PublishedSourceDescriptor {
    /// Builds a source descriptor after checking the source graph invariants.
    ///
    /// Failure means the caller assembled an invalid room-domain source and
    /// should abort the surrounding publish commit before any registry state is
    /// made authoritative.
    pub(crate) fn new(parts: PublishedSourceDescriptorParts) -> Result<Self, SourceModelError> {
        if parts.encodings.is_empty() {
            return Err(SourceModelError::SourceWithoutEncodings {
                source_id: parts.source_id,
            });
        }
        if let Some(encoding) = parts
            .encodings
            .iter()
            .find(|encoding| encoding.source_id() != parts.source_id)
        {
            return Err(SourceModelError::EncodingSourceMismatch {
                source_id: parts.source_id,
                encoding_id: encoding.encoding_id(),
                encoding_source_id: encoding.source_id(),
            });
        }
        Ok(Self {
            source_id: parts.source_id,
            owner: parts.owner,
            stream_type: parts.stream_type,
            media_kind: parts.media_kind,
            mid: parts.mid,
            encodings: parts.encodings,
        })
    }

    #[must_use]
    pub(crate) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[must_use]
    pub(crate) const fn owner(&self) -> &PublishedSourceOwner {
        &self.owner
    }

    #[must_use]
    pub(crate) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    #[must_use]
    pub(crate) const fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub(crate) fn mid(&self) -> Option<&Mid> {
        self.mid.as_ref()
    }

    pub(crate) fn encodings(&self) -> impl Iterator<Item = &SourceEncodingDescriptor> {
        self.encodings.iter()
    }

    /// Returns an encoding by source-encoding identity.
    ///
    /// Missing values are normal for best-effort callers such as diagnostics or
    /// selector resolution after a source changed. Mutation paths should treat a
    /// miss as stale work and re-read authoritative channel state.
    #[must_use]
    pub(crate) fn encoding(
        &self,
        encoding_id: SourceEncodingId,
    ) -> Option<&SourceEncodingDescriptor> {
        self.encodings
            .iter()
            .find(|encoding| encoding.encoding_id() == encoding_id)
    }

    /// Iterates transport handles currently attached through the encodings.
    ///
    /// The result is a cold-path projection. It may contain repeated transport
    /// media ids when several source encodings are realized by one negotiated
    /// media section.
    pub(crate) fn transport_bindings(&self) -> impl Iterator<Item = SourceTransportBinding> + '_ {
        self.encodings
            .iter()
            .filter_map(SourceEncodingDescriptor::transport_binding)
    }
}

/// Construction input for [`PublishedSourceDescriptor`].
///
/// The grouped input keeps descriptor construction explicit without growing a
/// long positional constructor. Callers should fill it from already-normalized
/// runtime facts, not raw SDP or browser JSON
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedSourceDescriptorParts {
    /// Stable source id allocated by the room-domain registry.
    pub(crate) source_id: PublishedSourceId,
    /// Publishing session authority for stale-work checks.
    pub(crate) owner: PublishedSourceOwner,
    /// Compatibility label used by the existing Odoo-facing API.
    pub(crate) stream_type: StreamType,
    /// Router-facing media family for this source.
    pub(crate) media_kind: MediaKind,
    /// Negotiated media-section id when known.
    pub(crate) mid: Option<Mid>,
    /// Encodings that belong to this source.
    pub(crate) encodings: Vec<SourceEncodingDescriptor>,
}

/// Room-domain description of one advertised source encoding.
///
/// This keeps policy identity separate from negotiated transport facts. RID,
/// SSRC, bitrate and codec data are metadata that help route projection,
/// diagnostics and recording describe the encoding; they are not substitutes
/// for [`SourceEncodingId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceEncodingDescriptor {
    /// Stable source-encoding identity used by selectors.
    encoding_id: SourceEncodingId,
    /// Parent logical source.
    source_id: PublishedSourceId,
    /// Negotiated RID when simulcast or layered transport exposes one.
    rid: Option<Rid>,
    /// Primary RTP SSRC when known from negotiation or packet observation.
    primary_ssrc: Option<Ssrc>,
    /// Repair RTP SSRC such as RTX when known.
    repair_ssrc: Option<Ssrc>,
    /// Sender-declared bitrate ceiling for this encoding.
    max_bitrate: Option<u64>,
    /// Negotiated payload and codec information for this encoding.
    negotiated_format: Option<MediaFormat>,
    /// Current local or relayed transport realization.
    transport_binding: Option<SourceTransportBinding>,
}

impl SourceEncodingDescriptor {
    /// Creates an encoding descriptor from normalized runtime facts.
    ///
    /// This constructor does not validate parent membership because a single
    /// encoding is not authoritative alone. [`PublishedSourceDescriptor::new`]
    /// validates the full source graph when the encoding list is assembled.
    #[must_use]
    pub(crate) fn new(parts: SourceEncodingDescriptorParts) -> Self {
        Self {
            encoding_id: parts.encoding_id,
            source_id: parts.source_id,
            rid: parts.rid,
            primary_ssrc: parts.primary_ssrc,
            repair_ssrc: parts.repair_ssrc,
            max_bitrate: parts.max_bitrate,
            negotiated_format: parts.negotiated_format,
            transport_binding: parts.transport_binding,
        }
    }

    #[must_use]
    pub(crate) const fn encoding_id(&self) -> SourceEncodingId {
        self.encoding_id
    }

    #[must_use]
    pub(crate) const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[must_use]
    pub(crate) fn rid(&self) -> Option<&Rid> {
        self.rid.as_ref()
    }

    #[must_use]
    pub(crate) const fn primary_ssrc(&self) -> Option<Ssrc> {
        self.primary_ssrc
    }

    #[must_use]
    pub(crate) const fn repair_ssrc(&self) -> Option<Ssrc> {
        self.repair_ssrc
    }

    #[must_use]
    pub(crate) const fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }

    #[must_use]
    pub(crate) fn negotiated_format(&self) -> Option<&MediaFormat> {
        self.negotiated_format.as_ref()
    }

    #[must_use]
    pub(crate) const fn transport_binding(&self) -> Option<SourceTransportBinding> {
        self.transport_binding
    }
}

/// Construction input for [`SourceEncodingDescriptor`]
///
/// The fields are optional where negotiation may not have learned the fact yet.
/// The source and encoding ids must still be stable before the descriptor is
/// stored in chanel state
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceEncodingDescriptorParts {
    /// Stable encoding id allocated by the room-domain registry.
    pub(crate) encoding_id: SourceEncodingId,
    /// Parent source id.
    pub(crate) source_id: PublishedSourceId,
    /// Negotiated RID for RID-based simulcast.
    pub(crate) rid: Option<Rid>,
    /// Primary RTP SSRC when available.
    pub(crate) primary_ssrc: Option<Ssrc>,
    /// Repair RTP SSRC when available.
    pub(crate) repair_ssrc: Option<Ssrc>,
    /// Optional bitrate ceiling advertised for this encoding.
    pub(crate) max_bitrate: Option<u64>,
    /// Negotiated codec and payload information when available.
    pub(crate) negotiated_format: Option<MediaFormat>,
    /// Current transport attachment for this encoding.
    pub(crate) transport_binding: Option<SourceTransportBinding>,
}

/// Rejection returned while assembling a source descriptor.
///
/// # Error handling guidance
///
/// These are construction-time domain errors. They should be handled before a
/// publish becomes authoritative in chanel state. They are not transport
/// failures and should not be retried without rebuilding the source descriptor
/// from valid runtime facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum SourceModelError {
    #[error("published source {source_id} has no advertised encoding")]
    SourceWithoutEncodings { source_id: PublishedSourceId },
    #[error(
        "encoding {encoding_id} belongs to {encoding_source_id}, not published source {source_id}"
    )]
    EncodingSourceMismatch {
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        encoding_source_id: PublishedSourceId,
    },
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::unwrap_used,
        reason = "test assertions use expect, unwrap and direct indexing for direct fixture failures"
    )]

    use o_sfu_router::{MediaCodec, PayloadType};

    use super::*;

    fn video_format(payload_type: u8) -> MediaFormat {
        MediaFormat::new(
            MediaKind::Video,
            MediaCodec::from("VP8"),
            PayloadType::new(payload_type),
            90_000,
        )
    }

    fn source_encoding(
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        rid: &str,
        transport_media_id: TransportMediaId,
    ) -> SourceEncodingDescriptor {
        let raw_encoding_id =
            u32::try_from(encoding_id.as_u64()).expect("test encoding id should fit in u32");
        SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id,
            source_id,
            rid: Some(Rid::new(rid)),
            primary_ssrc: Some(Ssrc::new(100 + raw_encoding_id)),
            repair_ssrc: Some(Ssrc::new(200 + raw_encoding_id)),
            max_bitrate: Some(150_000 * encoding_id.as_u64()),
            negotiated_format: Some(video_format(96)),
            transport_binding: Some(SourceTransportBinding::new(transport_media_id)),
        })
    }

    #[test]
    fn descriptor_keeps_source_encoding_and_transport_identity_separate() {
        let source_id = PublishedSourceId::from_raw(7);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let owner = PublishedSourceOwner::new(SessionId::Integer(42), ConnectionId::from_raw(9));
        let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner,
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: Some(Mid::new("video-0")),
            encodings: vec![
                source_encoding(source_id, low_encoding_id, "lo", TransportMediaId::new(55)),
                source_encoding(source_id, high_encoding_id, "hi", TransportMediaId::new(55)),
            ],
        })
        .expect("source descriptor should be valid");

        assert_eq!(descriptor.source_id(), source_id);
        assert_eq!(descriptor.owner().session_id(), &SessionId::Integer(42));
        assert_eq!(
            descriptor.owner().connection_id(),
            ConnectionId::from_raw(9)
        );
        assert_eq!(descriptor.stream_type(), StreamType::Camera);
        assert_eq!(descriptor.media_kind(), MediaKind::Video);
        assert_eq!(
            descriptor.mid().map(Mid::as_str),
            Some("video-0"),
            "the source owns the SDP media-section identity separately from RID"
        );

        let encodings = descriptor.encodings().collect::<Vec<_>>();
        assert_eq!(encodings.len(), 2);
        assert_eq!(encodings[0].source_id(), source_id);
        assert_eq!(encodings[0].rid().map(Rid::as_str), Some("lo"));
        assert_eq!(encodings[0].primary_ssrc(), Some(Ssrc::new(101)));
        assert_eq!(encodings[0].repair_ssrc(), Some(Ssrc::new(201)));
        assert_eq!(encodings[0].max_bitrate(), Some(150_000));
        assert_eq!(
            encodings[0]
                .negotiated_format()
                .map(MediaFormat::payload_type_id),
            Some(PayloadType::new(96))
        );
        assert_eq!(
            descriptor
                .transport_bindings()
                .map(SourceTransportBinding::transport_media_id)
                .collect::<Vec<_>>(),
            vec![TransportMediaId::new(55), TransportMediaId::new(55)]
        );
        assert_eq!(
            descriptor
                .encoding(high_encoding_id)
                .and_then(SourceEncodingDescriptor::rid)
                .map(Rid::as_str),
            Some("hi")
        );
    }

    #[test]
    fn descriptor_rejects_encoding_from_another_source() {
        let source_id = PublishedSourceId::from_raw(7);
        let other_source_id = PublishedSourceId::from_raw(8);
        let encoding_id = SourceEncodingId::from_raw(1);
        let result = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(SessionId::Integer(42), ConnectionId::from_raw(9)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings: vec![source_encoding(
                other_source_id,
                encoding_id,
                "lo",
                TransportMediaId::new(55),
            )],
        });

        assert_eq!(
            result.unwrap_err(),
            SourceModelError::EncodingSourceMismatch {
                source_id,
                encoding_id,
                encoding_source_id: other_source_id,
            }
        );
    }

    #[test]
    fn selector_targets_runtime_encoding_identity_not_transport_or_rid() {
        let encoding_id = SourceEncodingId::from_raw(3);

        assert_eq!(
            SourceSelector::Encoding(encoding_id).selected_encoding(),
            Some(encoding_id)
        );
        assert_eq!(SourceSelector::Open.selected_encoding(), None);
        assert_eq!(
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::Thumbnail).selected_encoding(),
            None
        );
    }

    #[test]
    fn id_allocation_is_monotonic_and_topology_neutral() {
        let mut next_source_id = 1;
        let mut next_encoding_id = 1;

        assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 1);
        assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 2);
        assert_eq!(
            SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
            1
        );
        assert_eq!(
            SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
            2
        );
    }
}
