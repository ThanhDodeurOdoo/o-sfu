use o_sfu_router::{MediaFormat, MediaKind, Mid, Rid, Ssrc};

use super::{
    PublishedSourceId, PublishedSourceOwner, SourceEncodingId, SourceModelError, SourcePolicy,
    SourceTemporalLayerId, UploadLayerPolicyRole, UserStreamId,
};

/// Authoritative room-domain description of one published source.
///
/// The descriptor groups the stable source id, owner, orchestration stream id,
/// media kind, source policy and negotiated source facts that recording,
/// diagnostics and transport projection need to agree on. It does not own
/// router producer state, socket state or packet-loop routing tables.
///
/// # Invariants
///
/// A descriptor must contain at least one encoding and every encoding must point
/// back to this descriptor's source id. [`Self::new`] validates both rules so
/// callers do not keep a parallel identity model by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceDescriptor {
    /// Runtime source identity shared by every view of this publication.
    source_id: PublishedSourceId,
    /// Live publishing authority used to reject stale commit or cleanup work.
    owner: PublishedSourceOwner,
    /// Orchestration-owned stream identity scoped by the source owner.
    stream_id: UserStreamId,
    /// Router-facing media family used by negotiation and route planning.
    media_kind: MediaKind,
    /// Room policy metadata supplied by orchestration for this source.
    policy: SourcePolicy,
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
    ///
    /// # Errors
    ///
    /// Returns [`SourceModelError`] when the descriptor has no encodings or
    /// when its encoding identities are duplicated.
    pub fn new(parts: PublishedSourceDescriptorParts) -> Result<Self, SourceModelError> {
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
            stream_id: parts.stream_id,
            media_kind: parts.media_kind,
            policy: parts.policy,
            mid: parts.mid,
            encodings: parts.encodings,
        })
    }

    #[must_use]
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[must_use]
    pub const fn owner(&self) -> &PublishedSourceOwner {
        &self.owner
    }

    #[must_use]
    pub const fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    #[must_use]
    pub const fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub const fn policy(&self) -> SourcePolicy {
        self.policy
    }

    #[must_use]
    pub fn mid(&self) -> Option<&Mid> {
        self.mid.as_ref()
    }

    pub fn encodings(&self) -> impl Iterator<Item = &SourceEncodingDescriptor> {
        self.encodings.iter()
    }

    /// Returns an encoding by source-encoding identity.
    ///
    /// Missing values are normal for best-effort callers such as diagnostics or
    /// selector resolution after a source changed. Mutation paths should treat a
    /// miss as stale work and re-read authoritative room state.
    #[must_use]
    pub fn encoding(&self, encoding_id: SourceEncodingId) -> Option<&SourceEncodingDescriptor> {
        self.encodings
            .iter()
            .find(|encoding| encoding.encoding_id() == encoding_id)
    }
}

/// Construction input for [`PublishedSourceDescriptor`].
///
/// The grouped input keeps descriptor construction explicit without growing a
/// long positional constructor. Callers should fill it from already-normalized
/// runtime facts, not raw SDP or browser JSON
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceDescriptorParts {
    /// Stable source id allocated by the room-domain registry.
    pub source_id: PublishedSourceId,
    /// Publishing user authority for stale-work checks.
    pub owner: PublishedSourceOwner,
    /// Orchestration-owned stream identity scoped by the source owner.
    pub stream_id: UserStreamId,
    /// Router-facing media family for this source.
    pub media_kind: MediaKind,
    /// Room policy metadata supplied by orchestration for this source.
    pub policy: SourcePolicy,
    /// Negotiated media-section id when known.
    pub mid: Option<Mid>,
    /// Encodings that belong to this source.
    pub encodings: Vec<SourceEncodingDescriptor>,
}

/// Room-domain description of one advertised source encoding.
///
/// This keeps policy identity separate from negotiated transport facts. RID,
/// SSRC, bitrate and codec data are metadata that help route projection,
/// diagnostics and recording describe the encoding. They are not substitutes
/// for [`SourceEncodingId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEncodingDescriptor {
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
    /// Sender-side resolution downscale advertised for this encoding.
    resolution_scale: Option<u16>,
    /// Sender-side frame-rate ceiling advertised for this encoding.
    max_framerate: Option<u16>,
    /// Server-owned policy role associated with this encoding.
    policy_role: Option<UploadLayerPolicyRole>,
    /// Highest temporal layer advertised for codec-native layered forwarding.
    max_temporal_layer_id: Option<SourceTemporalLayerId>,
    /// Negotiated payload and codec information for this encoding.
    negotiated_format: Option<MediaFormat>,
}

impl SourceEncodingDescriptor {
    /// Creates an encoding descriptor from normalized runtime facts.
    ///
    /// This constructor does not validate parent membership because a single
    /// encoding is not authoritative alone. [`PublishedSourceDescriptor::new`]
    /// validates the full source graph when the encoding list is assembled.
    #[must_use]
    pub fn new(parts: SourceEncodingDescriptorParts) -> Self {
        Self {
            encoding_id: parts.encoding_id,
            source_id: parts.source_id,
            rid: parts.rid,
            primary_ssrc: parts.primary_ssrc,
            repair_ssrc: parts.repair_ssrc,
            max_bitrate: parts.max_bitrate,
            resolution_scale: parts.resolution_scale,
            max_framerate: parts.max_framerate,
            policy_role: parts.policy_role,
            max_temporal_layer_id: parts.max_temporal_layer_id,
            negotiated_format: parts.negotiated_format,
        }
    }

    #[must_use]
    pub const fn encoding_id(&self) -> SourceEncodingId {
        self.encoding_id
    }

    #[must_use]
    pub const fn source_id(&self) -> PublishedSourceId {
        self.source_id
    }

    #[must_use]
    pub fn rid(&self) -> Option<&Rid> {
        self.rid.as_ref()
    }

    #[must_use]
    pub const fn primary_ssrc(&self) -> Option<Ssrc> {
        self.primary_ssrc
    }

    #[must_use]
    pub const fn repair_ssrc(&self) -> Option<Ssrc> {
        self.repair_ssrc
    }

    #[must_use]
    pub const fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }

    #[must_use]
    pub const fn resolution_scale(&self) -> Option<u16> {
        self.resolution_scale
    }

    #[must_use]
    pub const fn max_framerate(&self) -> Option<u16> {
        self.max_framerate
    }

    #[must_use]
    pub const fn policy_role(&self) -> Option<UploadLayerPolicyRole> {
        self.policy_role
    }

    #[must_use]
    pub const fn max_temporal_layer_id(&self) -> Option<SourceTemporalLayerId> {
        self.max_temporal_layer_id
    }

    #[must_use]
    pub fn negotiated_format(&self) -> Option<&MediaFormat> {
        self.negotiated_format.as_ref()
    }
}

/// Construction input for [`SourceEncodingDescriptor`]
///
/// The fields are optional where negotiation may not have learned the fact yet.
/// The source and encoding ids must still be stable before the descriptor is
/// stored in room state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEncodingDescriptorParts {
    /// Stable encoding id allocated by the room-domain registry.
    pub encoding_id: SourceEncodingId,
    /// Parent source id.
    pub source_id: PublishedSourceId,
    /// Negotiated RID for RID-based simulcast.
    pub rid: Option<Rid>,
    /// Primary RTP SSRC when available.
    pub primary_ssrc: Option<Ssrc>,
    /// Repair RTP SSRC when available.
    pub repair_ssrc: Option<Ssrc>,
    /// Optional bitrate ceiling advertised for this encoding.
    pub max_bitrate: Option<u64>,
    /// Optional resolution downscale advertised for this encoding.
    pub resolution_scale: Option<u16>,
    /// Optional frame-rate ceiling advertised for this encoding.
    pub max_framerate: Option<u16>,
    /// Optional policy role advertised for this encoding.
    pub policy_role: Option<UploadLayerPolicyRole>,
    /// Optional temporal-layer ceiling advertised for codec-native SVC.
    pub max_temporal_layer_id: Option<SourceTemporalLayerId>,
    /// Negotiated codec and payload information when available.
    pub negotiated_format: Option<MediaFormat>,
}
