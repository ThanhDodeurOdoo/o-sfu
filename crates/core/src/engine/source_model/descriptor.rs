use o_sfu_rfc::rtp::{Mid, Rid, Ssrc};
use o_sfu_router::{MediaKind, rtp::MediaFormat};
use thiserror::Error;

use super::{
    PublishedSourceId, PublishedSourceOwner, SourceEncodingId, SourcePolicy, UploadLayerPolicyRole,
    UserStreamId,
};
use crate::Bitrate;

/// Rejection returned while assembling a source descriptor.
///
/// # Error handling
///
/// These are construction-time domain errors. They should be handled before a
/// publish becomes authoritative in room state. They are not transport
/// failures and should not be retried without rebuilding the source descriptor
/// from valid runtime facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceModelError {
    #[error("published source {source_id} has no advertised encoding")]
    SourceWithoutEncodings { source_id: PublishedSourceId },
    #[error("published source {source_id} has duplicate encoding {encoding_id}")]
    DuplicateEncodingId {
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
    },
    #[error(
        "encoding {encoding_id} belongs to {encoding_source_id}, not published source {source_id}"
    )]
    EncodingSourceMismatch {
        source_id: PublishedSourceId,
        encoding_id: SourceEncodingId,
        encoding_source_id: PublishedSourceId,
    },
}

/// Authoritative room-domain description of one published source.
///
/// The descriptor groups the stable source id, publishing authority, caller
/// stream id, media kind, source policy and negotiated source facts required by
/// recording, diagnostics and transport projection. It deliberately excludes
/// router producer state, socket state and packet-loop routing tables.
///
/// # Invariants
///
/// A descriptor must contain at least one encoding, every encoding id must be
/// unique and every encoding must point back to this descriptor's source id.
/// [`Self::new`] validates these rules so callers do not keep a parallel
/// identity model by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceDescriptor {
    /// Runtime source identity shared by every view of this publication.
    source_id: PublishedSourceId,
    /// Live publishing authority used to reject stale commit or cleanup work.
    owner: PublishedSourceOwner,
    /// User-scoped stream identity supplied with the publish intent.
    stream_id: UserStreamId,
    /// Router-facing media family used by negotiation and route planning.
    media_kind: MediaKind,
    /// Room policy captured by the publish intent for this source.
    policy: SourcePolicy,
    /// Negotiated media-section identity when the RTC edge has one.
    mid: Option<Mid>,
    /// Advertised encodings that belong to this logical source.
    encodings: Vec<SourceEncodingDescriptor>,
    /// Source-policy selectable encodings ordered by receiver budget priority.
    selectable_encoding_indices: Vec<usize>,
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
    /// Returns [`SourceModelError`] when the descriptor has no encodings, uses
    /// a duplicate encoding id or contains an encoding whose source id points
    /// elsewhere
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
        if let Some(encoding_id) = duplicate_encoding_id(&parts.encodings) {
            return Err(SourceModelError::DuplicateEncodingId {
                source_id: parts.source_id,
                encoding_id,
            });
        }
        let selectable_encoding_indices = selectable_encoding_indices(&parts.encodings);
        Ok(Self {
            source_id: parts.source_id,
            owner: parts.owner,
            stream_id: parts.stream_id,
            media_kind: parts.media_kind,
            policy: parts.policy,
            mid: parts.mid,
            encodings: parts.encodings,
            selectable_encoding_indices,
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

    /// Returns the encodings that receiver video policy may select.
    ///
    /// Partial RID coverage disables selection because packet-gate projection
    /// cannot represent every advertised encoding. Sources with bitrate hints
    /// use ascending advertised maximum bitrate order. Sources without bitrate
    /// hints use upload layer policy role order when available, then keep the
    /// publisher-declared order.
    pub fn selectable_encodings(&self) -> impl Iterator<Item = &SourceEncodingDescriptor> {
        self.selectable_encoding_indices
            .iter()
            .filter_map(|index| self.encodings.get(*index))
    }

    #[must_use]
    pub fn selectable_encoding_count(&self) -> usize {
        self.selectable_encoding_indices.len()
    }

    #[must_use]
    pub fn selectable_encoding_by_rank(&self, rank: usize) -> Option<&SourceEncodingDescriptor> {
        self.selectable_encoding_indices
            .get(rank)
            .and_then(|index| self.encodings.get(*index))
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

fn duplicate_encoding_id(encodings: &[SourceEncodingDescriptor]) -> Option<SourceEncodingId> {
    encodings.iter().enumerate().find_map(|(index, encoding)| {
        let encoding_id = encoding.encoding_id();
        encodings
            .iter()
            .skip(index + 1)
            .any(|other| other.encoding_id() == encoding_id)
            .then_some(encoding_id)
    })
}

fn selectable_encoding_indices(encodings: &[SourceEncodingDescriptor]) -> Vec<usize> {
    if encodings.iter().any(|encoding| encoding.rid().is_none()) {
        return Vec::new();
    }
    let mut indices = (0..encodings.len()).collect::<Vec<_>>();
    if encodings
        .iter()
        .any(|encoding| encoding.max_bitrate().is_some())
    {
        indices.sort_by_key(|index| {
            encodings
                .get(*index)
                .and_then(SourceEncodingDescriptor::max_bitrate)
                .unwrap_or(Bitrate::from_bps(u64::MAX))
        });
    } else if encodings
        .iter()
        .any(|encoding| encoding.policy_role().is_some())
    {
        indices.sort_by_key(|index| {
            encodings
                .get(*index)
                .and_then(SourceEncodingDescriptor::policy_role)
                .map_or(u8::MAX, upload_layer_policy_role_rank)
        });
    }
    indices
}

// Keep policy roles low-to-high so rank 0 has the same meaning as in the
// ascending-bitrate path.
const fn upload_layer_policy_role_rank(role: UploadLayerPolicyRole) -> u8 {
    match role {
        UploadLayerPolicyRole::DegradedThumbnail => 0,
        UploadLayerPolicyRole::Thumbnail => 1,
        UploadLayerPolicyRole::Featured => 2,
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
    /// User-scoped stream identity supplied with the publish intent.
    pub stream_id: UserStreamId,
    /// Router-facing media family for this source.
    pub media_kind: MediaKind,
    /// Room policy captured by the publish intent for this source.
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
    max_bitrate: Option<Bitrate>,
    /// Sender-side resolution downscale advertised for this encoding.
    resolution_scale: Option<u16>,
    /// Sender-side frame-rate ceiling advertised for this encoding.
    max_framerate: Option<u16>,
    /// Server-defined policy role associated with this encoding.
    policy_role: Option<UploadLayerPolicyRole>,
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
    pub const fn max_bitrate(&self) -> Option<Bitrate> {
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
    pub max_bitrate: Option<Bitrate>,
    /// Optional resolution downscale advertised for this encoding.
    pub resolution_scale: Option<u16>,
    /// Optional frame-rate ceiling advertised for this encoding.
    pub max_framerate: Option<u16>,
    /// Optional policy role advertised for this encoding.
    pub policy_role: Option<UploadLayerPolicyRole>,
    /// Negotiated codec and payload information when available.
    pub negotiated_format: Option<MediaFormat>,
}
