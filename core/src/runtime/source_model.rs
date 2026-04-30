//! Runtime-native source, encoding, and selection vocabulary for published media.
//!
//! # Boundary role
//!
//! This module defines the room-domain source identity that room state,
//! transport projection, diagnostics and recording metadata are expected to
//! share. It is the vocabulary above SDP, browser APIs and worker-local media
//! handles: those layers may attach facts to a source, but they must not define
//! the source identity.
//!
//! A published camera, screen share or audio track is modeled as one
//! [`PublishedSourceId`] plus one or more [`SourceEncodingId`] values. `Mid`,
//! `Rid` and `Ssrc` stay as negotiated or transport-facing attachment points.
//! Keeping those identities separate lets later same-room
//! spillover and recording consume the same source inventory without redefining
//! it around local worker placement.
//!
//! # Performance
//!
//! The types here are cold-path metadata used while planning or describing
//! publications. Packet loops should consume already-projected transport gates
//! instead of walking these descriptors per packet.
//!
//! # Upload layer profiles
//!
//! The server-owned upload ladder currently lives at the RTC offer edge as
//! upload-slot metadata, while this module stores the negotiated source
//! encodings that result from the answer. When task 17 makes resolution scale
//! and frame-rate hints server-owned, the lasting upload-layer profile
//! vocabulary belongs here so browser hints, source descriptors, diagnostics
//! and future recording manifests share one model.

use std::fmt::{self, Display, Formatter};

use o_sfu_rfc::rtp::frame_marking;
use o_sfu_router::{MediaFormat, MediaKind, Mid, Rid, Ssrc};
use thiserror::Error;

use crate::runtime::{StreamType, UserId};

/// Stable room-domain identity for one logical published source.
///
/// A source id identifies the publication itself, not the SDP media section,
/// negotiated RID, RTP SSRC or transport media handle currently realizing it.
/// Room state should allocate it when a publish becomes a room-domain source
/// and keep using it across renegotiation or transport reattachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedSourceId(u64);

impl PublishedSourceId {
    #[must_use]
    pub fn allocate(next_source_id: &mut u64) -> Self {
        let source_id = Self(*next_source_id);
        *next_source_id = next_source_id.saturating_add(1);
        source_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
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
pub struct SourceEncodingId(u64);

impl SourceEncodingId {
    #[must_use]
    pub fn allocate(next_encoding_id: &mut u64) -> Self {
        let encoding_id = Self(*next_encoding_id);
        *next_encoding_id = next_encoding_id.saturating_add(1);
        encoding_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for SourceEncodingId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "encoding-{}", self.0)
    }
}

/// Codec-native temporal layer id used by SVC operating points.
///
/// The current representation intentionally follows the RFC 9626 frame-marking
/// temporal-id range. Spatial identity remains modeled by the source encoding,
/// which keeps hybrid simulcast plus SVC as one selected encoding plus one
/// temporal ceiling instead of a second parallel source graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceTemporalLayerId(u8);

impl SourceTemporalLayerId {
    #[allow(
        dead_code,
        reason = "production SVC descriptors will construct temporal ids after RFC 9626 negotiation lands"
    )]
    #[must_use]
    pub const fn new(value: u8) -> Option<Self> {
        if frame_marking::is_valid_temporal_layer_id(value) {
            Some(Self(value))
        } else {
            None
        }
    }

    #[allow(
        dead_code,
        reason = "base-layer operating-point diagnostics are staged until production SVC selection is reachable"
    )]
    #[must_use]
    pub const fn base() -> Self {
        Self(frame_marking::BASE_LAYER_ID)
    }

    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

/// One source operating point selected by room or receiver policy.
///
/// The selected encoding carries the simulcast or spatial choice. The temporal
/// layer ceiling carries the first codec-native SVC dimension that can be
/// projected into transport packet gates when frame-marking metadata is present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceOperatingPoint {
    encoding_id: SourceEncodingId,
    max_temporal_layer_id: SourceTemporalLayerId,
}

impl SourceOperatingPoint {
    #[allow(
        dead_code,
        reason = "operating-point selectors are staged until negotiated temporal metadata is production-reachable"
    )]
    #[must_use]
    pub const fn new(
        encoding_id: SourceEncodingId,
        max_temporal_layer_id: SourceTemporalLayerId,
    ) -> Self {
        Self {
            encoding_id,
            max_temporal_layer_id,
        }
    }

    #[must_use]
    pub const fn encoding_id(self) -> SourceEncodingId {
        self.encoding_id
    }

    #[must_use]
    pub const fn max_temporal_layer_id(self) -> SourceTemporalLayerId {
        self.max_temporal_layer_id
    }
}

/// Publishing user authority attached to a source descriptor.
///
/// The user identifies the logical owner visible to room policy. Connection
/// freshness is tracked by producer and transport indexes, because source
/// descriptors are room-domain metadata rather than async commit guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceOwner {
    user_id: UserId,
}

impl PublishedSourceOwner {
    #[must_use]
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}

/// Consumer-side routing intent for a source.
///
/// Selectors live above packet gates. Room policy can express whether a
/// consumer wants the source open, pinned to a concrete source encoding or left
/// to a room policy bucket. Transport code should receive a projected
/// transport-native gate after this intent is resolved
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceSelector {
    /// No source-level limit has been requested.
    #[default]
    Open,
    /// Select one source encoding by runtime identity.
    Encoding(SourceEncodingId),
    /// Select one source encoding plus a codec-native temporal layer ceiling.
    #[allow(
        dead_code,
        reason = "operating-point selectors stay internal until RFC 9626 metadata negotiation is implemented"
    )]
    OperatingPoint(SourceOperatingPoint),
    /// Defer the concrete encoding choice to room-level policy.
    #[allow(
        dead_code,
        reason = "room-policy selectors are policy input today; the budget planner still resolves them before transport projection"
    )]
    RoomPolicy(SourceRoomPolicySelector),
}

impl SourceSelector {
    #[must_use]
    pub const fn selected_encoding(self) -> Option<SourceEncodingId> {
        match self {
            Self::Encoding(encoding_id) => Some(encoding_id),
            Self::OperatingPoint(operating_point) => Some(operating_point.encoding_id()),
            Self::Open | Self::RoomPolicy(_) => None,
        }
    }

    #[must_use]
    pub const fn selected_operating_point(self) -> Option<SourceOperatingPoint> {
        match self {
            Self::OperatingPoint(operating_point) => Some(operating_point),
            Self::Open | Self::Encoding(_) | Self::RoomPolicy(_) => None,
        }
    }
}

/// Room policy bucket used before policy resolves to a concrete encoding.
///
/// This keeps layout intent out of the transport layer. The room can decide
/// later that a thumbnail should map to a lower simulcast encoding while a
/// featured source stays unconstrained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRoomPolicySelector {
    /// The receiver explicitly pinned this source.
    Pinned,
    /// The receiver explicitly promoted this source to featured treatment.
    Featured,
    /// Screen share keeps readability-focused priority separate from cameras.
    ScreenShare,
    /// The active-speaker snapshot promoted this camera source.
    ActiveSpeaker,
    /// Source is consumed as a visible small tile.
    VisibleThumbnail,
    /// Source is subscribed but not currently visible in the receiver layout.
    Hidden,
    /// Source is outside the visible receiver tile set.
    Overflow,
}

impl SourceRoomPolicySelector {
    #[must_use]
    pub const fn priority(self) -> SourceRoutePriority {
        match self {
            Self::Pinned | Self::Featured => SourceRoutePriority::PinnedOrFeatured,
            Self::ScreenShare => SourceRoutePriority::ScreenShare,
            Self::ActiveSpeaker => SourceRoutePriority::ActiveSpeaker,
            Self::VisibleThumbnail => SourceRoutePriority::VisibleThumbnail,
            Self::Hidden | Self::Overflow => SourceRoutePriority::HiddenOrOverflow,
        }
    }

    #[must_use]
    pub const fn uses_featured_quality(self) -> bool {
        matches!(
            self,
            Self::Pinned | Self::Featured | Self::ScreenShare | Self::ActiveSpeaker
        )
    }

    #[must_use]
    pub const fn counts_toward_visible_budget(self) -> bool {
        !matches!(self, Self::Hidden | Self::Overflow)
    }
}

/// Server-owned policy role for one upload or source encoding layer.
///
/// The browser may use this as sender-parameter metadata, but the room remains
/// authoritative for deciding when a receiver should consume the layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadLayerPolicyRole {
    /// Highest useful quality for featured, pinned, or screen-readable video.
    Featured,
    /// Normal thumbnail quality for visible secondary video.
    Thumbnail,
    /// Future low-cost thumbnail rung used before pausing a route.
    DegradedThumbnail,
}

impl UploadLayerPolicyRole {
    #[must_use]
    pub const fn as_wire_value(self) -> &'static str {
        match self {
            Self::Featured => "featured",
            Self::Thumbnail => "thumbnail",
            Self::DegradedThumbnail => "degradedThumbnail",
        }
    }
}

/// Receiver-side priority bucket used by room policy and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceRoutePriority {
    /// User-pinned or explicitly featured video wins overload decisions.
    PinnedOrFeatured,
    /// Screen share keeps readability ahead of active-speaker camera bias.
    ScreenShare,
    /// Active speaker is important, but still below explicit receiver intent.
    ActiveSpeaker,
    /// Visible camera thumbnails are useful but degradable.
    VisibleThumbnail,
    /// Hidden and overflow videos are first to lose budget in later overload work.
    HiddenOrOverflow,
}

/// Server-owned reason why video policy may withhold a live route.
///
/// The current production policy never emits pause actions; it only selects
/// encodings. The reason vocabulary is defined here so the later budget solver,
/// diagnostics, and recording metadata all describe policy pauses with the same
/// source-domain terms instead of transport drop reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "semantic route pause reasons are reserved for the upcoming budget solver"
)]
pub enum PolicyPauseReason {
    /// The receiver budget cannot fit this route after lower layers were tried.
    BudgetPressure,
    /// The receiver layout says the source is currently hidden.
    HiddenTile,
    /// The receiver layout puts this source outside the visible tile set.
    OverflowTile,
    /// No negotiated encoding or operating point can be forwarded usefully.
    MissingUsableLayer,
}

/// Consumer-side desired state for one published source.
///
/// The active flag is the compatibility-level subscription decision. The
/// selector is the source-level quality intent that later adaptation and
/// layout policy can resolve into a transport-native gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSourceSelection {
    active: bool,
    selector: SourceSelector,
    pressure_observations: u8,
    upgrade_observations: u8,
}

impl ConsumerSourceSelection {
    #[must_use]
    pub const fn open(active: bool) -> Self {
        Self {
            active,
            selector: SourceSelector::Open,
            pressure_observations: 0,
            upgrade_observations: 0,
        }
    }

    #[must_use]
    pub const fn active(self) -> bool {
        self.active
    }

    #[must_use]
    pub const fn selector(self) -> SourceSelector {
        self.selector
    }

    #[must_use]
    pub const fn pressure_observations(self) -> u8 {
        self.pressure_observations
    }

    #[must_use]
    pub const fn upgrade_observations(self) -> u8 {
        self.upgrade_observations
    }

    pub const fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    pub const fn set_selector(&mut self, selector: SourceSelector) {
        self.selector = selector;
    }

    pub const fn set_adaptation_observations(
        &mut self,
        pressure_observations: u8,
        upgrade_observations: u8,
    ) {
        self.pressure_observations = pressure_observations;
        self.upgrade_observations = upgrade_observations;
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
pub struct PublishedSourceDescriptor {
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
            stream_type: parts.stream_type,
            media_kind: parts.media_kind,
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
    pub const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    #[must_use]
    pub const fn media_kind(&self) -> MediaKind {
        self.media_kind
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
    /// Compatibility label used by the existing Odoo-facing API.
    pub stream_type: StreamType,
    /// Router-facing media family for this source.
    pub media_kind: MediaKind,
    /// Negotiated media-section id when known.
    pub mid: Option<Mid>,
    /// Encodings that belong to this source.
    pub encodings: Vec<SourceEncodingDescriptor>,
}

/// Room-domain description of one advertised source encoding.
///
/// This keeps policy identity separate from negotiated transport facts. RID,
/// SSRC, bitrate and codec data are metadata that help route projection,
/// diagnostics and recording describe the encoding; they are not substitutes
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

/// Rejection returned while assembling a source descriptor.
///
/// # Error handling guidance
///
/// These are construction-time domain errors. They should be handled before a
/// publish becomes authoritative in room state. They are not transport
/// failures and should not be retried without rebuilding the source descriptor
/// from valid runtime facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceModelError {
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
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: Some(video_format(96)),
        })
    }

    #[test]
    fn descriptor_keeps_source_encoding_identity_separate() {
        let source_id = PublishedSourceId::from_raw(7);
        let low_encoding_id = SourceEncodingId::from_raw(1);
        let high_encoding_id = SourceEncodingId::from_raw(2);
        let owner = PublishedSourceOwner::new(UserId::Integer(42));
        let descriptor = PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner,
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: Some(Mid::new("video-0")),
            encodings: vec![
                source_encoding(source_id, low_encoding_id, "lo"),
                source_encoding(source_id, high_encoding_id, "hi"),
            ],
        })
        .expect("source descriptor should be valid");

        assert_eq!(descriptor.source_id(), source_id);
        assert_eq!(descriptor.owner().user_id(), &UserId::Integer(42));
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
        assert_eq!(encodings[0].max_temporal_layer_id(), None);
        assert_eq!(
            encodings[0]
                .negotiated_format()
                .map(MediaFormat::payload_type_id),
            Some(PayloadType::new(96))
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
            owner: PublishedSourceOwner::new(UserId::Integer(42)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: None,
            encodings: vec![source_encoding(other_source_id, encoding_id, "lo")],
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
        let temporal_layer = SourceTemporalLayerId::new(2)
            .expect("test temporal layer should fit the RFC 9626 TID range");
        let operating_point = SourceOperatingPoint::new(encoding_id, temporal_layer);

        assert_eq!(
            SourceSelector::Encoding(encoding_id).selected_encoding(),
            Some(encoding_id)
        );
        assert_eq!(
            SourceSelector::OperatingPoint(operating_point).selected_encoding(),
            Some(encoding_id)
        );
        assert_eq!(
            SourceSelector::OperatingPoint(operating_point).selected_operating_point(),
            Some(operating_point)
        );
        assert_eq!(SourceSelector::Open.selected_encoding(), None);
        assert_eq!(
            SourceSelector::RoomPolicy(SourceRoomPolicySelector::VisibleThumbnail)
                .selected_encoding(),
            None
        );
    }

    #[test]
    fn temporal_layer_ids_follow_the_rfc_frame_marking_range() {
        assert_eq!(SourceTemporalLayerId::base().as_u8(), 0);
        assert_eq!(
            SourceTemporalLayerId::new(frame_marking::TEMPORAL_LAYER_ID_MAX)
                .map(SourceTemporalLayerId::as_u8),
            Some(frame_marking::TEMPORAL_LAYER_ID_MAX)
        );
        assert_eq!(
            SourceTemporalLayerId::new(frame_marking::TEMPORAL_LAYER_ID_MAX + 1),
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
