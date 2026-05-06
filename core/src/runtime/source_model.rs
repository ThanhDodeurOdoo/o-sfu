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
//! A published media stream is modeled as one [`PublishedSourceId`] plus one or
//! more [`SourceEncodingId`] values. `Mid`, `Rid` and `Ssrc` stay as negotiated
//! or transport-facing attachment points. Keeping those identities separate
//! lets later same-room spillover and recording consume the same source
//! inventory without redefining it around local worker placement.
//!
//! Business layers should express stream-specific behavior by constructing
//! [`SourcePublishIntent`] values. Core policy reads the generic
//! [`SourcePolicy`] carried by each source, never compatibility stream labels.
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
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::runtime::{UserId, VideoLayoutIntent};

/// Orchestration-owned stream identity inside one user's source inventory.
///
/// The room treats this value as an opaque slot name scoped by the publishing
/// user. Compatibility layers and future orchestrators allocate the stable
/// stream ids and attach media and policy metadata separately.
///
/// # Invariant
///
/// `UserStreamId` is not globally unique. It is unique only together with the
/// owning user id. Room indexes that need publication identity must pair it with
/// the owner or use [`PublishedSourceId`] after a publish commits.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserStreamId(String);

impl UserStreamId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for UserStreamId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for UserStreamId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for UserStreamId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[cfg(any(test, feature = "testing-transport"))]
#[path = "source_model/test_support.rs"]
pub(crate) mod test_support;

/// Orchestration-provided publish intent for one user stream.
///
/// This is the API business-layer code should pass into core when a user starts
/// publishing. It carries the stream identity, technical media kind and room
/// policy as one immutable decision. Core captures these values when the staged
/// publish commits.
///
/// # Boundary role
///
/// Compatibility concepts such as "camera" or "screen" must be translated into
/// this type before entering core. If a product stream needs different layout
/// or bandwidth behavior, change the orchestration catalog that builds this
/// intent instead of adding stream-specific branches to room state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePublishIntent {
    stream_id: UserStreamId,
    media_kind: MediaKind,
    policy: SourcePolicy,
}

impl SourcePublishIntent {
    #[must_use]
    pub fn new(stream_id: UserStreamId, media_kind: MediaKind, policy: SourcePolicy) -> Self {
        Self {
            stream_id,
            media_kind,
            policy,
        }
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
}

/// Room policy metadata supplied by orchestration for one source.
///
/// `SourcePolicy` is the generic contract between business orchestration and
/// core room policy. It tells core what it may do with a source after publish,
/// but it does not name the product feature that created the stream and it does
/// not limit how many streams a user may publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePolicy {
    layout: Option<SourceLayoutPolicy>,
    adaptation: SourceAdaptationPolicy,
    active_speaker: Option<ActiveSpeakerPolicy>,
}

impl SourcePolicy {
    #[must_use]
    pub const fn new(
        layout: Option<SourceLayoutPolicy>,
        adaptation: SourceAdaptationPolicy,
        active_speaker: Option<ActiveSpeakerPolicy>,
    ) -> Self {
        Self {
            layout,
            adaptation,
            active_speaker,
        }
    }

    #[must_use]
    pub const fn hidden() -> Self {
        Self::new(None, SourceAdaptationPolicy::None, None)
    }

    #[must_use]
    pub const fn layout(self) -> Option<SourceLayoutPolicy> {
        self.layout
    }

    #[must_use]
    pub const fn adaptation(self) -> SourceAdaptationPolicy {
        self.adaptation
    }

    #[must_use]
    pub const fn active_speaker(self) -> Option<ActiveSpeakerPolicy> {
        self.active_speaker
    }
}

/// Default receiver-layout role for one source.
///
/// Orchestration sets this when a source is published. Core combines it with a
/// receiver's explicit layout preference and the active-speaker snapshot to
/// choose a [`SourceRoomPolicySelector`] for each receiver/source route.
///
/// If a source has no layout policy, core treats it as hidden for video budget
/// planning. That is useful for audio and for future sources that should route
/// without entering receiver video layout decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLayoutPolicy {
    visible_selector: SourceRoomPolicySelector,
    active_speaker_selector: Option<SourceRoomPolicySelector>,
}

impl SourceLayoutPolicy {
    #[must_use]
    pub const fn new(
        visible_selector: SourceRoomPolicySelector,
        active_speaker_selector: Option<SourceRoomPolicySelector>,
    ) -> Self {
        Self {
            visible_selector,
            active_speaker_selector,
        }
    }

    #[must_use]
    pub fn resolve(
        self,
        preference: Option<VideoLayoutIntent>,
        active_speaker: bool,
    ) -> SourceRoomPolicySelector {
        match preference {
            Some(VideoLayoutIntent::Pinned) => SourceRoomPolicySelector::Pinned,
            Some(VideoLayoutIntent::Featured) => SourceRoomPolicySelector::Featured,
            Some(VideoLayoutIntent::Hidden) => SourceRoomPolicySelector::Hidden,
            Some(VideoLayoutIntent::Overflow) => SourceRoomPolicySelector::Overflow,
            None if active_speaker => self
                .active_speaker_selector
                .unwrap_or(self.visible_selector),
            Some(VideoLayoutIntent::VisibleThumbnail) | None => self.visible_selector,
        }
    }
}

/// Receiver bandwidth behavior for one published source.
///
/// Orchestration chooses this once, when building [`SourcePublishIntent`]. Core
/// then uses it to decide whether the source participates in receiver-video
/// layer selection, route pausing and over-budget diagnostics.
///
/// # Example situations
///
/// Use [`Self::None`] for audio or metadata-like sources that should not spend
/// receiver video budget. Use [`Self::ScalableVideo`] for video with cheap and
/// high-quality encodings. Use [`Self::ReadableDetail`] when the receiver must
/// keep enough detail to inspect text or other fine visual content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAdaptationPolicy {
    /// Keep this source out of receiver-video BWE control.
    ///
    /// Core may still route the source through normal subscriptions. The
    /// budget planner does not choose a video encoding for it, count it in the
    /// receiver video budget or pause it because of BWE pressure.
    ///
    /// Example: a speech detector source can drive active-speaker state without
    /// entering video-layer selection.
    None,
    /// Let the receiver-video planner choose among advertised encodings.
    ///
    /// The planner can pick a lower encoding for thumbnail routes, request a
    /// keyframe after the selected encoding changes and pause non-protected
    /// routes when the receiver budget cannot carry all selected video.
    ///
    /// Example: a two-layer video source can use the high layer while featured
    /// and the low layer while shown as a secondary tile.
    ScalableVideo,
    /// Keep readable detail ahead of normal thumbnail adaptation.
    ///
    /// The planner targets the highest advertised encoding. Routes with this
    /// policy are protected from normal overload pauses, so diagnostics report
    /// a protected over-budget exception if they keep the receiver above BWE.
    ///
    /// Example: a text-heavy visual source should stay on the readable encoding
    /// even when ordinary thumbnails are degraded or paused.
    ReadableDetail,
}

/// Active-speaker relationship supplied by orchestration.
///
/// Core receives transport active-speaker observations, but orchestration
/// decides which sources participate in that relationship. Audio-like sources
/// normally detect speech. Video-like sources may be promotable by speech from
/// another source in the same group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerPolicy {
    group: ActiveSpeakerGroup,
    role: ActiveSpeakerSourceRole,
}

impl ActiveSpeakerPolicy {
    #[must_use]
    pub const fn new(group: ActiveSpeakerGroup, role: ActiveSpeakerSourceRole) -> Self {
        Self { group, role }
    }

    #[must_use]
    pub const fn group(self) -> ActiveSpeakerGroup {
        self.group
    }

    #[must_use]
    pub const fn role(self) -> ActiveSpeakerSourceRole {
        self.role
    }
}

/// Orchestration-owned active-speaker group id.
///
/// Groups let future orchestrators keep unrelated speech domains separate
/// without teaching core about product stream names. The current Odoo
/// compatibility layer uses [`Self::MAIN`] for the call's normal audio and
/// camera relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerGroup(u16);

impl ActiveSpeakerGroup {
    pub const MAIN: Self = Self(0);
}

/// Role one source plays inside an active-speaker group.
///
/// # Example situation
///
/// A speech source and a video source can share [`ActiveSpeakerGroup::MAIN`].
/// The speech source uses [`Self::Detector`]. The video source uses
/// [`Self::Promotable`], so speech observations can promote that user's video
/// route for receivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerSourceRole {
    /// Transport observations from this source can mark its owner active.
    ///
    /// A detector is usually an audio-like source. It does not receive video
    /// layout treatment by itself.
    Detector,
    /// This source can receive active-speaker video treatment for its owner.
    ///
    /// Core promotes a promotable source only when a detector in the same group
    /// marks the same owner as active.
    Promotable,
}

/// Per-source subscription update supplied by orchestration.
///
/// This is the generic core shape for receiver download intent. Business
/// compatibility code decides which stream ids to include. Core merges partial
/// updates by stream id and applies the resulting active or layout preference
/// to existing and future consumer routes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceSubscriptionIntent {
    active: Option<bool>,
    layout: Option<VideoLayoutIntent>,
}

impl SourceSubscriptionIntent {
    #[must_use]
    pub const fn new(active: Option<bool>, layout: Option<VideoLayoutIntent>) -> Self {
        Self { active, layout }
    }

    #[must_use]
    pub const fn active(self) -> Option<bool> {
        self.active
    }

    #[must_use]
    pub const fn layout(self) -> Option<VideoLayoutIntent> {
        self.layout
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.active.is_none() && self.layout.is_none()
    }

    pub const fn merge(&mut self, update: Self) {
        if update.active.is_some() {
            self.active = update.active;
        }
        if update.layout.is_some() {
            self.layout = update.layout;
        }
    }
}

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

/// Resolved packet-selection command for one consumer/source route.
///
/// The budget planner writes selectors into room state. A later projection step
/// turns them into transport packet gates such as "open" or "forward this RID".
/// Transport code should never receive [`Self::RoomPolicy`], because that value
/// still needs room-level policy resolution.
///
/// # Example situations
///
/// [`Self::Open`] means the route has no source-level packet gate.
/// [`Self::Encoding`] means "forward the negotiated RID for this encoding".
/// [`Self::OperatingPoint`] means "forward this encoding up to this temporal
/// layer". [`Self::RoomPolicy`] means "this route is a visible thumbnail" or
/// another room role that must still be resolved before transport sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceSelector {
    /// Forward the source without a source-level packet gate.
    ///
    /// This is the default for sources that are not controlled by receiver-video
    /// adaptation or when the planner has not selected a narrower gate.
    #[default]
    Open,
    /// Forward only one advertised source encoding.
    ///
    /// Projection maps the encoding id to its negotiated RID. If the encoding
    /// has no RID, projection fails rather than guessing at packet identity.
    Encoding(SourceEncodingId),
    /// Forward one encoding up to a codec-native temporal layer ceiling.
    ///
    /// Projection requires advertised temporal metadata and rejects a selector
    /// whose temporal ceiling is higher than the source declared.
    #[allow(
        dead_code,
        reason = "operating-point selectors stay internal until RFC 9626 metadata negotiation is implemented"
    )]
    OperatingPoint(SourceOperatingPoint),
    /// Keep the route in a named room-policy bucket.
    ///
    /// This is valid as policy input and diagnostics vocabulary. It must be
    /// resolved to [`Self::Open`], [`Self::Encoding`] or [`Self::OperatingPoint`]
    /// before transport packet-gate projection.
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

/// Receiver-specific layout role before a concrete encoding is chosen.
///
/// Layout code produces this role for each receiver/source route. The budget
/// planner reads it to decide quality targets, overload protection and pause
/// order. Transport code sees only the final packet gate after this role has
/// been resolved.
///
/// # Example situations
///
/// A receiver action can produce [`Self::Pinned`] or [`Self::Featured`]. Source
/// policy can produce [`Self::ReadableDetail`]. Active-speaker state can produce
/// [`Self::ActiveSpeaker`]. Default visible video often starts as
/// [`Self::VisibleThumbnail`], while receiver layout can move a subscribed
/// route to [`Self::Hidden`] or [`Self::Overflow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRoomPolicySelector {
    /// The receiver explicitly pinned this source.
    ///
    /// Pinned routes use featured quality and are protected from overload
    /// pauses. They share the highest budget priority with featured routes.
    Pinned,
    /// The receiver explicitly requested featured treatment for this source.
    ///
    /// Featured routes use featured quality and are protected from overload
    /// pauses. They share the highest budget priority with pinned routes.
    Featured,
    /// Source policy says readable detail matters for this route.
    ///
    /// Readable-detail routes use featured quality and are protected from
    /// overload pauses, but explicit pinned or featured receiver intent outranks
    /// them.
    ReadableDetail,
    /// Active-speaker policy promoted this source for the current receiver.
    ///
    /// Active-speaker routes use featured quality and are protected from
    /// overload pauses. Explicit receiver intent and readable detail outrank
    /// this role.
    ActiveSpeaker,
    /// The source is visible as a secondary tile.
    ///
    /// Visible thumbnails count toward the receiver's visible-video budget.
    /// They can downswitch to cheaper encodings and can be paused if protected
    /// routes still leave the receiver over budget.
    VisibleThumbnail,
    /// The receiver is subscribed but the source is not visible right now.
    ///
    /// Hidden routes do not count toward the visible-video budget. They are
    /// first-class diagnostics and first candidates for overload pause.
    Hidden,
    /// The source is outside the receiver's visible tile set.
    ///
    /// Overflow routes behave like hidden routes for budget purposes, but keep
    /// a distinct pause reason so diagnostics can explain the layout decision.
    Overflow,
}

impl SourceRoomPolicySelector {
    #[must_use]
    pub const fn priority(self) -> SourceRoutePriority {
        match self {
            Self::Pinned | Self::Featured => SourceRoutePriority::PinnedOrFeatured,
            Self::ReadableDetail => SourceRoutePriority::ReadableDetail,
            Self::ActiveSpeaker => SourceRoutePriority::ActiveSpeaker,
            Self::VisibleThumbnail => SourceRoutePriority::VisibleThumbnail,
            Self::Hidden | Self::Overflow => SourceRoutePriority::HiddenOrOverflow,
        }
    }

    #[must_use]
    pub const fn uses_featured_quality(self) -> bool {
        matches!(
            self,
            Self::Pinned | Self::Featured | Self::ReadableDetail | Self::ActiveSpeaker
        )
    }

    #[must_use]
    pub const fn counts_toward_visible_budget(self) -> bool {
        !matches!(self, Self::Hidden | Self::Overflow)
    }
}

/// Server-owned role for one advertised upload encoding.
///
/// The role lets the budget planner understand what an encoding is meant for
/// without reading product stream names. It is advertised to the browser as
/// sender metadata and later copied into the source descriptor after negotiation.
///
/// # Example situation
///
/// A two-layer video offer can mark the high layer as [`Self::Featured`] and the
/// low layer as [`Self::Thumbnail`]. The budget planner can then choose the low
/// layer for a secondary tile without knowing why the product created the
/// source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadLayerPolicyRole {
    /// Highest useful quality for protected or detail-focused routes.
    ///
    /// The planner avoids using this as the first cheap fallback for thumbnail
    /// routes when a lower-cost encoding exists.
    Featured,
    /// Normal quality target for visible secondary video.
    ///
    /// This is the expected low-cost encoding for thumbnail routes before the
    /// planner considers pausing the route.
    Thumbnail,
    /// Lower-cost thumbnail rung below the normal thumbnail target.
    ///
    /// This is reserved for a future upload ladder where the server advertises
    /// more than two useful video encodings.
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

/// Overload priority derived from a route's room-policy selector.
///
/// The receiver budget planner uses this ordering when it has already tried
/// cheaper encodings and still needs to pause routes. Lower-priority buckets are
/// paused first. Protected buckets are not paused by normal overload handling.
///
/// # Example situation
///
/// When selected video exceeds the receiver BWE, hidden and overflow routes are
/// paused before visible thumbnails. Active-speaker, readable-detail and pinned
/// or featured routes stay protected, so they can produce a protected
/// over-budget diagnostic instead of a pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceRoutePriority {
    /// Explicit receiver intent.
    ///
    /// Pinned and featured routes are protected and outrank every other route.
    PinnedOrFeatured,
    /// Detail-preserving source policy.
    ///
    /// Readable-detail routes are protected. They outrank active-speaker routes
    /// but stay below explicit pinned or featured receiver intent.
    ReadableDetail,
    /// Server-promoted active-speaker route.
    ///
    /// Active-speaker routes are protected. Explicit receiver intent and
    /// readable-detail source policy outrank them.
    ActiveSpeaker,
    /// Visible secondary route.
    ///
    /// Visible thumbnails can be downswitched and then paused if protected
    /// routes still leave the receiver over budget.
    VisibleThumbnail,
    /// Route that is not currently visible.
    ///
    /// Hidden and overflow routes are first to pause under receiver budget
    /// pressure.
    HiddenOrOverflow,
}

/// Reason why room policy withholds media for a subscribed route.
///
/// This is separate from subscription state. A receiver can remain subscribed
/// while core temporarily closes the packet gate for budget or layout reasons.
///
/// # Example situations
///
/// [`Self::HiddenTile`] means layout hid the source. [`Self::OverflowTile`]
/// means layout pushed it outside the visible tile set. [`Self::BudgetPressure`]
/// means the route was useful, but the receiver BWE could not fit it after
/// cheaper layers were tried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyPauseReason {
    /// The receiver budget cannot fit this route after cheaper layers were tried.
    BudgetPressure,
    /// The receiver layout explicitly hides this source.
    HiddenTile,
    /// The receiver layout puts this source outside the visible tile set.
    OverflowTile,
    /// No negotiated encoding or operating point can be forwarded usefully.
    MissingUsableLayer,
}

/// Reason selected video is allowed to exceed the receiver bandwidth estimate.
///
/// # Example situation
///
/// After thumbnails and hidden routes have been degraded or paused, a pinned or
/// readable-detail route can still exceed BWE. Diagnostics use this reason to
/// show that the over-budget state came from protected room policy rather than
/// from a missing pause decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverBudgetExceptionReason {
    /// The remaining over-budget routes are protected by room policy.
    ///
    /// This means the planner already degraded or paused every non-protected
    /// route it could, but protected routes still exceed the latest BWE.
    ProtectedRoute,
}

/// Latest receiver-level budget facts attached to a source selection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiverVideoBudgetDiagnostics {
    latest_receiver_bandwidth_bps: Option<u64>,
    selected_video_budget_bps: Option<u64>,
    active_video_route_count: usize,
    selected_video_bitrate_bps: u64,
    over_budget_exception_reason: Option<OverBudgetExceptionReason>,
}

impl ReceiverVideoBudgetDiagnostics {
    #[must_use]
    pub const fn new(
        latest_receiver_bandwidth_bps: Option<u64>,
        selected_video_budget_bps: Option<u64>,
        active_video_route_count: usize,
        selected_video_bitrate_bps: u64,
        over_budget_exception_reason: Option<OverBudgetExceptionReason>,
    ) -> Self {
        Self {
            latest_receiver_bandwidth_bps,
            selected_video_budget_bps,
            active_video_route_count,
            selected_video_bitrate_bps,
            over_budget_exception_reason,
        }
    }

    #[must_use]
    pub const fn latest_receiver_bandwidth_bps(self) -> Option<u64> {
        self.latest_receiver_bandwidth_bps
    }

    #[must_use]
    pub const fn selected_video_budget_bps(self) -> Option<u64> {
        self.selected_video_budget_bps
    }

    #[must_use]
    pub const fn active_video_route_count(self) -> usize {
        self.active_video_route_count
    }

    #[must_use]
    pub const fn selected_video_bitrate_bps(self) -> u64 {
        self.selected_video_bitrate_bps
    }

    #[must_use]
    pub const fn over_budget_exception_reason(self) -> Option<OverBudgetExceptionReason> {
        self.over_budget_exception_reason
    }
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
    policy_pause_reason: Option<PolicyPauseReason>,
    budget: ReceiverVideoBudgetDiagnostics,
    pressure_observations: u8,
    upgrade_observations: u8,
}

impl ConsumerSourceSelection {
    #[must_use]
    pub const fn open(active: bool) -> Self {
        Self {
            active,
            selector: SourceSelector::Open,
            policy_pause_reason: None,
            budget: ReceiverVideoBudgetDiagnostics::new(None, None, 0, 0, None),
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
    pub const fn policy_pause_reason(self) -> Option<PolicyPauseReason> {
        self.policy_pause_reason
    }

    #[must_use]
    pub const fn policy_allows_delivery(self) -> bool {
        self.policy_pause_reason.is_none()
    }

    #[must_use]
    pub const fn budget(self) -> ReceiverVideoBudgetDiagnostics {
        self.budget
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

    pub const fn set_policy_pause_reason(&mut self, reason: Option<PolicyPauseReason>) {
        self.policy_pause_reason = reason;
    }

    pub const fn set_budget(&mut self, budget: ReceiverVideoBudgetDiagnostics) {
        self.budget = budget;
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
            stream_id: UserStreamId::new("main-video"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::hidden(),
            mid: Some(Mid::new("video-0")),
            encodings: vec![
                source_encoding(source_id, low_encoding_id, "lo"),
                source_encoding(source_id, high_encoding_id, "hi"),
            ],
        })
        .expect("source descriptor should be valid");

        assert_eq!(descriptor.source_id(), source_id);
        assert_eq!(descriptor.owner().user_id(), &UserId::Integer(42));
        assert_eq!(descriptor.stream_id().as_str(), "main-video");
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
            stream_id: UserStreamId::new("main-video"),
            media_kind: MediaKind::Video,
            policy: SourcePolicy::hidden(),
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
