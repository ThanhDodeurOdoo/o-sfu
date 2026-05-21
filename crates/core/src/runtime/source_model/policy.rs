use crate::runtime::VideoLayoutIntent;

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
/// Orchestration chooses this once, when building
/// [`SourcePublishIntent`](crate::prelude::SourcePublishIntent). Core then uses it to
/// decide whether the source participates in receiver-video layer selection,
/// route pausing and over-budget diagnostics.
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
/// cheaper layers were tried. Hard activation caps use dedicated reasons so
/// operators can distinguish them from bandwidth policy.
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
    /// The active-audio-speaker cap withheld this route.
    AudioSpeakerLimit,
    /// The per-receiver live-video cap withheld this route.
    VideoDownloadLimit,
}

/// Server-owned role for one published source encoding.
///
/// The role lets the budget planner understand what an encoding is meant for
/// without reading product stream names. Room state assigns it when committing
/// the source descriptor after transport negotiation.
///
/// # Example situation
///
/// A two-layer video source can mark the high layer as [`Self::Featured`] and
/// the low layer as [`Self::Thumbnail`]. The budget planner can then choose the
/// low layer for a secondary tile without knowing why the product created the
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
