use o_sfu_router::MediaKind;

use super::{SourcePolicy, UserStreamId};
use crate::runtime::VideoLayoutIntent;

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
