//! Receiver video layout intent derived from current room state.
//!
//! The current implementation has only two server-derived roles: featured
//! camera and thumbnail camera. Keeping that role explicit gives later manual
//! pinning, hidden tiles, and overflow layout policy one owner without teaching
//! the RTC route-control layer about room UI semantics.

use std::collections::BTreeSet;

use o_sfu_protocol::shared::SessionId;

use super::{
    super::shared::ChannelState,
    action::FeaturedSessionUpdate,
    input::{
        first_featured_camera_session_for_active_speakers,
        first_featured_camera_sessions_for_active_speakers,
    },
};
use crate::runtime::transport_adapter::ActiveSpeakerSource;

const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

/// Receiver-specific importance of one video source for the current layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) enum ReceiverVideoLayoutIntent {
    /// The route should receive featured quality treatment.
    Featured,
    /// The route is currently treated as a multiparty thumbnail.
    Thumbnail,
}

impl ReceiverVideoLayoutIntent {
    #[must_use]
    pub(in crate::runtime::channel) const fn is_featured(self) -> bool {
        matches!(self, Self::Featured)
    }
}

pub(in crate::runtime::channel) fn featured_camera_session_ids_for_active_speakers(
    state: &ChannelState,
    active_speaker_sources: &[ActiveSpeakerSource],
) -> BTreeSet<SessionId> {
    first_featured_camera_sessions_for_active_speakers(
        state,
        active_speaker_sources,
        ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT,
    )
}

impl ChannelState {
    /// Plans public featured-state changes from the active-speaker snapshot.
    ///
    /// The returned updates are committed together with source-policy effects
    /// so the user-visible `isFeatured` projection and the quality floor come
    /// from the same observation. If there is no current active speaker, the
    /// method only emits clears when some session still has server-derived
    /// featured state.
    pub(in crate::runtime::channel) fn featured_session_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<FeaturedSessionUpdate> {
        let desired_featured_session_id =
            first_featured_camera_session_for_active_speakers(self, active_speaker_sources);
        let should_clear_featured_state = desired_featured_session_id.is_none()
            && self
                .sessions
                .values()
                .any(|session| session.layout.featured().is_some());
        if desired_featured_session_id.is_none() && !should_clear_featured_state {
            return Vec::new();
        }
        self.sessions
            .iter()
            .filter_map(|(session_id, session)| {
                let desired_featured = desired_featured_session_id.as_ref().map_or_else(
                    || session.layout.featured().is_some().then_some(false),
                    |featured_session_id| Some(featured_session_id == session_id),
                );
                (desired_featured != session.layout.featured())
                    .then(|| FeaturedSessionUpdate::new(session_id.clone(), desired_featured))
            })
            .collect()
    }
}
