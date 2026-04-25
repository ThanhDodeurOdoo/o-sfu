//! Receiver video layout intent derived from current room state.
//!
//! Layout roles are receiver-specific room policy. The transport layer only
//! sees the selector that this policy later resolves to a packet gate; it never
//! learns whether a route is pinned, screen-share, active-speaker, visible,
//! hidden, or overflow.

use std::collections::BTreeSet;

use o_sfu_protocol::shared::{DownloadStates, SessionId, StreamType, VideoLayoutIntent};

use super::{
    super::shared::ChannelState,
    action::FeaturedSessionUpdate,
    input::{
        first_featured_camera_session_for_active_speakers,
        first_featured_camera_sessions_for_active_speakers,
    },
};
use crate::runtime::{
    source_model::{PublishedSourceDescriptor, SourceRoomPolicySelector, SourceRoutePriority},
    transport_adapter::ActiveSpeakerSource,
};

const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

/// Receiver-specific importance of one video source for the current layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::channel) struct ReceiverVideoLayoutIntent {
    role: SourceRoomPolicySelector,
}

impl ReceiverVideoLayoutIntent {
    #[must_use]
    pub(in crate::runtime::channel) const fn new(role: SourceRoomPolicySelector) -> Self {
        Self { role }
    }

    #[must_use]
    pub(in crate::runtime::channel) const fn role(self) -> SourceRoomPolicySelector {
        self.role
    }

    #[must_use]
    pub(in crate::runtime::channel) const fn priority(self) -> SourceRoutePriority {
        self.role.priority()
    }

    #[must_use]
    pub(in crate::runtime::channel) const fn uses_featured_quality(self) -> bool {
        self.role.uses_featured_quality()
    }

    #[must_use]
    pub(in crate::runtime::channel) const fn counts_toward_visible_budget(self) -> bool {
        self.role.counts_toward_visible_budget()
    }

    #[must_use]
    pub(in crate::runtime::channel) fn resolve(
        stream_type: StreamType,
        preference: Option<VideoLayoutIntent>,
        active_speaker: bool,
    ) -> Self {
        let role = match stream_type {
            StreamType::Camera => preference.map_or_else(
                || {
                    if active_speaker {
                        SourceRoomPolicySelector::ActiveSpeaker
                    } else {
                        SourceRoomPolicySelector::VisibleThumbnail
                    }
                },
                explicit_camera_layout_role,
            ),
            StreamType::Screen => preference.map_or(
                SourceRoomPolicySelector::ScreenShare,
                explicit_screen_layout_role,
            ),
            StreamType::Audio => SourceRoomPolicySelector::Hidden,
        };
        Self::new(role)
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
    #[must_use]
    pub(in crate::runtime::channel) fn receiver_video_layout_intent(
        &self,
        consumer_session_id: &SessionId,
        source: &PublishedSourceDescriptor,
        active_speaker_camera_session_ids: &BTreeSet<SessionId>,
    ) -> ReceiverVideoLayoutIntent {
        let preference = self
            .sessions
            .get(consumer_session_id)
            .and_then(|session| {
                session
                    .desired_download_states
                    .get(source.owner().session_id())
            })
            .and_then(|states| layout_preference_for_stream_type(states, source.stream_type()));
        ReceiverVideoLayoutIntent::resolve(
            source.stream_type(),
            preference,
            active_speaker_camera_session_ids.contains(source.owner().session_id()),
        )
    }

    #[must_use]
    pub(in crate::runtime::channel) fn diagnostics_video_layout_intent(
        &self,
        consumer_session_id: &SessionId,
        source: &PublishedSourceDescriptor,
    ) -> Option<ReceiverVideoLayoutIntent> {
        if !matches!(
            source.stream_type(),
            StreamType::Camera | StreamType::Screen
        ) {
            return None;
        }
        let active_speaker_camera_session_ids = self
            .sessions
            .iter()
            .filter(|(_session_id, session)| session.layout.featured() == Some(true))
            .map(|(session_id, _session)| session_id.clone())
            .collect();
        Some(self.receiver_video_layout_intent(
            consumer_session_id,
            source,
            &active_speaker_camera_session_ids,
        ))
    }

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

fn layout_preference_for_stream_type(
    states: &DownloadStates,
    stream_type: StreamType,
) -> Option<VideoLayoutIntent> {
    match stream_type {
        StreamType::Audio => None,
        StreamType::Camera => states.camera_layout,
        StreamType::Screen => states.screen_layout,
    }
}

const fn explicit_camera_layout_role(preference: VideoLayoutIntent) -> SourceRoomPolicySelector {
    match preference {
        VideoLayoutIntent::Pinned => SourceRoomPolicySelector::Pinned,
        VideoLayoutIntent::Featured => SourceRoomPolicySelector::Featured,
        VideoLayoutIntent::VisibleThumbnail => SourceRoomPolicySelector::VisibleThumbnail,
        VideoLayoutIntent::Hidden => SourceRoomPolicySelector::Hidden,
        VideoLayoutIntent::Overflow => SourceRoomPolicySelector::Overflow,
    }
}

const fn explicit_screen_layout_role(preference: VideoLayoutIntent) -> SourceRoomPolicySelector {
    match preference {
        VideoLayoutIntent::Pinned => SourceRoomPolicySelector::Pinned,
        VideoLayoutIntent::Featured => SourceRoomPolicySelector::Featured,
        VideoLayoutIntent::VisibleThumbnail => SourceRoomPolicySelector::ScreenShare,
        VideoLayoutIntent::Hidden => SourceRoomPolicySelector::Hidden,
        VideoLayoutIntent::Overflow => SourceRoomPolicySelector::Overflow,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_speaker_is_default_camera_intent_without_explicit_layout() {
        let intent = ReceiverVideoLayoutIntent::resolve(StreamType::Camera, None, true);

        assert_eq!(intent.role(), SourceRoomPolicySelector::ActiveSpeaker);
        assert_eq!(intent.priority(), SourceRoutePriority::ActiveSpeaker);
        assert!(intent.uses_featured_quality());
    }

    #[test]
    fn pinned_camera_intent_overrides_active_speaker_default() {
        let intent = ReceiverVideoLayoutIntent::resolve(
            StreamType::Camera,
            Some(VideoLayoutIntent::Pinned),
            false,
        );

        assert_eq!(intent.role(), SourceRoomPolicySelector::Pinned);
        assert_eq!(intent.priority(), SourceRoutePriority::PinnedOrFeatured);
        assert!(intent.uses_featured_quality());
    }

    #[test]
    fn explicit_hidden_camera_does_not_use_featured_quality_even_when_speaking() {
        let intent = ReceiverVideoLayoutIntent::resolve(
            StreamType::Camera,
            Some(VideoLayoutIntent::Hidden),
            true,
        );

        assert_eq!(intent.role(), SourceRoomPolicySelector::Hidden);
        assert_eq!(intent.priority(), SourceRoutePriority::HiddenOrOverflow);
        assert!(!intent.uses_featured_quality());
        assert!(!intent.counts_toward_visible_budget());
    }

    #[test]
    fn screen_share_has_screen_specific_priority_by_default() {
        let intent = ReceiverVideoLayoutIntent::resolve(StreamType::Screen, None, false);

        assert_eq!(intent.role(), SourceRoomPolicySelector::ScreenShare);
        assert_eq!(intent.priority(), SourceRoutePriority::ScreenShare);
        assert!(intent.uses_featured_quality());
    }
}
