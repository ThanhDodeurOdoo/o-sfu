//! Receiver video layout intent derived from current room state.
//!
//! Layout roles are receiver-specific room policy. The transport layer only
//! sees the selector that this policy later resolves to a packet gate; it never
//! learns whether a route is pinned, screen-share, active-speaker, visible,
//! hidden, or overflow.

use std::collections::BTreeSet;

use super::{
    super::shared::RoomState,
    action::FeaturedUserUpdate,
    input::{
        first_featured_camera_session_for_active_speakers,
        first_featured_camera_sessions_for_active_speakers,
    },
};
use crate::runtime::{
    DownloadStates, StreamType, UserId, VideoLayoutIntent,
    media_transport::ActiveSpeakerSource,
    source_model::{PublishedSourceDescriptor, SourceRoomPolicySelector, SourceRoutePriority},
};

const ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT: usize = 5;

/// Receiver-specific importance of one video source for the current layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::room) struct ReceiverVideoLayoutIntent {
    role: SourceRoomPolicySelector,
}

impl ReceiverVideoLayoutIntent {
    #[must_use]
    pub(in crate::runtime::room) const fn new(role: SourceRoomPolicySelector) -> Self {
        Self { role }
    }

    #[must_use]
    pub(in crate::runtime::room) const fn role(self) -> SourceRoomPolicySelector {
        self.role
    }

    #[must_use]
    pub(in crate::runtime::room) const fn priority(self) -> SourceRoutePriority {
        self.role.priority()
    }

    #[must_use]
    pub(in crate::runtime::room) const fn uses_featured_quality(self) -> bool {
        self.role.uses_featured_quality()
    }

    #[must_use]
    pub(in crate::runtime::room) const fn counts_toward_visible_budget(self) -> bool {
        self.role.counts_toward_visible_budget()
    }

    #[must_use]
    pub(in crate::runtime::room) fn resolve(
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

pub(in crate::runtime::room) fn featured_camera_user_ids_for_active_speakers(
    state: &RoomState,
    active_speaker_sources: &[ActiveSpeakerSource],
) -> BTreeSet<UserId> {
    first_featured_camera_sessions_for_active_speakers(
        state,
        active_speaker_sources,
        ACTIVE_SPEAKER_CAMERA_CLEAR_LIMIT,
    )
}

impl RoomState {
    #[must_use]
    pub(in crate::runtime::room) fn receiver_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
        active_speaker_camera_user_ids: &BTreeSet<UserId>,
    ) -> ReceiverVideoLayoutIntent {
        let preference = self
            .users
            .get(consumer_user_id)
            .and_then(|user| user.desired_download_states.get(source.owner().user_id()))
            .and_then(|states| layout_preference_for_stream_type(states, source.stream_type()));
        ReceiverVideoLayoutIntent::resolve(
            source.stream_type(),
            preference,
            active_speaker_camera_user_ids.contains(source.owner().user_id()),
        )
    }

    #[must_use]
    pub(in crate::runtime::room) fn diagnostics_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
    ) -> Option<ReceiverVideoLayoutIntent> {
        if !matches!(
            source.stream_type(),
            StreamType::Camera | StreamType::Screen
        ) {
            return None;
        }
        let active_speaker_camera_user_ids = self
            .users
            .iter()
            .filter(|(_user_id, user)| user.layout.featured() == Some(true))
            .map(|(user_id, _session)| user_id.clone())
            .collect();
        Some(self.receiver_video_layout_intent(
            consumer_user_id,
            source,
            &active_speaker_camera_user_ids,
        ))
    }

    /// Plans public featured-state changes from the active-speaker snapshot.
    ///
    /// The returned updates are committed together with source-policy effects
    /// so the user-visible `isFeatured` projection and the quality floor come
    /// from the same observation. If there is no current active speaker, the
    /// method only emits clears when some user still has server-derived
    /// featured state.
    pub(in crate::runtime::room) fn featured_session_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<FeaturedUserUpdate> {
        let desired_featured_user_id =
            first_featured_camera_session_for_active_speakers(self, active_speaker_sources);
        let should_clear_featured_state = desired_featured_user_id.is_none()
            && self
                .users
                .values()
                .any(|user| user.layout.featured().is_some());
        if desired_featured_user_id.is_none() && !should_clear_featured_state {
            return Vec::new();
        }
        self.users
            .iter()
            .filter_map(|(user_id, user)| {
                let desired_featured = desired_featured_user_id.as_ref().map_or_else(
                    || user.layout.featured().is_some().then_some(false),
                    |featured_user_id| Some(featured_user_id == user_id),
                );
                (desired_featured != user.layout.featured())
                    .then(|| FeaturedUserUpdate::new(user_id.clone(), desired_featured))
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
