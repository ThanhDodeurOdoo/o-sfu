//! Receiver video layout intent derived from current room state.
//!
//! Layout roles are receiver-specific room policy. The transport layer only
//! sees the selector that this policy later resolves to a packet gate; it never
//! learns whether a route is pinned, active-speaker, visible,
//! hidden, or overflow.

use std::collections::BTreeSet;

use super::{
    super::{super::state::RoomState, action::FeaturedUserUpdate},
    input::{
        first_featured_source_user_for_active_speakers,
        first_featured_source_users_for_active_speakers,
    },
};
use crate::engine::{
    UserId, VideoLayoutIntent,
    media_transport::ActiveSpeakerSource,
    source_model::{
        PublishedSourceDescriptor, PublishedSourceId, SourceRoomPolicySelector, SourceRoutePriority,
    },
};

const ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT: usize = 5;

/// Receiver-specific importance of one video source for the current layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::room) struct ReceiverVideoLayoutIntent {
    role: SourceRoomPolicySelector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::engine::room) struct VideoAdmissionRank {
    priority: u8,
    active_speaker_rank: usize,
    source_id: u64,
}

impl ReceiverVideoLayoutIntent {
    #[must_use]
    pub const fn new(role: SourceRoomPolicySelector) -> Self {
        Self { role }
    }

    #[must_use]
    pub const fn role(self) -> SourceRoomPolicySelector {
        self.role
    }

    #[must_use]
    pub const fn priority(self) -> SourceRoutePriority {
        self.role.priority()
    }

    #[must_use]
    pub const fn uses_featured_quality(self) -> bool {
        self.role.uses_featured_quality()
    }

    #[must_use]
    pub const fn counts_toward_visible_budget(self) -> bool {
        self.role.counts_toward_visible_budget()
    }

    #[must_use]
    pub fn resolve(
        source: &PublishedSourceDescriptor,
        preference: Option<VideoLayoutIntent>,
        active_speaker: bool,
    ) -> Self {
        let role = source
            .policy()
            .layout()
            .map_or(SourceRoomPolicySelector::Hidden, |policy| {
                policy.resolve(preference, active_speaker)
            });
        Self::new(role)
    }
}

impl VideoAdmissionRank {
    pub const fn new(
        priority: SourceRoutePriority,
        active_speaker_rank: Option<usize>,
        source_id: PublishedSourceId,
    ) -> Self {
        Self {
            priority: video_admission_priority(priority),
            active_speaker_rank: match active_speaker_rank {
                Some(rank) => rank,
                None => usize::MAX,
            },
            source_id: source_id.as_u64(),
        }
    }
}

const fn video_admission_priority(priority: SourceRoutePriority) -> u8 {
    match priority {
        SourceRoutePriority::PinnedOrFeatured => 0,
        SourceRoutePriority::ReadableDetail => 1,
        SourceRoutePriority::ActiveSpeaker => 2,
        SourceRoutePriority::VisibleThumbnail => 3,
        SourceRoutePriority::HiddenOrOverflow => 4,
    }
}

pub(in crate::engine::room) fn featured_source_user_ids_for_active_speakers(
    state: &RoomState,
    ranked_active_speaker_sources: &[ActiveSpeakerSource],
) -> BTreeSet<UserId> {
    first_featured_source_users_for_active_speakers(
        state,
        ranked_active_speaker_sources,
        ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT,
    )
}

impl RoomState {
    #[must_use]
    pub fn receiver_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
        active_speaker_source_user_ids: &BTreeSet<UserId>,
    ) -> ReceiverVideoLayoutIntent {
        let preference = self.source_policy_layout_preference(
            consumer_user_id,
            source.owner().user_id(),
            source.stream_id(),
        );
        ReceiverVideoLayoutIntent::resolve(
            source,
            preference,
            active_speaker_source_user_ids.contains(source.owner().user_id()),
        )
    }

    #[must_use]
    pub fn diagnostics_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
    ) -> Option<ReceiverVideoLayoutIntent> {
        source.policy().layout()?;
        let active_speaker_source_user_ids = self
            .source_policy_user_featured_states()
            .filter(|(_user_id, featured)| *featured == Some(true))
            .map(|(user_id, _featured)| user_id.clone())
            .collect();
        Some(self.receiver_video_layout_intent(
            consumer_user_id,
            source,
            &active_speaker_source_user_ids,
        ))
    }

    /// Plans public featured-state changes from the active-speaker snapshot.
    ///
    /// The returned updates are committed together with source-policy effects
    /// so the user-visible `isFeatured` projection and the quality floor come
    /// from the same observation. If there is no current active speaker, the
    /// method only emits clears when some user still has server-derived
    /// featured state.
    pub fn featured_user_updates(
        &self,
        ranked_active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<FeaturedUserUpdate> {
        let desired_featured_user_id =
            first_featured_source_user_for_active_speakers(self, ranked_active_speaker_sources);
        let should_clear_featured_state = desired_featured_user_id.is_none()
            && self
                .source_policy_user_featured_states()
                .any(|(_user_id, featured)| featured.is_some());
        if desired_featured_user_id.is_none() && !should_clear_featured_state {
            return Vec::new();
        }
        self.source_policy_user_featured_states()
            .filter_map(|(user_id, current_featured)| {
                let desired_featured = desired_featured_user_id.as_ref().map_or_else(
                    || current_featured.is_some().then_some(false),
                    |featured_user_id| Some(featured_user_id == user_id),
                );
                (desired_featured != current_featured)
                    .then(|| FeaturedUserUpdate::new(user_id.clone(), desired_featured))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test source fixtures should fail loudly when descriptor construction regresses"
    )]

    use super::*;
    use crate::engine::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
        SourceAdaptationPolicy, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceLayoutPolicy, SourcePolicy, UserStreamId,
    };

    fn source_with_layout(policy: SourceLayoutPolicy) -> PublishedSourceDescriptor {
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id: PublishedSourceId::from_raw(1),
            owner: PublishedSourceOwner::new(UserId::Integer(1)),
            stream_id: UserStreamId::new("video"),
            media_kind: o_sfu_router::MediaKind::Video,
            policy: SourcePolicy::new(Some(policy), SourceAdaptationPolicy::None, None),
            mid: None,
            encodings: vec![SourceEncodingDescriptor::new(
                SourceEncodingDescriptorParts {
                    encoding_id: SourceEncodingId::from_raw(1),
                    source_id: PublishedSourceId::from_raw(1),
                    rid: Some(o_sfu_router::Rid::new("hi")),
                    primary_ssrc: None,
                    repair_ssrc: None,
                    max_bitrate: None,
                    resolution_scale: None,
                    max_framerate: None,
                    policy_role: None,
                    max_temporal_layer_id: None,
                    negotiated_format: None,
                },
            )],
        })
        .expect("test source descriptor should be valid")
    }

    #[test]
    fn active_speaker_is_default_when_source_policy_allows_it() {
        let source = source_with_layout(SourceLayoutPolicy::new(
            SourceRoomPolicySelector::VisibleThumbnail,
            Some(SourceRoomPolicySelector::ActiveSpeaker),
        ));
        let intent = ReceiverVideoLayoutIntent::resolve(&source, None, true);

        assert_eq!(intent.role(), SourceRoomPolicySelector::ActiveSpeaker);
        assert_eq!(intent.priority(), SourceRoutePriority::ActiveSpeaker);
        assert!(intent.uses_featured_quality());
    }

    #[test]
    fn pinned_intent_overrides_active_speaker_default() {
        let source = source_with_layout(SourceLayoutPolicy::new(
            SourceRoomPolicySelector::VisibleThumbnail,
            Some(SourceRoomPolicySelector::ActiveSpeaker),
        ));
        let intent =
            ReceiverVideoLayoutIntent::resolve(&source, Some(VideoLayoutIntent::Pinned), false);

        assert_eq!(intent.role(), SourceRoomPolicySelector::Pinned);
        assert_eq!(intent.priority(), SourceRoutePriority::PinnedOrFeatured);
        assert!(intent.uses_featured_quality());
    }

    #[test]
    fn explicit_hidden_source_does_not_use_featured_quality_even_when_speaking() {
        let source = source_with_layout(SourceLayoutPolicy::new(
            SourceRoomPolicySelector::VisibleThumbnail,
            Some(SourceRoomPolicySelector::ActiveSpeaker),
        ));
        let intent =
            ReceiverVideoLayoutIntent::resolve(&source, Some(VideoLayoutIntent::Hidden), true);

        assert_eq!(intent.role(), SourceRoomPolicySelector::Hidden);
        assert_eq!(intent.priority(), SourceRoutePriority::HiddenOrOverflow);
        assert!(!intent.uses_featured_quality());
        assert!(!intent.counts_toward_visible_budget());
    }

    #[test]
    fn readable_detail_source_has_readability_priority_by_default() {
        let source = source_with_layout(SourceLayoutPolicy::new(
            SourceRoomPolicySelector::ReadableDetail,
            None,
        ));
        let intent = ReceiverVideoLayoutIntent::resolve(&source, None, false);

        assert_eq!(intent.role(), SourceRoomPolicySelector::ReadableDetail);
        assert_eq!(intent.priority(), SourceRoutePriority::ReadableDetail);
        assert!(intent.uses_featured_quality());
    }
}
