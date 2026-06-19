use std::collections::BTreeSet;

use super::super::super::state::RoomState;
use crate::engine::{
    UserId, VideoLayoutIntent,
    source_model::{
        PublishedSourceDescriptor, PublishedSourceId, SourceRoomPolicySelector, SourceRoutePriority,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverVideoLayoutIntent {
    role: SourceRoomPolicySelector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VideoAdmissionRank {
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

impl RoomState {
    #[must_use]
    pub fn receiver_video_layout_intent(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
        active_speaker_source_user_ids: &BTreeSet<UserId>,
    ) -> ReceiverVideoLayoutIntent {
        let preference = layout_preference(self, consumer_user_id, source);
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
        let preference = layout_preference(self, consumer_user_id, source);
        let active_speaker = self
            .users
            .get(source.owner().user_id())
            .is_some_and(|user| user.featured() == Some(true));
        Some(ReceiverVideoLayoutIntent::resolve(
            source,
            preference,
            active_speaker,
        ))
    }
}

fn layout_preference(
    state: &RoomState,
    consumer_user_id: &UserId,
    source: &PublishedSourceDescriptor,
) -> Option<VideoLayoutIntent> {
    state
        .users
        .get(consumer_user_id)
        .and_then(|user| {
            user.desired_source_subscriptions
                .get(source.owner().user_id())
        })
        .and_then(|states| states.get(source.stream_id()))
        .and_then(|intent| intent.layout())
}

#[cfg(test)]
#[path = "TESTS/layout.rs"]
mod tests;
