//! Room-owned audio route admission.
//!
//! Transport reports active-speaker facts. Room policy turns those facts into a
//! bounded admitted set so large rooms cannot forward every simultaneous talker.

use std::{cmp::Reverse, collections::BTreeSet};

use o_sfu_router::MediaKind;

use super::{super::shared::RoomState, action::ConsumerPacketSelectionUpdate};
use crate::runtime::{
    media_transport::{ActiveSpeakerSource, TransportMediaId},
    source_model::PolicyPauseReason,
};

impl RoomState {
    pub fn audio_route_activity_updates(
        &self,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let active_speakers = active_speaker_media_ids(active_speaker_sources);
        let admitted_speakers = admitted_audio_speakers(
            active_speaker_sources,
            self.media_limits.max_active_audio_speakers(),
        );
        self.current_live_consumer_routes()
            .filter_map(|route| {
                if route.source.media_kind() != MediaKind::Audio || !route.producer.active {
                    return None;
                }
                let desired_active = self.desired_source_subscription_active(
                    &route.consumer_user_id,
                    route.source.owner().user_id(),
                    route.source.stream_id(),
                );
                let current_selection = route.selection_or_open(desired_active);
                if !current_selection.active() {
                    return None;
                }
                let active_speaker = active_speakers.contains(&route.state.source_media);
                let admitted = admitted_speakers.contains(&route.state.source_media);
                let next_reason =
                    (active_speaker && !admitted).then_some(PolicyPauseReason::AudioSpeakerLimit);
                if next_reason.is_none()
                    && current_selection.policy_pause_reason()
                        != Some(PolicyPauseReason::AudioSpeakerLimit)
                {
                    return None;
                }
                ConsumerPacketSelectionUpdate::route_activity(
                    route.transport_ref(),
                    route.source.source_id(),
                    current_selection,
                    next_reason,
                )
            })
            .collect()
    }
}

fn active_speaker_media_ids(sources: &[ActiveSpeakerSource]) -> BTreeSet<TransportMediaId> {
    sources
        .iter()
        .map(|source| source.transport_media_id())
        .collect()
}

fn admitted_audio_speakers(
    sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<TransportMediaId> {
    let mut ranked_sources = sources.to_vec();
    ranked_sources.sort_by_key(|source| {
        (
            Reverse(source.observed_at()),
            Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            source.transport_media_id().as_u64(),
        )
    });
    ranked_sources
        .into_iter()
        .take(limit)
        .map(ActiveSpeakerSource::transport_media_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn audio_admission_prefers_recent_then_louder_speaker_then_stable_id() {
        let earlier = Instant::now();
        let Some(now) = earlier.checked_add(Duration::from_millis(1)) else {
            return;
        };
        let admitted = admitted_audio_speakers(
            &[
                ActiveSpeakerSource::with_audio_level(TransportMediaId::new(3), earlier, Some(-20)),
                ActiveSpeakerSource::with_audio_level(TransportMediaId::new(2), now, Some(-30)),
                ActiveSpeakerSource::with_audio_level(TransportMediaId::new(1), now, Some(-10)),
            ],
            2,
        );

        assert!(admitted.contains(&TransportMediaId::new(1)));
        assert!(admitted.contains(&TransportMediaId::new(2)));
        assert!(!admitted.contains(&TransportMediaId::new(3)));
    }
}
