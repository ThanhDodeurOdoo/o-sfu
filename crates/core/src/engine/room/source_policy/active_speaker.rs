//! Shared active-speaker ranking for room source policy.
//!
//! Transport snapshots preserve observation order. Room policy ranks those
//! observations before applying admission, layout and video budget decisions so
//! every policy path agrees on the same dominant speaker.

use std::cmp::Reverse;

use crate::engine::media_transport::ActiveSpeakerSource;

#[must_use]
pub(in crate::engine::room) fn rank_active_speaker_sources(
    sources: &[ActiveSpeakerSource],
) -> Vec<ActiveSpeakerSource> {
    let mut sources = sources.to_vec();
    sources.sort_by_key(|source| {
        (
            Reverse(source.observed_at()),
            Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            source.transport_media_id().as_u64(),
        )
    });
    sources
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::engine::media_transport::TransportMediaId;

    #[test]
    fn rankings_prefer_recent_then_louder_speaker_then_stable_id() {
        let earlier = Instant::now();
        let now = earlier + Duration::from_millis(1);
        let ranked_sources = rank_active_speaker_sources(&[
            ActiveSpeakerSource::with_audio_level(TransportMediaId::new(4), earlier, Some(-1)),
            ActiveSpeakerSource::with_audio_level(TransportMediaId::new(3), now, Some(-30)),
            ActiveSpeakerSource::with_audio_level(TransportMediaId::new(2), now, Some(-10)),
            ActiveSpeakerSource::with_audio_level(TransportMediaId::new(1), now, Some(-10)),
        ]);

        assert_eq!(
            ranked_sources
                .iter()
                .map(|source| source.transport_media_id())
                .collect::<Vec<_>>(),
            vec![
                TransportMediaId::new(1),
                TransportMediaId::new(2),
                TransportMediaId::new(3),
                TransportMediaId::new(4),
            ]
        );
    }
}
