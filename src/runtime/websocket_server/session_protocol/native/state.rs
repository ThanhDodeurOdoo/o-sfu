use std::collections::BTreeMap;

use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StagedPublishTransaction {
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishTransition {
    Queued,
    Staged(StagedPublishTransaction),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClearedPublishTransition {
    Queued,
    Staged(StagedPublishTransaction),
}

#[derive(Debug, Default)]
pub(super) struct NativeSessionState {
    publish_transitions: BTreeMap<StreamType, PublishTransition>,
}

impl NativeSessionState {
    pub(super) fn contains_publish_transition(&self, stream_type: StreamType) -> bool {
        self.publish_transitions.contains_key(&stream_type)
    }

    pub(super) fn queue_publish_stream(&mut self, stream_type: StreamType) {
        self.publish_transitions
            .insert(stream_type, PublishTransition::Queued);
    }

    pub(super) fn stage_publish_transaction(&mut self, staged_publish: StagedPublishTransaction) {
        self.publish_transitions.insert(
            staged_publish.stream_type,
            PublishTransition::Staged(staged_publish),
        );
    }

    pub(super) fn clear_publish_transition(
        &mut self,
        stream_type: StreamType,
    ) -> Option<ClearedPublishTransition> {
        match self.publish_transitions.remove(&stream_type)? {
            PublishTransition::Queued => Some(ClearedPublishTransition::Queued),
            PublishTransition::Staged(staged_publish) => {
                Some(ClearedPublishTransition::Staged(staged_publish))
            }
        }
    }

    pub(super) fn take_queued_publish_streams(&mut self) -> Vec<StreamType> {
        let queued_streams = self
            .publish_transitions
            .iter()
            .filter_map(|(stream_type, transition)| {
                matches!(transition, PublishTransition::Queued).then_some(*stream_type)
            })
            .collect::<Vec<_>>();
        for stream_type in &queued_streams {
            let _removed = self.publish_transitions.remove(stream_type);
        }
        queued_streams
    }

    pub(super) fn take_staged_publish_transactions(&mut self) -> Vec<StagedPublishTransaction> {
        let staged_streams = self
            .publish_transitions
            .iter()
            .filter_map(|(stream_type, transition)| {
                matches!(transition, PublishTransition::Staged(_)).then_some(*stream_type)
            })
            .collect::<Vec<_>>();
        staged_streams
            .into_iter()
            .filter_map(
                |stream_type| match self.publish_transitions.remove(&stream_type) {
                    Some(PublishTransition::Staged(staged_publish)) => Some(staged_publish),
                    Some(PublishTransition::Queued) | None => None,
                },
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClearedPublishTransition, NativeSessionState, StagedPublishTransaction};
    use crate::{
        runtime::transport_adapter::TransportMediaId,
        signaling::{shared::StreamType, webrtc::MediaKind},
    };

    #[test]
    fn queued_and_staged_publish_transitions_are_tracked_separately() {
        let mut state = NativeSessionState::default();

        state.queue_publish_stream(StreamType::Camera);
        state.stage_publish_transaction(StagedPublishTransaction {
            stream_type: StreamType::Screen,
            media_kind: MediaKind::Video,
            transport_media_id: TransportMediaId::new(7),
        });

        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
        assert_eq!(
            state.clear_publish_transition(StreamType::Screen),
            Some(ClearedPublishTransition::Staged(StagedPublishTransaction {
                stream_type: StreamType::Screen,
                media_kind: MediaKind::Video,
                transport_media_id: TransportMediaId::new(7),
            }))
        );
    }

    #[test]
    fn staged_publish_transactions_drain_without_touching_queued_entries() {
        let mut state = NativeSessionState::default();

        state.queue_publish_stream(StreamType::Audio);
        state.stage_publish_transaction(StagedPublishTransaction {
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            transport_media_id: TransportMediaId::new(11),
        });

        assert_eq!(
            state.take_staged_publish_transactions(),
            vec![StagedPublishTransaction {
                stream_type: StreamType::Camera,
                media_kind: MediaKind::Video,
                transport_media_id: TransportMediaId::new(11),
            }]
        );
        assert_eq!(
            state.clear_publish_transition(StreamType::Audio),
            Some(ClearedPublishTransition::Queued)
        );
    }
}
