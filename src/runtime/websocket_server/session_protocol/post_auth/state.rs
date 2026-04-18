use std::collections::BTreeSet;

use o_sfu_protocol::shared::StreamType;

#[derive(Debug, Default)]
pub(super) struct PostAuthSessionState {
    queued_publish_streams: BTreeSet<StreamType>,
}

impl PostAuthSessionState {
    pub(super) fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_streams.contains(&stream_type)
    }

    pub(super) fn queue_publish_stream(&mut self, stream_type: StreamType) {
        self.queued_publish_streams.insert(stream_type);
    }

    pub(super) fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_streams.remove(&stream_type)
    }

    pub(super) fn take_queued_publish_streams(&mut self) -> Vec<StreamType> {
        let queued_publish_streams = self.queued_publish_streams.iter().copied().collect();
        self.queued_publish_streams.clear();
        queued_publish_streams
    }
}

#[cfg(test)]
mod tests {
    use super::PostAuthSessionState;
    use o_sfu_protocol::shared::StreamType;

    #[test]
    fn queued_publish_streams_are_unique() {
        let mut state = PostAuthSessionState::default();

        state.queue_publish_stream(StreamType::Camera);
        state.queue_publish_stream(StreamType::Camera);

        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
    }

    #[test]
    fn clearing_a_queued_publish_only_affects_that_stream() {
        let mut state = PostAuthSessionState::default();

        state.queue_publish_stream(StreamType::Audio);
        state.queue_publish_stream(StreamType::Screen);

        assert!(state.clear_queued_publish(StreamType::Audio));
        assert!(!state.has_queued_publish(StreamType::Audio));
        assert!(state.has_queued_publish(StreamType::Screen));
    }
}
