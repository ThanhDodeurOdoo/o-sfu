use std::{collections::BTreeSet, mem};

use crate::runtime::transport_adapter::TransportMediaId;
use crate::signaling::{shared::StreamType, webrtc::MediaKind as SignalingMediaKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PendingPublishCommit {
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Default)]
pub(super) struct NativeSessionState {
    pending_publish_commits: Vec<PendingPublishCommit>,
    queued_publish_streams: BTreeSet<StreamType>,
}

impl NativeSessionState {
    pub(super) fn contains_publish_transition(&self, stream_type: StreamType) -> bool {
        self.pending_publish_commits
            .iter()
            .any(|pending| pending.stream_type == stream_type)
            || self.queued_publish_streams.contains(&stream_type)
    }

    pub(super) fn queue_publish_stream(&mut self, stream_type: StreamType) {
        self.queued_publish_streams.insert(stream_type);
    }

    pub(super) fn remove_queued_publish_stream(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_streams.remove(&stream_type)
    }

    pub(super) fn take_pending_publish_for_stream(
        &mut self,
        stream_type: StreamType,
    ) -> Option<PendingPublishCommit> {
        let position = self
            .pending_publish_commits
            .iter()
            .position(|pending| pending.stream_type == stream_type)?;
        Some(self.pending_publish_commits.remove(position))
    }

    pub(super) fn push_pending_publish(&mut self, pending_publish: PendingPublishCommit) {
        self.pending_publish_commits.push(pending_publish);
    }

    pub(super) fn take_queued_publish_streams(&mut self) -> Vec<StreamType> {
        mem::take(&mut self.queued_publish_streams)
            .into_iter()
            .collect()
    }

    pub(super) fn take_pending_publish_commits(&mut self) -> Vec<PendingPublishCommit> {
        mem::take(&mut self.pending_publish_commits)
    }
}
