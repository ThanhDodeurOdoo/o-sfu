use std::collections::{BTreeMap, BTreeSet};

use crate::{
    shared::{DownloadStates, StreamType, UserId, UserInfo},
    signaling::{ClientEnvelope, ClientMessage, EnvelopeBatch, SubscribePayload},
};

/// remembers reconnect-safe client intent outside the live socket
///
/// welcome replay sends subscriptions and user info
/// transport-ready replay sends active publications
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct StickyReplayState {
    active_publications: BTreeSet<StreamType>,
    desired_subscriptions: BTreeMap<UserId, DownloadStates>,
    desired_info: Option<UserInfo>,
}

impl StickyReplayState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn clear(&mut self) {
        self.active_publications.clear();
        self.desired_subscriptions.clear();
        self.desired_info = None;
    }

    pub(super) fn set_publish_active(&mut self, stream_type: StreamType, active: bool) {
        if active {
            self.active_publications.insert(stream_type);
        } else {
            self.active_publications.remove(&stream_type);
        }
    }

    pub(super) fn active_publications(&self) -> impl Iterator<Item = StreamType> + '_ {
        self.active_publications.iter().copied()
    }

    pub(super) fn remember_subscription_states(
        &mut self,
        user_id: &UserId,
        states: &DownloadStates,
    ) {
        let existing_states = self
            .desired_subscriptions
            .entry(user_id.clone())
            .or_default();
        existing_states.apply_partial_update(states);
        if *existing_states == DownloadStates::default() {
            self.desired_subscriptions.remove(user_id);
        }
    }

    pub(super) fn remember_info(&mut self, info: &UserInfo) {
        let existing_info = self.desired_info.get_or_insert_with(UserInfo::default);
        let is_featured = existing_info.is_featured;
        existing_info.apply_partial_update(info);
        existing_info.is_featured = is_featured;
    }

    pub(super) fn replay_session_batch(&self) -> Option<EnvelopeBatch> {
        let mut replay_batch = Vec::new();

        for (user_id, states) in &self.desired_subscriptions {
            let Some(envelope) =
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    user_id: user_id.clone(),
                    states: states.clone(),
                }))
                .into_envelope()
                .ok()
            else {
                continue;
            };
            replay_batch.push(envelope);
        }

        if let Some(info) = self.desired_info.clone() {
            let envelope = ClientEnvelope::Message(ClientMessage::Info(info))
                .into_envelope()
                .ok()?;
            replay_batch.push(envelope);
        }

        if replay_batch.is_empty() {
            None
        } else {
            Some(replay_batch)
        }
    }
}
