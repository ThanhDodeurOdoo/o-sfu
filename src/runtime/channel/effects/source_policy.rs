//! Async effect executor for room-owned video source policy.
//!
//! The pure video-policy planner returns source-domain selector updates and
//! featured-state changes while the channel lock is held. This executor owns
//! the post-lock part of the transaction: apply packet gates to the transport
//! adapter, request keyframes for accepted upswitches, and commit only the
//! updates that still match the live channel route after those awaits.

use std::collections::BTreeSet;

use tracing::warn;

use super::super::{
    Channel,
    state::{ChannelState, ConsumerPacketSelectionUpdate, FeaturedSessionUpdate},
};
use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, ConsumerPacketGateUpdate, MediaPort, ReceiverBandwidthSnapshot,
};

/// Executes one source-policy refresh after pure channel planning.
///
/// Consumer packet updates touch the transport before channel state records the
/// new selector. That keeps stale transport failures local to one update
/// instead of rolling back a room transition that may already have moved on.
/// Featured-session projection is part of the same plan so outbound layout
/// state and selector floors derive from the same active-speaker observation.
#[derive(Debug, Default)]
pub(in crate::runtime::channel) struct SourcePolicyEffectPlan {
    consumer_packets: Vec<ConsumerPacketSelectionUpdate>,
    featured_sessions: Vec<FeaturedSessionUpdate>,
}

impl SourcePolicyEffectPlan {
    /// Builds a cold-path effect plan from one transport observation snapshot.
    ///
    /// The caller has already released any long-lived transport work. This
    /// constructor only reads channel state and does not mutate transport state.
    pub(in crate::runtime::channel) fn from_state(
        state: &ChannelState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        Self {
            consumer_packets: state.consumer_packet_selection_updates(
                active_speaker_sources,
                receiver_bandwidth_snapshot,
            ),
            featured_sessions: state.featured_session_updates(active_speaker_sources),
        }
    }

    pub(in crate::runtime::channel) fn is_empty(&self) -> bool {
        self.consumer_packets.is_empty() && self.featured_sessions.is_empty()
    }

    /// Applies transport-visible gates before committing selector state.
    ///
    /// The channel write lock is acquired only after transport I/O finishes.
    /// Only updates accepted by the transport are committed back into
    /// `ChannelState`; rejected or stale updates are left for the next policy
    /// refresh.
    pub(in crate::runtime::channel) async fn execute(
        self,
        channel: &Channel,
        media_port: &impl MediaPort,
    ) {
        let applied_consumer_packet_updates =
            Self::apply_consumer_packet_updates(channel, media_port, self.consumer_packets).await;
        if applied_consumer_packet_updates.is_empty() && self.featured_sessions.is_empty() {
            return;
        }
        Self::record_source_selection_metrics(channel, &applied_consumer_packet_updates);
        let info_fanout = {
            let mut state = channel.state.write().await;
            state.commit_consumer_packet_selection_updates(&applied_consumer_packet_updates);
            state.commit_featured_session_updates(&self.featured_sessions)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }

    fn record_source_selection_metrics(
        channel: &Channel,
        updates: &[ConsumerPacketSelectionUpdate],
    ) {
        for update in updates {
            if update.packet_gate().is_some() {
                channel
                    .metrics
                    .record_source_selection_update(update.selector());
            }
        }
    }

    async fn apply_consumer_packet_updates(
        channel: &Channel,
        media_port: &impl MediaPort,
        updates: Vec<ConsumerPacketSelectionUpdate>,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let mut packet_gate_updates = Vec::with_capacity(updates.len());
        let mut update_indexes_with_packet_gates = Vec::with_capacity(updates.len());
        for (index, update) in updates.iter().enumerate() {
            let Some(packet_gate) = update.packet_gate() else {
                continue;
            };
            packet_gate_updates.push(ConsumerPacketGateUpdate::new(
                channel.transport_session_key(
                    update.consumer_session_id(),
                    update.consumer_connection_id(),
                ),
                update.consumer_transport_media_id(),
                channel.transport_session_key(
                    update.source_session_id(),
                    update.source_connection_id(),
                ),
                update.source_transport_media_id(),
                packet_gate.clone(),
            ));
            update_indexes_with_packet_gates.push(index);
        }
        let rejected_packet_gate_update_offsets =
            Self::rejected_packet_gate_updates(media_port, &packet_gate_updates).await;
        let rejected_packet_gate_updates = rejected_packet_gate_update_offsets
            .into_iter()
            .filter_map(|offset| update_indexes_with_packet_gates.get(offset).copied())
            .collect::<BTreeSet<_>>();
        let mut applied_updates = Vec::with_capacity(updates.len());
        for (index, update) in updates.into_iter().enumerate() {
            if rejected_packet_gate_updates.contains(&index) {
                warn!(
                    consumer_session_id = ?update.consumer_session_id(),
                    source_session_id = ?update.source_session_id(),
                    source_transport_media_id = ?update.source_transport_media_id(),
                    consumer_transport_media_id = ?update.consumer_transport_media_id(),
                    "transport adapter rejected the receiver-driven packet selection update"
                );
                continue;
            }
            if !Self::request_adaptation_keyframe(channel, media_port, &update).await {
                warn!(
                    consumer_session_id = ?update.consumer_session_id(),
                    source_session_id = ?update.source_session_id(),
                    source_transport_media_id = ?update.source_transport_media_id(),
                    consumer_transport_media_id = ?update.consumer_transport_media_id(),
                    "transport adapter failed to request an adaptation keyframe refresh"
                );
            }
            applied_updates.push(update);
        }
        applied_updates
    }

    async fn rejected_packet_gate_updates(
        media_port: &impl MediaPort,
        packet_gate_updates: &[ConsumerPacketGateUpdate],
    ) -> BTreeSet<usize> {
        let mut rejected_updates = BTreeSet::new();
        let results = media_port
            .set_consumer_packet_gates(packet_gate_updates)
            .await;
        for index in 0..packet_gate_updates.len() {
            if !matches!(results.get(index), Some(Ok(()))) {
                rejected_updates.insert(index);
            }
        }
        rejected_updates
    }

    async fn request_adaptation_keyframe(
        channel: &Channel,
        media_port: &impl MediaPort,
        update: &ConsumerPacketSelectionUpdate,
    ) -> bool {
        if !update.request_keyframe() {
            return true;
        }
        media_port
            .request_consumer_keyframe(
                &channel.transport_session_key(
                    update.consumer_session_id(),
                    update.consumer_connection_id(),
                ),
                update.consumer_transport_media_id(),
                &channel.transport_session_key(
                    update.source_session_id(),
                    update.source_connection_id(),
                ),
                update.source_transport_media_id(),
            )
            .await
            .is_ok()
    }
}
