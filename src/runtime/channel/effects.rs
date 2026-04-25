//! Post-lock effect plans for `Chanel` transitions.
//!
//! `ChannelState` owns pure room mutation and validation under lock. This
//! module owns the transport calls, diagnostics writes and fanout that must run
//! after that lock is released.
//!
//! 1. read or mutate channel state under lock
//! 2. build a typed plan for the side effects of that transitition
//! 3. execute transport work after unlock
//! 4. commit only the post-transport state that is still valid
//!
//! The important invariant is that each transition makes its ordering and failure
//! handling explicit where room state meets transport state.

use o_sfu_protocol::shared::{SessionId, StreamType};
use tracing::warn;

use super::{
    Channel, ChannelMediaCounts,
    state::{
        ChannelState, ConsumerBootstrapOrigin, ConsumerPacketSelectionUpdate, ConsumerRouteUpdate,
        FeaturedSessionUpdate, PendingConsumerBootstrap, PendingConsumerBootstrapTarget,
        PlannedConsumerBootstrap, PlannedSubscriptionChange, PreparedConsumerBootstrap,
        SourcePacketSelectionUpdate, TransportMediaRemoval,
    },
};
use crate::runtime::{
    ConnectionId,
    diagnostics::DiagnosticsEventData,
    telemetry::schema::event as telemetry_event,
    transport_adapter::{
        ActiveSpeakerSource, MediaPort, ReceiverBandwidthSnapshot, TransportMediaId,
    },
};

#[derive(Debug, Clone, Copy)]
pub(super) struct MediaCountDelta {
    before: ChannelMediaCounts,
    after: ChannelMediaCounts,
}

impl MediaCountDelta {
    fn new(before: ChannelMediaCounts, after: ChannelMediaCounts) -> Self {
        Self { before, after }
    }

    fn record(self, channel: &Channel) {
        channel.record_media_count_delta(self.before, self.after);
    }
}

#[derive(Debug)]
struct SubscriptionTransportOp {
    consumer_session_id: SessionId,
    consumer_connection_id: ConnectionId,
    consumer_media: TransportMediaId,
    producer_session_id: SessionId,
    producer_connection_id: ConnectionId,
    source_media: TransportMediaId,
    stream_type: StreamType,
    active: bool,
    diagnostics: DiagnosticsEventData,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SubscriptionEffectContext<'a> {
    pub(super) session_id: &'a SessionId,
    pub(super) connection_id: ConnectionId,
    pub(super) target_session_id: &'a SessionId,
    pub(super) media_counts_before: ChannelMediaCounts,
    pub(super) media_counts_after: ChannelMediaCounts,
    pub(super) origin: ConsumerBootstrapOrigin,
}

#[derive(Debug, Default)]
pub(super) struct SubscriptionEffectPlan {
    media_count_delta: Option<MediaCountDelta>,
    transport_ops: Vec<SubscriptionTransportOp>,
    bootstrap_ops: Vec<ConsumerBootstrapOp>,
}

#[derive(Debug)]
struct ConsumerBootstrapOp {
    target: PendingConsumerBootstrapTarget,
    prepared: PreparedConsumerBootstrap,
    pending_bootstrap: PendingConsumerBootstrap,
    origin: ConsumerBootstrapOrigin,
}

impl SubscriptionEffectPlan {
    /// Persists the accepted route toggles as transport work plus the matching
    /// diagnostics payloads. The diagnostics are shaped here so the caller can
    /// finish all subscription-side side effects from one post-lock executor
    pub(super) fn from_route_updates(
        channel: &Channel,
        session_id: &SessionId,
        connection_id: ConnectionId,
        _target_session_id: &SessionId,
        route_updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let transport_ops = route_updates
            .into_iter()
            .map(|route_update| SubscriptionTransportOp {
                consumer_session_id: session_id.clone(),
                consumer_connection_id: route_update.consumer_connection_id(),
                consumer_media: route_update.consumer_media(),
                producer_session_id: route_update.producer_session_id().clone(),
                producer_connection_id: route_update.source_connection_id(),
                source_media: route_update.source_media(),
                stream_type: route_update.stream_type(),
                active: route_update.active(),
                diagnostics: DiagnosticsEventData::for_session(
                    channel.uuid(),
                    session_id,
                    telemetry_event::SUBSCRIPTION_ACTIVITY_CHANGED,
                )
                .with_connection_id(connection_id.as_u64())
                .with_media_worker_id(channel.media_worker_id())
                .with_transport_media_id(route_update.consumer_media().as_u64())
                .insert_field("active", route_update.active())
                .insert_field(
                    "producer_session_id",
                    serde_json::to_value(route_update.producer_session_id())
                        .unwrap_or(serde_json::Value::Null),
                )
                .insert_field(
                    "source_transport_media_id",
                    route_update.source_media().as_u64(),
                )
                .insert_field(
                    "stream_type",
                    format!("{:?}", route_update.stream_type()).to_lowercase(),
                ),
            })
            .collect();
        Self {
            media_count_delta: None,
            transport_ops,
            bootstrap_ops: Vec::new(),
        }
    }

    pub(super) fn from_planned_change(
        channel: &Channel,
        context: SubscriptionEffectContext<'_>,
        planned_change: PlannedSubscriptionChange,
    ) -> Self {
        let (route_updates, planned_bootstraps) = planned_change.into_parts();
        let mut effect_plan = Self::from_route_updates(
            channel,
            context.session_id,
            context.connection_id,
            context.target_session_id,
            route_updates,
        );
        effect_plan.media_count_delta = Some(MediaCountDelta::new(
            context.media_counts_before,
            context.media_counts_after,
        ));
        effect_plan.bootstrap_ops = planned_bootstraps
            .into_iter()
            .map(|planned_bootstrap| ConsumerBootstrapOp::new(planned_bootstrap, context.origin))
            .collect();
        effect_plan
    }

    pub(super) fn from_planned_bootstraps(
        media_counts_before: ChannelMediaCounts,
        media_counts_after: ChannelMediaCounts,
        planned_bootstraps: Vec<PlannedConsumerBootstrap>,
        origin: ConsumerBootstrapOrigin,
    ) -> Self {
        Self {
            media_count_delta: Some(MediaCountDelta::new(
                media_counts_before,
                media_counts_after,
            )),
            transport_ops: Vec::new(),
            bootstrap_ops: planned_bootstraps
                .into_iter()
                .map(|planned_bootstrap| ConsumerBootstrapOp::new(planned_bootstrap, origin))
                .collect(),
        }
    }

    /// Applies the subscription decision to the transport adapter.
    ///
    /// A resumed video route needs more than an `active=true` flip. After a
    /// long pause the receiver may need a fresh decodable frame before it can
    /// render again, so successful camera and screen resumes trigger an
    /// immediate keyframe request on the underlying consumer route.
    pub(super) async fn execute(self, channel: &Channel, media_port: &impl MediaPort) {
        if let Some(media_count_delta) = self.media_count_delta {
            media_count_delta.record(channel);
        }
        for transport_op in self.transport_ops {
            // Transport activity is best-effort here because the channel intent has
            // already been committed, so the failure path is to surface it to
            // operators instead of trying to rebuild the previous room state.
            if media_port
                .set_consumer_active(
                    &channel.transport_session_key(
                        &transport_op.consumer_session_id,
                        transport_op.consumer_connection_id,
                    ),
                    transport_op.consumer_media,
                    &channel.transport_session_key(
                        &transport_op.producer_session_id,
                        transport_op.producer_connection_id,
                    ),
                    transport_op.source_media,
                    transport_op.active,
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?transport_op.consumer_session_id,
                    target_session_id = ?transport_op.producer_session_id,
                    stream_type = ?transport_op.stream_type,
                    active = transport_op.active,
                    "transport adapter failed to update consumer route activity"
                );
            } else if transport_op.active
                && matches!(
                    transport_op.stream_type,
                    StreamType::Camera | StreamType::Screen
                )
                && media_port
                    .request_consumer_keyframe(
                        &channel.transport_session_key(
                            &transport_op.consumer_session_id,
                            transport_op.consumer_connection_id,
                        ),
                        transport_op.consumer_media,
                        &channel.transport_session_key(
                            &transport_op.producer_session_id,
                            transport_op.producer_connection_id,
                        ),
                        transport_op.source_media,
                    )
                    .await
                    .is_err()
            {
                warn!(
                    session_id = ?transport_op.consumer_session_id,
                    target_session_id = ?transport_op.producer_session_id,
                    stream_type = ?transport_op.stream_type,
                    "transport adapter failed to request a consumer keyframe refresh"
                );
            }
            channel.diagnostics.record(transport_op.diagnostics);
        }
        for bootstrap_op in self.bootstrap_ops {
            bootstrap_op.execute(channel, media_port).await;
        }
    }
}

impl ConsumerBootstrapOp {
    fn new(planned_bootstrap: PlannedConsumerBootstrap, origin: ConsumerBootstrapOrigin) -> Self {
        let (target, prepared, pending_bootstrap) = planned_bootstrap.into_parts();
        Self {
            target,
            prepared,
            pending_bootstrap,
            origin,
        }
    }

    async fn execute(self, channel: &Channel, media_port: &impl MediaPort) {
        let Self {
            target,
            prepared,
            pending_bootstrap,
            origin,
        } = self;
        let Some((consumer_transport_media_id, consumer_mid)) =
            Self::declare_consumer_transport_media(&target, &prepared, origin, channel, media_port)
                .await
        else {
            return;
        };
        let (media_counts_before, outbound, media_counts_after) = {
            let mut state = channel.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let outbound = state.commit_consumer_bootstrap(
                &target,
                pending_bootstrap,
                consumer_transport_media_id,
                consumer_mid,
            );
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            (media_counts_before, outbound, media_counts_after)
        };
        let media_count_delta = MediaCountDelta::new(media_counts_before, media_counts_after);
        Self::finish(
            channel,
            media_port,
            &target,
            origin,
            consumer_transport_media_id,
            media_count_delta,
            outbound,
        )
        .await;
    }

    /// Declare the transport-side consumer media before committing the channel
    /// bootstrap.
    ///
    /// Keepign this outside the state commit makes transport failure handling
    /// explicit: the channel can release the pending bootstrap instead of
    /// committing room state that points at missing transport media
    async fn declare_consumer_transport_media(
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
        origin: ConsumerBootstrapOrigin,
        channel: &Channel,
        media_port: &impl MediaPort,
    ) -> Option<(TransportMediaId, Option<String>)> {
        let consumer_session_key = channel.transport_session_key(
            target.consumer_session_id(),
            target.consumer_connection_id(),
        );
        let consumer_transport_media_id = match media_port
            .consume_media(
                &consumer_session_key,
                target.media_kind(),
                &channel.transport_session_key(
                    target.producer_session_id(),
                    target.producer_connection_id(),
                ),
                target.transport_media_id(),
                prepared.consumer_rtp_parameters(),
            )
            .await
        {
            Ok(transport_media_id) => transport_media_id,
            Err(error) => {
                channel.release_pending_consumer_bootstrap(target).await;
                warn!(
                    consumer_session_id = ?target.consumer_session_id(),
                    consumer_connection_id = ?target.consumer_connection_id(),
                    producer_session_id = ?target.producer_session_id(),
                    producer_connection_id = ?target.producer_connection_id(),
                    source_transport_media_id = ?target.transport_media_id(),
                    error = ?error,
                    consumer_mid = prepared.consumer_rtp_parameters().mid(),
                    ?origin,
                    "transport adapter rejected consume media declaration"
                );
                return None;
            }
        };
        let consumer_mid = media_port
            .transport_media_mid(&consumer_session_key, consumer_transport_media_id)
            .await;
        Some((consumer_transport_media_id, consumer_mid))
    }

    /// Finalize a prepared consumer bootstrap once the transport media exists.
    ///
    /// This step intentionally does not request a keyframe. Fresh subscribers
    /// need that refresh only after the receiver has applied the
    /// relevant SDP answer, which is handled by the later session-negotiation
    /// callbacks.
    async fn finish(
        channel: &Channel,
        media_port: &impl MediaPort,
        target: &PendingConsumerBootstrapTarget,
        origin: ConsumerBootstrapOrigin,
        consumer_transport_media_id: TransportMediaId,
        media_count_delta: MediaCountDelta,
        outbound: Option<(
            super::outbound::OutboundSender,
            super::RemoteTrackBootstrap,
            bool,
        )>,
    ) {
        let Some((sender, bootstrap, consumer_active)) = outbound else {
            media_count_delta.record(channel);
            channel
                .cleanup_transport_media(
                    target.consumer_session_id(),
                    target.consumer_connection_id(),
                    consumer_transport_media_id,
                    media_port,
                    "transport adapter failed to remove consumer transport media after bootstrap state commit failed",
                )
                .await;
            return;
        };
        media_count_delta.record(channel);
        channel
            .apply_initial_consumer_pause_state(
                target,
                consumer_transport_media_id,
                consumer_active,
                media_port,
                origin,
            )
            .await;
        channel.diagnostics.record(
            DiagnosticsEventData::for_session(
                channel.uuid(),
                target.consumer_session_id(),
                telemetry_event::SUBSCRIBE_SUCCEEDED,
            )
            .with_connection_id(target.consumer_connection_id().as_u64())
            .with_media_worker_id(channel.media_worker_id())
            .with_transport_media_id(consumer_transport_media_id.as_u64())
            .insert_field(
                "producer_session_id",
                serde_json::to_value(target.producer_session_id())
                    .unwrap_or(serde_json::Value::Null),
            )
            .insert_field(
                "source_transport_media_id",
                target.transport_media_id().as_u64(),
            )
            .insert_field(
                "stream_type",
                format!("{:?}", target.stream_type()).to_lowercase(),
            )
            .insert_field("origin", format!("{origin:?}").to_lowercase()),
        );
        let _ = sender.send(super::SessionOutbound::Request(Box::new(
            bootstrap.into_channel_event_request(),
        )));
    }
}

#[derive(Debug)]
pub(super) struct UnpublishEffectPlan {
    session_id: SessionId,
    connection_id: ConnectionId,
    stream_type: StreamType,
    transport_removals: Vec<TransportMediaRemoval>,
}

impl UnpublishEffectPlan {
    pub(super) fn new(
        session_id: SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        transport_removals: Vec<TransportMediaRemoval>,
    ) -> Self {
        Self {
            session_id,
            connection_id,
            stream_type,
            transport_removals,
        }
    }

    pub(super) async fn execute(self, channel: &Channel, media_port: &impl MediaPort) -> bool {
        // Explicit unpublish tears down transport state first so a later state
        // commit failure cannot leave routable media alive for a track the room
        // already considers removed.
        if !channel
            .cleanup_transport_removals_strict(media_port, &self.transport_removals)
            .await
        {
            return false;
        }
        let (media_counts_before, outcome, media_counts_after) = {
            let mut state = channel.state.write().await;
            let media_counts_before = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            let outcome =
                state.unpublish_track(&self.session_id, self.connection_id, self.stream_type);
            let media_counts_after = ChannelMediaCounts {
                publications: state.publication_count(),
                subscriptions: state.subscription_count(),
            };
            drop(state);
            (media_counts_before, outcome, media_counts_after)
        };
        let Some(outcome) = outcome else {
            warn!(
                session_id = ?self.session_id,
                connection_id = ?self.connection_id,
                stream_type = ?self.stream_type,
                "transport cleanup succeeded but channel state commit failed"
            );
            return false;
        };
        MediaCountDelta::new(media_counts_before, media_counts_after).record(channel);
        outcome.emit(&self.session_id, self.stream_type);
        true
    }
}

/// Executes room-owned source policy after pure channel planning.
///
/// Source and consumer packet updates touch the transport before channel state
/// records the new selector. That keeps stale transport failures local to one
/// update instead of rolling back a room transition that may already have moved
/// on. Featured-session projection is still part of this plan so outbound
/// layout state is derived from the same active-speaker observation.
#[derive(Debug, Default)]
pub(super) struct SourcePacketPolicyEffectPlan {
    source_packets: Vec<SourcePacketSelectionUpdate>,
    consumer_packets: Vec<ConsumerPacketSelectionUpdate>,
    featured_sessions: Vec<FeaturedSessionUpdate>,
}

impl SourcePacketPolicyEffectPlan {
    /// Builds a cold-path effect plan from one transport observation snapshot.
    ///
    /// The caller has already released any long-lived transport work. This
    /// constructor only reads channel state and does not mutate transport state.
    pub(super) fn from_state(
        state: &ChannelState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
    ) -> Self {
        Self {
            source_packets: state.source_packet_selection_updates(active_speaker_sources),
            consumer_packets: state.consumer_packet_selection_updates(
                active_speaker_sources,
                receiver_bandwidth_snapshot,
            ),
            featured_sessions: state.featured_session_updates(active_speaker_sources),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.source_packets.is_empty()
            && self.consumer_packets.is_empty()
            && self.featured_sessions.is_empty()
    }

    /// Applies transport-visible gates before committing selector state.
    ///
    /// This method must not hold the chanel write lock while awaiting the
    /// transport adapter. Only updates accepted by the transport are committed
    /// back into `ChannelState`
    pub(super) async fn execute(self, channel: &Channel, media_port: &impl MediaPort) {
        let applied_source_packet_updates =
            Self::apply_source_packet_updates(channel, media_port, self.source_packets).await;
        let applied_consumer_packet_updates =
            Self::apply_consumer_packet_updates(channel, media_port, self.consumer_packets).await;
        if applied_source_packet_updates.is_empty()
            && applied_consumer_packet_updates.is_empty()
            && self.featured_sessions.is_empty()
        {
            return;
        }
        let info_fanout = {
            let mut state = channel.state.write().await;
            state.commit_source_packet_selection_updates(&applied_source_packet_updates);
            state.commit_consumer_packet_selection_updates(&applied_consumer_packet_updates);
            state.commit_featured_session_updates(&self.featured_sessions)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }

    async fn apply_source_packet_updates(
        channel: &Channel,
        media_port: &impl MediaPort,
        updates: Vec<SourcePacketSelectionUpdate>,
    ) -> Vec<SourcePacketSelectionUpdate> {
        let mut applied_updates = Vec::with_capacity(updates.len());
        for update in updates {
            if media_port
                .set_source_packet_gate(
                    &channel.transport_session_key(
                        update.owner_session_id(),
                        update.owner_connection_id(),
                    ),
                    update.transport_media_id(),
                    update.packet_gate().clone(),
                )
                .await
                .is_err()
            {
                warn!(
                    session_id = ?update.owner_session_id(),
                    connection_id = ?update.owner_connection_id(),
                    transport_media_id = ?update.transport_media_id(),
                    "transport adapter rejected the room-owned source packet selection update"
                );
                continue;
            }
            applied_updates.push(update);
        }
        applied_updates
    }

    async fn apply_consumer_packet_updates(
        channel: &Channel,
        media_port: &impl MediaPort,
        updates: Vec<ConsumerPacketSelectionUpdate>,
    ) -> Vec<ConsumerPacketSelectionUpdate> {
        let mut applied_updates = Vec::with_capacity(updates.len());
        for update in updates {
            if let Some(packet_gate) = update.packet_gate()
                && media_port
                    .set_consumer_packet_gate(
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
                        packet_gate.clone(),
                    )
                    .await
                    .is_err()
            {
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
