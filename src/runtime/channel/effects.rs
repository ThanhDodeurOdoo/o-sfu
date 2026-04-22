//! Effect-plan system for `Channel`.
//!
//! `ChannelState` has the pure room mutation and validation work under lock,
//! while this module has the side effects that must happen after that lock is
//! released. The goal is to keep the call sites in `media.rs`
//! `media_transaction.rs` and `source_packet_policy.rs` on one consistent
//! shape:
//!
//! 1. read or mutate channel state under lock
//! 2. build a tpyed plan describing the side effects for that transition
//! 3. execute transport calls, diagnostics writes, and fanout after unlock
//!
//!
//! The important invariant is not that every effect in the channel shares one algebra but
//! that each transition keeps its ordering and failure handling explicit at the
//! boundary where room state meets transport state

use tracing::warn;

use crate::runtime::ConnectionId;
use crate::runtime::diagnostics::DiagnosticsEventData;
use crate::runtime::telemetry::schema::event as telemetry_event;
use crate::runtime::transport_adapter::{
    ActiveSpeakerSource, MediaPort, ObservabilityPort, SourcePacketGate, TransportMediaId,
};
use o_sfu_protocol::shared::{SessionId, StreamType};

use super::{
    Channel, ChannelMediaCounts,
    state::{
        ChannelState, ConsumerBootstrapOrigin, ConsumerRouteUpdate, FeaturedSessionUpdate,
        PendingConsumerBootstrap, PendingConsumerBootstrapTarget, PlannedConsumerBootstrap,
        PlannedSubscriptionChange, PreparedConsumerBootstrap, SourcePacketSelectionUpdate,
        TransportMediaRemoval,
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
        target_session_id: &SessionId,
        route_updates: Vec<ConsumerRouteUpdate>,
    ) -> Self {
        let transport_ops = route_updates
            .into_iter()
            .map(|route_update| SubscriptionTransportOp {
                consumer_session_id: session_id.clone(),
                consumer_connection_id: route_update.consumer_connection_id(),
                consumer_media: route_update.consumer_media(),
                producer_session_id: target_session_id.clone(),
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
                    serde_json::to_value(target_session_id).unwrap_or(serde_json::Value::Null),
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

    /// Apply thee subscription decision to the transport adapter.
    ///
    /// A resumed video route needs more than an `active=true` flip: after a
    /// long pause the receiver may need a fresh decodable frame before it can
    /// render again. so succesful camera/screen resumes trigger an
    /// immediate keyframe request on the underlying consumer route.
    pub(super) async fn execute(self, channel: &Channel, media_port: &impl MediaPort) {
        if let Some(media_count_delta) = self.media_count_delta {
            media_count_delta.record(channel);
        }
        for transport_op in self.transport_ops {
            // Transport activity is best-efort here: the channel intent has
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
    /// This step intentionally not request a keyframe.
    /// Fresh subscribers need that refresh only after the receiver has applied the
    /// relevant SDP answer, which is handled by the later session-negotiation
    /// callbacks
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
pub(super) enum StagedPublishCommitEffectPlan {
    Commit {
        producer_id: String,
        media_count_delta: MediaCountDelta,
        consumer_targets: Vec<PendingConsumerBootstrapTarget>,
        diagnostics: DiagnosticsEventData,
    },
    Reject {
        session_id: SessionId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    },
}

impl StagedPublishCommitEffectPlan {
    pub(super) fn committed(
        producer_id: String,
        media_count_delta: (ChannelMediaCounts, ChannelMediaCounts),
        consumer_targets: Vec<PendingConsumerBootstrapTarget>,
        diagnostics: DiagnosticsEventData,
    ) -> Self {
        Self::Commit {
            producer_id,
            media_count_delta: MediaCountDelta::new(media_count_delta.0, media_count_delta.1),
            consumer_targets,
            diagnostics,
        }
    }

    pub(super) fn rejected(
        session_id: SessionId,
        connection_id: ConnectionId,
        transport_media_id: TransportMediaId,
    ) -> Self {
        Self::Reject {
            session_id,
            connection_id,
            transport_media_id,
        }
    }

    pub(super) async fn execute(
        self,
        channel: &Channel,
        observability_port: &impl ObservabilityPort,
        media_port: &impl MediaPort,
    ) -> Option<String> {
        match self {
            Self::Commit {
                producer_id,
                media_count_delta,
                consumer_targets,
                diagnostics,
            } => {
                media_count_delta.record(channel);
                // Publish commit must update room-owned source selection before
                // the newly publishe track fans out to consumers, otherwise a
                // multi-party camera publish can bootstrap consumers against a
                // stale gate decision
                channel
                    .sync_source_packet_selection_policy(Some(observability_port), media_port)
                    .await;
                channel
                    .bootstrap_consumer_targets(
                        media_port,
                        ConsumerBootstrapOrigin::Publish,
                        consumer_targets,
                    )
                    .await;
                channel.diagnostics.record(diagnostics);
                Some(producer_id)
            }
            Self::Reject {
                session_id,
                connection_id,
                transport_media_id,
            } => {
                channel
                    .cleanup_transport_media(
                        &session_id,
                        connection_id,
                        transport_media_id,
                        media_port,
                        "transport adapter failed to remove published transport media after channel commit failed",
                    )
                    .await;
                None
            }
        }
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

#[derive(Debug, Default)]
pub(super) struct SourcePacketPolicyEffectPlan {
    source_packet_updates: Vec<SourcePacketSelectionUpdate>,
    featured_session_updates: Vec<FeaturedSessionUpdate>,
}

impl SourcePacketPolicyEffectPlan {
    pub(super) fn from_state(
        state: &ChannelState,
        active_speaker_sources: &[ActiveSpeakerSource],
    ) -> Self {
        Self {
            source_packet_updates: state.source_packet_selection_updates(active_speaker_sources),
            featured_session_updates: state.featured_session_updates(active_speaker_sources),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.source_packet_updates.is_empty() && self.featured_session_updates.is_empty()
    }

    pub(super) async fn execute(self, channel: &Channel, media_port: &impl MediaPort) {
        let mut applied_source_packet_updates =
            Vec::with_capacity(self.source_packet_updates.len());
        for update in self.source_packet_updates {
            if media_port
                .set_source_packet_gate(
                    &channel.transport_session_key(
                        update.owner_session_id(),
                        update.owner_connection_id(),
                    ),
                    update.transport_media_id(),
                    update.selection().map(SourcePacketGate::from),
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
            applied_source_packet_updates.push(update);
        }
        if applied_source_packet_updates.is_empty() && self.featured_session_updates.is_empty() {
            return;
        }
        let info_fanout = {
            let mut state = channel.state.write().await;
            // Only the updates accepted by the transport are commited back into
            // room state. Featured-session projection still commits from the
            // same transition so the outward layout snapshot stays derived from
            // the same active-speaker observation.
            state.commit_source_packet_selection_updates(&applied_source_packet_updates);
            state.commit_featured_session_updates(&self.featured_session_updates)
        };
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
    }
}
