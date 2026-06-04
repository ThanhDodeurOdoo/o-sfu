use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex as StdMutex},
};

use tokio::sync::RwLock;

use super::{
    cleanup::CleanupReconciler,
    definition::RoomDefinition,
    init::RoomInit,
    media_graph::ConsumerRouteState,
    operation::RoomUserOperation,
    placement::{
        LoadTriggeredPlacementState, RoomPlacementUsageSnapshot, RoomWorkerLoadContribution,
    },
    state::RoomState,
    transition::StagedPublishRegistry,
};
use crate::{
    RoomSpilloverMode, RoomWorkerPolicy, RuntimeFeatureFlags,
    engine::{
        AvailableFeatures, ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState,
        RoomInstanceId, UserId,
        diagnostics::{
            self, DiagnosticsIncomingBitrate, DiagnosticsQualitySummary, DiagnosticsSource,
            DiagnosticsStore, DiagnosticsUserTransport, DiagnosticsUserView,
        },
        media_transport::{
            ActiveSpeakerSourceDiagnostic, MediaTransport, TransportMediaId,
            TransportQualitySample, TransportSessionKey, TransportSourceKey,
        },
        metrics::RuntimeMetrics,
        recording::RecordingService,
        router_events::RoomRouterEventSink,
        source_model::UserStreamId,
        sync::lock_unpoisoned,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// room join failures after the target room has been resolved
pub enum RoomJoinError {
    RoomFull,
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// room-manager join failures before or during room admission
pub enum RoomManagerJoinError {
    MissingRoom,
    RoomFull,
    RouterState,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// incoming bitrate totals observed for one room user
pub struct IncomingBitrateSnapshot {
    pub total: u64,
    pub by_stream: BTreeMap<UserStreamId, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// lightweight media stats returned with room-manager snapshots
pub struct RoomUserStatsSnapshot {
    pub incoming_bitrate: IncomingBitrateSnapshot,
    pub count: u64,
    pub active_stream_counts: BTreeMap<UserStreamId, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// committed room media counts used for metrics deltas
pub struct RoomMediaCounts {
    pub publications: usize,
    pub subscriptions: usize,
}

/// live room instance with synchronous state and post-lock effect owners
pub struct Room {
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    pub(super) definition: RoomDefinition,
    pub(super) load_triggered_placement: StdMutex<LoadTriggeredPlacementState>,
    #[allow(
        dead_code,
        reason = "recording control-plane wiring is deferred until the replacement baseline is validated"
    )]
    pub(super) recording_service: Arc<RecordingService>,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) cleanup_reconciler: StdMutex<CleanupReconciler>,
    pub(super) staged_publish_registry: StdMutex<StagedPublishRegistry>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_after_reservation: StdMutex<Option<TransportMediaId>>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_cleanup_target: StdMutex<Option<TransportMediaId>>,
    pub(super) state: RwLock<RoomState>,
}

impl Room {
    pub(crate) fn new(init: RoomInit) -> Self {
        let RoomInit {
            runtime_context,
            runtime_policy,
            issuer,
            key,
            config,
            services,
        } = init;
        let definition =
            RoomDefinition::new(&runtime_context, &runtime_policy, issuer, key, config);
        let recording_service = Arc::new(RecordingService::new(
            definition.instance_id(),
            services.packet_sink_registry,
            Arc::clone(&services.metrics),
        ));
        let recording_event_sink = Arc::<RecordingService>::clone(&recording_service);
        let router_event_sink: Arc<dyn RoomRouterEventSink> = recording_event_sink;
        Self {
            diagnostics: services.diagnostics,
            definition,
            load_triggered_placement: StdMutex::new(LoadTriggeredPlacementState::default()),
            recording_service: Arc::clone(&recording_service),
            metrics: services.metrics,
            cleanup_reconciler: StdMutex::new(CleanupReconciler::default()),
            staged_publish_registry: StdMutex::new(StagedPublishRegistry::default()),
            #[cfg(test)]
            duplicate_staged_publish_after_reservation: StdMutex::new(None),
            #[cfg(test)]
            duplicate_staged_publish_cleanup_target: StdMutex::new(None),
            state: RwLock::new(RoomState::new(
                &runtime_context,
                runtime_policy.admission_policy,
                runtime_policy.media_limits,
                runtime_policy.router_rtp_capabilities,
                router_event_sink,
            )),
        }
    }

    pub(crate) fn user_operation<'a>(
        &'a self,
        user_id: &'a UserId,
        connection_id: ConnectionId,
        media_transport: &'a MediaTransport,
    ) -> RoomUserOperation<'a> {
        RoomUserOperation::new(self, user_id, connection_id, media_transport)
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    #[must_use]
    pub async fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.state
            .read()
            .await
            .transport_user_key(user_id, connection_id)
    }

    pub(in crate::engine::room) async fn placement_usage_snapshot(
        &self,
    ) -> RoomPlacementUsageSnapshot {
        self.state.read().await.placement_usage_snapshot()
    }

    pub(in crate::engine::room) async fn worker_load_contribution(
        &self,
    ) -> RoomWorkerLoadContribution {
        let (session_entries, consumer_entries) = {
            let state = self.state.read().await;
            (
                state
                    .transport_user_entries()
                    .into_iter()
                    .map(|(user_id, connection_id)| {
                        state
                            .transport_user_key(&user_id, connection_id)
                            .media_worker_id()
                    })
                    .collect::<Vec<_>>(),
                state
                    .transport_consumer_entries()
                    .into_iter()
                    .map(|(_, connection_id)| state.media_worker_id_for_connection(connection_id))
                    .collect::<Vec<_>>(),
            )
        };
        RoomWorkerLoadContribution {
            session_worker_ids: session_entries,
            consumer_worker_ids: consumer_entries,
        }
    }

    pub(in crate::engine::room) async fn reconcile_spillover_routers(&self) {
        let spillover = self.room_worker_policy().spillover();
        if matches!(&spillover, RoomSpilloverMode::StrictSingleRouter) {
            return;
        }
        let mut state = self.state.write().await;
        let mut placement = lock_unpoisoned(&self.load_triggered_placement);
        state.reconcile_spillover_routers(spillover, &mut placement);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::room) fn load_triggered_last_decision_reason(
        &self,
    ) -> Option<super::placement::RoomPlacementDecisionReason> {
        lock_unpoisoned(&self.load_triggered_placement).last_decision_reason()
    }

    pub async fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.state
            .read()
            .await
            .consumer_route_state(consumer_user_id, producer_user_id, stream_id)
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        self.definition.issuer()
    }

    #[must_use]
    pub fn key(&self) -> &str {
        self.definition.key()
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        self.definition.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state()
    }

    pub async fn user_snapshots_except(&self, excluded_user_id: &UserId) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .user_snapshots_except(excluded_user_id)
    }

    pub async fn router_rtp_capabilities(&self) -> o_sfu_router::MediaCapabilities {
        self.state.read().await.router_rtp_capabilities()
    }

    pub(crate) async fn session_stats_snapshot(
        &self,
        transport: &MediaTransport,
    ) -> RoomUserStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .into_iter()
            .map(|(user_id, connection_id)| state.transport_user_key(&user_id, connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let mut incoming_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total.as_bps(),
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_id) =
                state.producer_stream_id_for_transport_media_id(transport_media_id)
            else {
                continue;
            };
            let entry = incoming_bitrate.by_stream.entry(stream_id).or_default();
            *entry = entry.saturating_add(bits.as_bps());
        }
        let (count, active_stream_counts) = state.user_stats_counts();
        drop(state);
        RoomUserStatsSnapshot {
            incoming_bitrate,
            count,
            active_stream_counts,
        }
    }

    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    #[must_use]
    pub(crate) const fn recording_available(&self) -> bool {
        self.definition.recording_available()
    }

    #[must_use]
    pub async fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.state.read().await.assigned_primary_media_worker_id()
    }

    pub(in crate::engine::room) fn room_worker_policy(&self) -> RoomWorkerPolicy {
        self.definition.room_worker_policy()
    }

    #[must_use]
    pub(crate) fn instance_id(&self) -> RoomInstanceId {
        self.definition.instance_id()
    }

    #[must_use]
    pub(crate) fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.definition.feature_flags()
    }

    pub async fn diagnostics_user_views(
        &self,
        transport: &MediaTransport,
    ) -> Vec<DiagnosticsUserView> {
        let state = self.state.read().await;
        let session_entries = state.transport_user_entries();
        let session_keys = session_entries
            .iter()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let quality_by_session = transport
            .transport_quality_snapshot(&session_keys)
            .per_session
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let incoming_bitrate_by_session =
            state.diagnostics_incoming_bitrate_by_session(&transport_snapshot.per_media);
        let transport_by_session = session_entries
            .into_iter()
            .map(|(user_id, connection_id)| {
                let session_key = state.transport_user_key(&user_id, connection_id);
                let transport = DiagnosticsUserTransport {
                    connection_id: connection_id.as_u64(),
                    health: transport
                        .session_transport_health(&session_key)
                        .map(diagnostics::diagnostics_transport_health),
                    media_worker_id: session_key.media_worker_id().as_usize(),
                    quality_summary: diagnostics_quality_summary(
                        incoming_bitrate_by_session
                            .get(&user_id)
                            .cloned()
                            .unwrap_or_default(),
                        quality_by_session.get(&session_key).copied(),
                    ),
                };
                (user_id, transport)
            })
            .collect();
        state.diagnostics_user_views(
            state
                .assigned_primary_media_worker_id()
                .map_or(0, MediaWorkerId::as_usize),
            &transport_by_session,
        )
    }

    pub async fn diagnostics_sources(&self, transport: &MediaTransport) -> Vec<DiagnosticsSource> {
        let active_speaker_diagnostics = active_speaker_diagnostics_by_media(
            transport.active_speaker_diagnostic_snapshot().await,
        );
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .iter()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_source =
            state.diagnostics_incoming_bitrate_by_source(&transport_snapshot.per_media);
        let sources = state
            .diagnostics_source_media()
            .into_iter()
            .map(|media| {
                TransportSourceKey::new(
                    state.transport_user_key(&media.owner, media.connection),
                    media.media,
                )
            })
            .collect::<Vec<_>>();
        drop(state);
        let source_activity_by_media = transport
            .source_activity_snapshot(&sources)
            .await
            .per_media
            .into_iter()
            .map(|activity| (activity.transport_media_id(), activity))
            .collect::<BTreeMap<_, _>>();
        let state = self.state.read().await;
        state.diagnostics_sources(
            &incoming_bitrate_by_source,
            &active_speaker_diagnostics,
            &source_activity_by_media,
        )
    }

    pub async fn diagnostics_matching_user(
        &self,
        requested_user_id: &str,
        transport: &MediaTransport,
    ) -> Option<(DiagnosticsUserView, UserId)> {
        self.diagnostics_user_views(transport)
            .await
            .into_iter()
            .find(|user| user_id_matches(&user.user_id, requested_user_id))
            .map(|user| {
                let user_id = user.user_id.clone();
                (user, user_id)
            })
    }
}

fn active_speaker_diagnostics_by_media(
    diagnostics: Vec<ActiveSpeakerSourceDiagnostic>,
) -> BTreeMap<TransportMediaId, ActiveSpeakerSourceDiagnostic> {
    diagnostics
        .into_iter()
        .map(|diagnostic| (diagnostic.transport_media_id(), diagnostic))
        .collect()
}

fn diagnostics_quality_summary(
    current_incoming_bitrate: DiagnosticsIncomingBitrate,
    quality_sample: Option<TransportQualitySample>,
) -> DiagnosticsQualitySummary {
    let Some(quality_sample) = quality_sample else {
        return DiagnosticsQualitySummary {
            current_incoming_bitrate,
            sampled_metrics_available: false,
            latest_bwe_bps: None,
            rtt_ms: None,
            ingress_loss_ppm: None,
            egress_loss_ppm: None,
            egress_jitter_rtp_timestamp_units: None,
            sample_count: 0,
        };
    };
    DiagnosticsQualitySummary {
        current_incoming_bitrate,
        sampled_metrics_available: quality_sample.sample_count > 0,
        latest_bwe_bps: quality_sample.latest_bwe_bps,
        rtt_ms: quality_sample.rtt_ms,
        ingress_loss_ppm: quality_sample.ingress_loss_ppm,
        egress_loss_ppm: quality_sample.egress_loss_ppm,
        egress_jitter_rtp_timestamp_units: quality_sample.egress_jitter_rtp_timestamp_units,
        sample_count: quality_sample.sample_count,
    }
}

impl fmt::Debug for Room {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let media_worker_id = self
            .state
            .try_read()
            .ok()
            .and_then(|state| state.assigned_primary_media_worker_id())
            .map(MediaWorkerId::as_usize);
        formatter
            .debug_struct("Room")
            .field("instance_id", &self.definition.instance_id())
            .field("media_worker_id", &media_worker_id)
            .field("uuid", &self.definition.uuid())
            .field("issuer", &self.definition.issuer())
            .field("web_rtc_enabled", &self.definition.web_rtc_enabled())
            .finish_non_exhaustive()
    }
}

fn user_id_matches(user_id: &UserId, requested_user_id: &str) -> bool {
    match user_id {
        UserId::Integer(value) => value.to_string() == requested_user_id,
        UserId::String(value) => value == requested_user_id,
    }
}
