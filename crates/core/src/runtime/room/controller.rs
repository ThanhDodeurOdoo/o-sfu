//! Room runtime layer: membership, bootstrap orchestration and room-local state.
//!
//! Internal modules:
//! - `manager`: server-global room lookup, creation and cleanup coordination
//! - `membership`: join/leave, user-info fan-out and transport readiness
//! - `media`: producer/consumer bootstrap plus upload/download activity transitions
//! - `outbound`: shared outbound fan-out helpers for user handlers
//! - `state`: room-local mutable state and internal bootstrap bookkeeping
//! - `router_state`: post-auth bridge from signaling user ids into the router core
//! - `topology`: room-local routing placement boundary
//! - `rtp_capabilities`: default router RTP capability surface
//! - signaling edges own the protocol wire mapping. the room boundary consumes
//!   browser codec baseline RTP capabilities, negotiated parameters and track bootstrap data
//!
//! `controller.rs` is the public face of the runtime `room/` domain. It
//! defines the room facade itself (`Room`) plus room-facing query and error
//! types. construction inputs live in `init`, placement inputs live in
//! `placement` and websocket-user handoff types live in `outbound`.
//!
//! The file exists to keep one clear contract at the room boundary:
//!
//! - immutable room identity lives in `RoomDefinition`, while committed
//!   placement lookup lives in `RoomPlacementState`
//! - mutable membership and media topology live behind `RoomState`
//! - websocket and transport work must happen after room locks are released
//! - signaling code consumes high-level room events instead of reaching into
//!   room internals or depending on router-shaped state directly

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
    media_transaction::PendingPublishTransactions,
    operation::RoomUserOperation,
    placement::{
        LoadTriggeredPlacementState, RoomPlacementState, RoomPlacementUsageSnapshot,
        RoomWorkerLoadContribution,
    },
    state::{ConsumerRouteState, ConsumerRouteTransportRef, RoomState},
};
use crate::{
    RoomSpilloverMode, RoomWorkerPolicy, RuntimeFeatureFlags,
    runtime::{
        AvailableFeatures, ConnectionId, PeerSnapshot, RecordingState, RoomInstanceId, UserId,
        diagnostics::{
            self, DiagnosticsIncomingBitrate, DiagnosticsQualitySummary, DiagnosticsSource,
            DiagnosticsStore, DiagnosticsUserTransport, DiagnosticsUserView,
        },
        media_transport::{
            ActiveSpeakerSourceDiagnostic, MediaTransport, TransportConsumerRoute,
            TransportMediaId, TransportQualitySample, TransportSessionKey, TransportSourceKey,
        },
        metrics::RuntimeMetrics,
        recording::RecordingService,
        router_events::RoomRouterEventSink,
        source_model::UserStreamId,
        sync::lock_unpoisoned,
    },
};

/// Join failures produced by one live room instance.
///
/// These errors come from room-local admission or state-sync rules after the
/// room has already been resolved by the manager.
///
/// # Error handling
///
/// `RoomFull` is an expected domain rejection. `RouterState` means the join
/// could not be committed cleanly inside the room and should be treated as an
/// internal failure by outer layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomJoinError {
    /// The room admission policy rejected one more concurrent user.
    ///
    /// This is a stable domain rejection, not an infrastructure failure.
    RoomFull,
    /// Room state and router state could not be kept in sync during the join.
    ///
    /// Callers should treat this as an internal failure because the join could
    /// not land cleanly across the room's state boundary.
    RouterState,
}

/// Join failures produced by the process-global room manager.
///
/// This extends [`RoomJoinError`] with process-level lookup failure. By the
/// time callers see this enum they know whether the failure happened before a
/// room was found or inside the room's own join transition.
///
/// This split matters because the runtime makes different decisions for stale
/// room identity versus a real room-level failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomManagerJoinError {
    /// The requested room UUID no longer points at a live room.
    ///
    /// This can happen when the caller holds stale room identity while the
    /// manager has already removed the old empty room instance.
    MissingRoom,
    /// The targeted room reached its configured user limit.
    RoomFull,
    /// The targeted room failed to apply the join to its router-backed state.
    RouterState,
}

/// Best-effort inbound bitrate totals grouped by orchestration stream id.
///
/// These numbers are cold-path observability data assembled from transport
/// snapshots plus room-owned producer metadata. They are not used for routing
/// decisions in the hot path.
///
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingBitrateSnapshot {
    /// Sum reported by the transport layer for every known media flow.
    ///
    /// This can be larger than the sum of the typed buckets if transport state
    /// still contains media that room state no longer classifies.
    pub total: u64,
    /// Bitrate grouped by the stream id supplied by orchestration.
    pub by_stream: BTreeMap<UserStreamId, u64>,
}

/// Cold-path observability snapshot for one live room.
///
/// This is the compact room-level view used by compatibility stats and manager
/// listings. It intentionally avoids exposing per-user details.
///
/// See [`Room::diagnostics_user_views`] for the richer per-user
/// inspection surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomUserStatsSnapshot {
    /// Aggregate inbound bitrate across the room's current transport media.
    ///
    /// The buckets reflect what the room currently believes each producer is.
    pub incoming_bitrate: IncomingBitrateSnapshot,
    /// Total live user count in the room.
    pub count: u64,
    /// Distinct users with at least one active publication for each stream id.
    pub active_stream_counts: BTreeMap<UserStreamId, u64>,
}

/// Cheap publication and subscription counters used around room transitions.
///
/// These are mostly used for diagnostics and telemetry emitted around room
/// effect execution, so transitions can record before/after media shape.
///
/// They are separate from the richer diagnostics types because
/// many room transitions only need a small before/after summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RoomMediaCounts {
    /// Number of live published streams in room state.
    ///
    /// A staged publish that has not been committed yet does not count here.
    pub publications: usize,
    /// Number of live consumer routes in room state.
    ///
    /// This counts room-owned consumer state, not pending bootstrap work.
    pub subscriptions: usize,
}

/// Represents one logical call room.
///
/// `Room` owns immutable room definition plus the guarded mutable state needed to run
/// membership, routing and recording for that room. Callers are expected to express
/// room-level intents through this facade, while process-level lookup and current-room
/// liveness stay in [`super::manager::RoomManager`].
///
/// The main invariant is that this facade keeps room state authoritative while
/// transport work happens after the relevant locks are released. That is why it
/// stores both a pure `RoomState` model and the async staging state needed
/// around publish and recording workflows.
///
/// # Concurrency
///
/// The room uses a `RwLock<RoomState>` for the pure mutable model and a
/// separate `Mutex<PendingPublishTransactions>` for staged publish work that
/// crosses async negotiation boundaries. Callers should treat all public async
/// methods on `Room` as cold-path orchestration entrypoints, not as hot-path
/// packet-loop helpers.
pub struct Room {
    /// Room-scoped diagnostics sink for lifecycle and media events
    ///
    /// This is written from room orchestration paths, not from the pure room
    /// model itself.
    pub(super) diagnostics: Arc<DiagnosticsStore>,
    /// Immutable identity and feature metadata for the room lifetime.
    ///
    /// `definition` is the stable read-only half of the room, while `state`
    /// contains the mutable membership and media graph.
    pub(super) definition: RoomDefinition,
    /// Mutable placement lookup for committed room connections.
    ///
    /// The pure topology owns router execution state. This state keeps the
    /// full committed placement needed to build session keys after placement
    /// has been committed.
    pub(super) placement_state: RoomPlacementState,
    /// Room-local memory for load-triggered placement hysteresis.
    pub(super) load_triggered_placement: StdMutex<LoadTriggeredPlacementState>,
    #[allow(
        dead_code,
        reason = "recording control-plane wiring is intentionally deferred until the replacement baseline is validated"
    )]
    /// Room-owned recording service shared with topology observers.
    ///
    /// The service is injected into the topology side so recording can observe
    /// routed media without making router state recording-aware.
    pub(super) recording_service: Arc<RecordingService>,
    /// Process-wide metrics catalog used by room-facing orchestration.
    ///
    /// Keeping this here avoids threading metrics handles through every room
    /// transition call that may want to report lifecycle changes.
    pub(super) metrics: Arc<RuntimeMetrics>,
    /// Room-owned reconciliation queue for transport cleanup that failed after
    /// state ownership was already removed.
    ///
    /// This lives on `Room` instead of `RoomState` because retry bookkeeping
    /// must survive the state transition that forgot the user or media object.
    /// Callers may lock it for short synchronous updates only, then must drop
    /// the guard before awaiting media transport work.
    pub(super) cleanup_reconciler: StdMutex<CleanupReconciler>,
    /// Staged publish reservations that live across the offer/answer gap.
    ///
    /// This stays outside `RoomState` because it tracks async transport work
    /// that has not become live room state yet. A publish only becomes real
    /// room state after the later commit path succeeds.
    pub(super) pending_publish_transactions: StdMutex<PendingPublishTransactions>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_after_reservation: StdMutex<Option<TransportMediaId>>,
    #[cfg(test)]
    pub(super) duplicate_staged_publish_cleanup_target: StdMutex<Option<TransportMediaId>>,
    /// Pure room state plus room-owned indexes.
    ///
    /// Callers must snapshot what they need and drop this lock before async
    /// transport or websocket work. This keeps room transitions deterministic
    /// and prevents async transport behavior from shaping the state model.
    pub(super) state: RwLock<RoomState>,
}

impl Room {
    /// Build one live room from semantic initialization input.
    ///
    /// Construction wires the immutable room definition, the room-owned state
    /// model and the recording observer surface together once. After that,
    /// higher-level runtime code should interact with the room through intent
    /// methods such as join, leave, publish, subscribe and stats queries.
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
        let instance_id = definition.instance_id();
        Self {
            diagnostics: services.diagnostics,
            definition,
            placement_state: RoomPlacementState::new(
                instance_id,
                runtime_context.local_routers().clone(),
            ),
            load_triggered_placement: StdMutex::new(LoadTriggeredPlacementState::default()),
            recording_service: Arc::clone(&recording_service),
            metrics: services.metrics,
            cleanup_reconciler: StdMutex::new(CleanupReconciler::default()),
            pending_publish_transactions: StdMutex::new(PendingPublishTransactions::default()),
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
    /// Stable UUID for this live room instance.
    ///
    /// This is the public identity used by diagnostics and manager lookups. It
    /// is generated once when the room is created and stays fixed until the
    /// room is removed
    ///
    /// It should be treated as instance identity, not as the user-facing
    /// compatibility identity. For that, use [`Self::issuer`].
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    #[must_use]
    /// Build the transport-layer user key for one live room user.
    ///
    /// This helper keeps transport addressing derived from the room's stable
    /// runtime placement plus the current `(user_id, connection_id)` pair.
    /// Callers should prefer this over rebuilding transport keys themselves.
    pub fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.placement_state
            .transport_user_key(user_id, connection_id)
    }

    #[must_use]
    pub(in crate::runtime::room) fn transport_consumer_route(
        &self,
        route: &ConsumerRouteTransportRef,
    ) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.transport_user_key(route.consumer_user_id(), route.consumer_connection_id()),
            route.consumer_media(),
            TransportSourceKey::new(
                self.transport_user_key(route.source_user_id(), route.source_connection_id()),
                route.source_media(),
            ),
        )
    }

    pub(in crate::runtime::room) fn placement_usage_snapshot(&self) -> RoomPlacementUsageSnapshot {
        self.placement_state.usage_snapshot()
    }

    pub(in crate::runtime::room) async fn worker_load_contribution(
        &self,
    ) -> RoomWorkerLoadContribution {
        let (session_entries, consumer_entries) = {
            let state = self.state.read().await;
            (
                state.transport_user_entries(),
                state.transport_consumer_entries(),
            )
        };
        RoomWorkerLoadContribution {
            session_workers: session_entries
                .into_iter()
                .map(|(user_id, connection_id)| {
                    self.transport_user_key(&user_id, connection_id)
                        .media_worker_id()
                })
                .collect(),
            consumer_workers: consumer_entries
                .into_iter()
                .map(|(_, connection_id)| {
                    self.placement_state
                        .media_worker_id_for_connection(connection_id)
                })
                .collect(),
        }
    }

    pub(in crate::runtime::room) async fn reconcile_spillover_routers(&self) {
        let spillover = self.room_worker_policy().spillover();
        if matches!(&spillover, RoomSpilloverMode::StrictSingleRouter) {
            return;
        }
        let mut state = self.state.write().await;
        let mut placement = lock_unpoisoned(&self.load_triggered_placement);
        state.reconcile_spillover_routers(spillover, &mut placement);
    }

    #[cfg(test)]
    pub(in crate::runtime::room) fn load_triggered_last_decision_reason(
        &self,
    ) -> Option<super::placement::RoomPlacementDecisionReason> {
        lock_unpoisoned(&self.load_triggered_placement).last_decision_reason()
    }

    /// Current route state for one consumer or producer pair
    ///
    /// This is mainly used by orchestration and diagnostics code that needs to
    /// know whether a logical room subscription currently resolves to a live,
    /// paused, or otherwise tracked consumer route.
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
    /// Issuer used as the room's stable compatibility identity.
    ///
    /// This is the identity callers usually care about when talking in Odoo
    /// terms. The manager uses it for idempotent room lookup and creation.
    ///
    /// Multiple live room instances should not share the same issuer inside one
    /// manager at the same time.
    pub fn issuer(&self) -> &str {
        self.definition.issuer()
    }

    #[must_use]
    /// Room key configured at room creation time.
    ///
    /// This is preserved as immutable room metadata and can later be used by
    /// control-plane or permission flows that need room-scoped secrets
    ///
    /// The room itself does not reinterpret or rotate this value.
    pub fn key(&self) -> &str {
        self.definition.key()
    }

    #[must_use]
    /// Feature flags this room currently advertises to clients.
    ///
    /// This is the room-facing compatibility view derived from the runtime
    /// policy and room config, not a reflection of every internal capability.
    ///
    /// Outer layers should prefer this method over reconstructing feature flags
    /// from runtime config because it already accounts for room-local toggles
    /// such as `web_rtc_enabled`.
    pub fn available_features(&self) -> AvailableFeatures {
        self.definition.available_features()
    }

    /// Current recording state as projected by room-owned state.
    ///
    /// Callers should treat this as the authoritative room view. It may lag
    /// behind lower-level media events only until the room transtion that
    /// records those changes has completed
    ///
    /// This is a room-level state query, not a direct peek into recording I/O.
    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state()
    }

    /// Snapshot of every user except the requested user.
    ///
    /// This is the room-facing view used when a user needs to learn about
    /// the rest of the room without receiving itself back as a user entry.
    /// The shape is already projected into protocol-facing `PeerSnapshot`
    /// values, so callers do not need to rebuild that view from raw room state.
    pub async fn user_snapshots_except(&self, excluded_user_id: &UserId) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .user_snapshots_except(excluded_user_id)
    }

    /// Router-native RTP capability surface exposed by this room.
    ///
    /// This is mainly used by diagnostics or negotiation-adjacent code that
    /// needs to understand what the room can currently negotiate.
    ///
    /// The result comes from room-owned state because it is part of the room's
    /// active negotiation baseline, not only static runtime config.
    pub async fn router_rtp_capabilities(&self) -> o_sfu_router::MediaCapabilities {
        self.state.read().await.router_rtp_capabilities()
    }

    /// Best-efort stats snapshot used by compatibility stats surfaces.
    ///
    /// Bitrate totals come from the transport boundary, while the per-stream
    /// split and user counts come from current room state
    ///
    /// This is a cold-path query. It snapshots room state first, then asks the
    /// transport observability boundary for bitrate data, so it is best-effort
    /// rather than one global atomic instant.
    pub(crate) async fn session_stats_snapshot(
        &self,
        transport: &MediaTransport,
    ) -> RoomUserStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .into_iter()
            .map(|(user_id, connection_id)| self.transport_user_key(&user_id, connection_id))
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
    /// Whether this room advertises RTC support to clients.
    ///
    /// This is a cheap immutable view over room configuration. It tells callers
    /// what the room contract is, not whether one specific user is currently
    /// connected to transport
    pub fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    #[must_use]
    /// Whether the room can accept production recording requests.
    ///
    /// This reflects the persistent recording backend gate, not whether
    /// recording is currently running. For the live room state, use
    /// [`Self::recording_state`].
    pub(crate) const fn recording_available(&self) -> bool {
        self.definition.recording_available()
    }

    #[must_use]
    /// Media worker that owns this room's transport users.
    ///
    /// Runtime diagnostics and transport command paths use this to route work
    /// to the correct RTC worker.
    pub fn media_worker_id(&self) -> usize {
        self.placement_state.media_worker_id()
    }

    pub(in crate::runtime::room) fn room_worker_policy(&self) -> RoomWorkerPolicy {
        self.definition.room_worker_policy()
    }

    #[must_use]
    /// Runtime-local instance id for this live room.
    ///
    /// Unlike [`Self::uuid`] and [`Self::issuer`], this id is only meaningful
    /// inside the current runtime process.
    pub(crate) fn instance_id(&self) -> RoomInstanceId {
        self.definition.instance_id()
    }

    #[must_use]
    /// Feature-flag policy attached to this room at creation time
    ///
    /// This is the raw runtime policy view. External callers usually want
    /// [`Self::available_features`] instead because it is already projected into
    /// the compatibility-facing surface.
    pub(crate) fn feature_flags(&self) -> RuntimeFeatureFlags {
        self.definition.feature_flags()
    }

    /// Merge room state and transport observability into diagnostics user views.
    ///
    /// This is richer than `session_stats_snapshot` because it builds a
    /// per-user view with transport health and current incoming bitrate.
    pub async fn diagnostics_user_views(
        &self,
        transport: &MediaTransport,
    ) -> Vec<DiagnosticsUserView> {
        let state = self.state.read().await;
        let session_entries = state.transport_user_entries();
        let session_keys = session_entries
            .iter()
            .map(|(user_id, connection_id)| self.transport_user_key(user_id, *connection_id))
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
                let session_key = self.transport_user_key(&user_id, connection_id);
                let transport = DiagnosticsUserTransport {
                    connection_id: connection_id.as_u64(),
                    health: transport
                        .session_transport_health(&session_key)
                        .map(diagnostics::diagnostics_transport_health),
                    media_worker_id: session_key.media_worker_id(),
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
            self.placement_state.media_worker_id(),
            &transport_by_session,
        )
    }

    /// Builds the live source inventory for operator diagnostics.
    ///
    /// Source descriptors are room-domain objects, while bitrate samples come
    /// from `MediaTransport`. This method keeps the merge at
    /// the room boundary so diagnostics routes do not inspect room state
    /// or transport internals directly.
    pub async fn diagnostics_sources(&self, transport: &MediaTransport) -> Vec<DiagnosticsSource> {
        let active_speaker_diagnostics = active_speaker_diagnostics_by_media(
            transport.active_speaker_diagnostic_snapshot().await,
        );
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .iter()
            .map(|(user_id, connection_id)| self.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_source =
            state.diagnostics_incoming_bitrate_by_source(&transport_snapshot.per_media);
        state.diagnostics_sources(&incoming_bitrate_by_source, &active_speaker_diagnostics)
    }

    /// Resolve a diagnostics request path agaisnt either nummeric or string user ids.
    ///
    /// Diagnostics routes take one raw path segment, but room users may use
    /// either integer or string ids, so this helper normalizes that lookup
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
        formatter
            .debug_struct("Room")
            .field("instance_id", &self.definition.instance_id())
            .field("media_worker_id", &self.placement_state.media_worker_id())
            .field("uuid", &self.definition.uuid())
            .field("issuer", &self.definition.issuer())
            .field("web_rtc_enabled", &self.definition.web_rtc_enabled())
            .finish_non_exhaustive()
    }
}

/// Diagnostics routes take raw path strings, so accept either `UserId` shape.
fn user_id_matches(user_id: &UserId, requested_user_id: &str) -> bool {
    match user_id {
        UserId::Integer(value) => value.to_string() == requested_user_id,
        UserId::String(value) => value == requested_user_id,
    }
}
