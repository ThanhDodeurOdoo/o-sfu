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
//! # Boundary role
//!
//! `controller.rs` is the public face of the runtime `room/` domain. It
//! defines the room facade itself (`Room`) plus the small set oftypes that
//! callers need to create a room, join it, query it, or project its outbound
//! work into websocket-user handling.
//!
//! The file exists to keep one clear contract at the room boundary:
//!
//! - imutable room identity and runtime placement live in `RoomDefinition`
//!   and the small policy/context/config types defined here
//! - mutable membership and media topology live behind `RoomState`
//! - websocket and transport work must happen after room locks are released
//! - signaling code consumes high-level room events instead of reaching into
//!   room internals or depending on router-shaped state directly
//!
//! If a caller wants to know "what is a room, what can I ask from it and
//! what kind of work can it send back to a user?" this is the file that
//! should answer that without requiring a deep read of the rest of `room/`.

use std::{collections::BTreeMap, fmt, sync::Arc};

use o_sfu_protocol::{
    shared::{AvailableFeatures, RecordingState, StreamType, UserId},
    signaling::PeerSnapshot,
};
use o_sfu_router::RouterId;
use tokio::sync::{Mutex, RwLock};

use super::{
    definition::RoomDefinition,
    events::RoomEventMessage,
    lifecycle::UserCloseReason,
    media_transaction::PendingPublishTransactions,
    state::{ConsumerRouteState, RemoteTrackBootstrap, RoomState},
};
use crate::{
    config::RuntimeFeatureFlags,
    runtime::{
        ConnectionId, RoomInstanceId,
        diagnostics::{
            DiagnosticsQualitySummary, DiagnosticsSource, DiagnosticsStore,
            DiagnosticsUserTransport, DiagnosticsUserView,
        },
        metrics::RuntimeMetrics,
        recording::{MediaSource, MediaTap, RecordingService},
        transport_adapter::{
            ActiveSpeakerSourceDiagnostic, ObservabilityPort, TransportMediaId, TransportSessionKey,
        },
    },
};

/// Delta sent from room state to one post-auth user's track projection.
///
/// This keeps the room boundary independent from wire `mid` assignment. The
/// websocket user projects the update onto its own current track bindings.
/// The room only talks in terms of publisher user ids and logical stream
/// kinds, which keeps room state independent from renegotiation details
#[derive(Debug, Clone)]
pub(crate) struct TrackBindingUpdate {
    /// Publisher whose projected track set changed.
    ///
    /// The receiver uses this together with `stream_type` to find its current
    /// browser-side binding for that remote track.
    pub(crate) user_id: UserId,
    /// Which logical stream changd for that publisher.
    ///
    /// The room never exposes transport media ids here because bindings are
    /// reprojected per user after negotiation.
    pub(crate) stream_type: StreamType,
    /// `Some(active)` updates an existing binding. `None` removes it
    ///
    /// `None` is used for unpublish or teardown paths where the receiver must
    /// drop the binding entirely and may need renegotiation.
    pub(crate) active: Option<bool>,
}

/// Outbound work the room wants one websocket user to perform
///
/// The room stays protocol-neutral here. Post-auth user code translates
/// each variant into the wire messages or local actions the socket needs.
///
/// This enum is the main handoff from room-owned state transitions to
/// user-owned protocol handling. It is intentionally small: the room says
/// what changed, while the websocket user decides how to express that over
/// the current connection.
///
/// # Design note
///
/// `UserOutbound` exists so the room can stay focused on membership and
/// media semantics instead of websocket mechanics. The room never writes a
/// close frame, never serializs a JSON envelope and never mutates browser
/// track bindings directly. It emits one of these values and leaves the
/// user-local projection to post-auth websocket code.
#[derive(Debug, Clone)]
pub enum UserOutbound {
    /// Fan-out payload that can be translated directly into server messages.
    ///
    /// This is the common path for peer joins, leaves, user-info updates,
    /// recording state fan-out and other room-level notifications.
    Message(RoomEventMessage),
    /// Imperative request that needs user-local bootstrap or renegotiation work.
    ///
    /// The room uses this when pure fan-out is not enough and the targeted
    /// user must do extra local work such as bootstrapping a new remote
    /// track on its own transport user.
    Request(Box<RoomEventRequest>),
    /// Minimal track-binding delta for the user's track projection.
    ///
    /// This lets the webssocket user update its track projection without
    /// rebuilding the whole user snapshot for every small media change.
    TrackBindingUpdate(TrackBindingUpdate),
    /// Ask the user owner to close the websocket with the mapped reason.
    ///
    /// The room decides that the user must stop, but the websocket edge
    /// still owns the actual close-frame mapping and socket shutdown.
    Close(UserCloseReason),
}

/// User-local work requested by the chanel after a room-state transition.
///
/// These requests are more specific than `RoomEventMessage`. They represent
/// work that must run in the context of one live websocket user because it
/// depends on that user's transport state, negotiation state, or browser
/// projection state
///
/// # Why this is a separate enum
///
/// A plain fan-out message is enough for room notifications such as "peer
/// joined" or "recording state changed". It is not enough for flows where the
/// targeted user must execute a local protocol step. Those flows go through
/// `RoomEventRequest` so the room can ask for a concrete action without
/// taking over websocket-user orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoomEventRequest {
    /// Bootstrap one newly visible remote track on the targeted user.
    ///
    /// The chanel has already decided that the consumer route should exist.
    /// The user now has to materialize the matching remote track details in
    /// its own post-auth flow.
    BootstrapRemoteTrack(RemoteTrackBootstrap),
}

/// Join failures produced by one live chanel instance.
///
/// These errors come from room-local admission or state-sync rules after the
/// chanel has already been resolved by the manager.
///
/// # Error handling guidance
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

/// Admission limits that stay fixed for one chanel lifetime.
///
/// This is kept separate from the wider runtime policy because admission is a
/// narrow concern with its own tests and state checks.
///
/// The policy is passed into `RoomState` at construction time and then
/// treated as immutable room configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoomAdmissionPolicy {
    /// Maximum number of live users the room accepts at once.
    ///
    /// Replaced connections still consume this budget until the room transition
    /// finishes and the old live user has been removed
    pub(crate) max_sessions: usize,
}

impl RoomAdmissionPolicy {
    #[must_use]
    pub(crate) const fn new(max_sessions: usize) -> Self {
        Self { max_sessions }
    }
}

/// Stable runtime placement chosen when the chanel is created.
///
/// These values identify where the room lives inside the current process.
/// Unlike room identity, they are runtime-local and mainly matter for routing,
/// transport ownership, diagnostics correlation and teardown.
///
/// Callers outside the runtime should generally care more about `issuer` and
/// `uuid` than about this placement data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoomRuntimeContext {
    /// Unique live instance id used to correlate runtime events and health.
    ///
    /// A recreated room with the same issuer still gets a fresh instance id.
    pub(crate) instance: RoomInstanceId,
    /// Worker that owns the transport users for this room.
    ///
    /// Chanel-level transport keys include this so observability and command
    /// paths can address the correct worker directly.
    pub(crate) media_worker: usize,
    /// Pure router instance backing this chanel's topology.
    ///
    /// The room topology stays distinct from signaling identity, so router
    /// placement can evolve without changing the outward room contract.
    pub(crate) router: RouterId,
}

/// Stable runtime policy bundle shared by the room and its state model.
///
/// This groups the room rules that are fixed for the room lifetime and read by
/// more than one boundary during join, negotiation and observability work.
///
/// `RoomRuntimeContext` says where the room lives. `RoomRuntimePolicy`
/// says which rules and capabilities apply once it lives there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoomRuntimePolicy {
    /// Room-level admission limits enforced by room state.
    pub(crate) admission_policy: RoomAdmissionPolicy,
    /// Feature surface the room advertises to clients.
    ///
    /// This is part of the observable room contract and feeds
    /// `available_features()` on the public `Chanel` facade.
    pub(crate) feature_flags: RuntimeFeatureFlags,
    /// Router-native capability baseline used for negotiation and bootstrap.
    ///
    /// The chanel consumes router-native capabilities here so signaling code
    /// does not have to leak wire-shaped capability bags into room state.
    pub(crate) router_rtp_capabilities: o_sfu_router::MediaCapabilities,
}

impl RoomRuntimePolicy {
    #[must_use]
    pub(crate) fn new(
        admission_policy: RoomAdmissionPolicy,
        feature_flags: RuntimeFeatureFlags,
        router_rtp_capabilities: o_sfu_router::MediaCapabilities,
    ) -> Self {
        Self {
            admission_policy,
            feature_flags,
            router_rtp_capabilities,
        }
    }
}

/// External room config passed in from the HTTP/runtime edge.
///
/// This type exists to keep room identity separate from operator-facing knobs
/// and compatibility toggles that may be chosen per room at creation time.
///
/// Unlike `ChanelRuntimePolicy`, this config is part of the per-room create
/// request shape rather than one validated runtime-wide policy bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoomConfig {
    /// Whether this room should expose WebRTC to clients at all.
    ///
    /// When false, the room still exists as a room identity but advertises
    /// that RTC is unavailable to clients.
    pub(crate) web_rtc_enabled: bool,
    /// Compatibility knob from `/v1/room` for recording-enabled rooms.
    ///
    /// The current chanel runtime treats this as an enable flag, not as a
    /// lasting recorder routing destination. The string is preserved because it
    /// matches the current HTTP contract even though the runtime does not route
    /// recording by address yet.
    pub(crate) recording_address: Option<String>,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            web_rtc_enabled: true,
            recording_address: None,
        }
    }
}

/// Best-effort inbound bitrate totals grouped by logical stream type.
///
/// These numbers are cold-path observability data assembled from transport
/// snapshots plus room-owned producer metadata. They are not used for routing
/// decisions in the hot path.
///
/// The split into audio, camera and screen is room-owned interpretation of the
/// current producer set, not a transport-native classification.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct IncomingBitrateSnapshot {
    /// Sum reported by the transport layer for every known media flow.
    ///
    /// This can be larger than the sum of the typed buckets if transport state
    /// still contains media that room state no longer classifies.
    pub(crate) total: u64,
    /// Audio-only share of `total`.
    pub(crate) audio: u64,
    /// Camera-video share of `total`.
    pub(crate) camera: u64,
    /// Screen-share share of `total`.
    pub(crate) screen: u64,
}

/// Cold-path observability snapshot for one live room.
///
/// This is the compact room-level view used by compatibility stats and manager
/// listings. It intentionally avoids exposing per-user details.
///
/// See [`Room::diagnostics_user_views`] for the richer per-user
/// inspection surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoomUserStatsSnapshot {
    /// Aggregate inbound bitrate across the room's current transport media.
    ///
    /// The buckets reflect what the room currently believes each producer is.
    pub(crate) incoming_bitrate: IncomingBitrateSnapshot,
    /// Total live user count in the room.
    pub(crate) count: u64,
    /// Sessions currently publishing camera video.
    ///
    /// This counts distinct users, not the number of camera producers.
    pub(crate) camera_count: u64,
    /// Sessions currently publishing screen video.
    ///
    /// This counts distinct users, not the number of screen producers.
    pub(crate) screen_count: u64,
}

/// Cheap publication and subscription counters used around room transitions.
///
/// These are mostly used for diagnostics and telemetry emitted around room
/// effect execution, so transitions can record before/after media shape.
///
/// They are deliberately separate from the richer diagnostics types because
/// many room transitions only need a small before/after summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RoomMediaCounts {
    /// Number of live published streams in chanel state.
    ///
    /// A staged publish that has not been committed yet does not count here.
    pub(crate) publications: usize,
    /// Number of live consumer routes in room state.
    ///
    /// This counts room-owned consumer state, not pending bootstrap work.
    pub(crate) subscriptions: usize,
}

/// Analogus to one Odoo Discuss room.
///
/// `Room` owns immutable room definition plus the guarded mutable state needed to run
/// membership, routing and recording for that room. Callers are expected to express
/// room-level intents through this facade, while process-level lookup and lifecycle
/// serialization stay in [`super::manager::RoomManager`].
///
/// The main invariant is that this facade keeps room state authoritative while
/// transport work happens after the relevant locks are released. That is why it
/// stores both a pure `RoomState` model and the async staging state needed
/// around publish and recording workflows.
///
/// # What belongs here
///
/// `Room` is responsible for:
///
/// - exposing immutable room identity and feature metadata
/// - owning the mutable room model for membership, publications, subscriptions,
///   and recording state
/// - sequencing room-level intents such as join, leave, publish, unpublish,
///   subscribe and diagnostics queries
/// - handing deferred user work back to websocket code through
///   [`UserOutbound`]
///
/// `Room` is not responsible for:
///
/// - process-global room lookup or room creation idempotence
/// - serializing websocket envelopes
/// - direct transport packet handling
/// - embedding transport or websocket logic into the pure room model
///
/// # Concurrency model
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
    /// Immutable identity, placement and feature metadata for the room lifetime.
    ///
    /// `definition` is the stable read-only half of the room, while `state`
    /// contains the mutable membership and media graph.
    pub(super) definition: RoomDefinition,
    #[allow(
        dead_code,
        reason = "recording control-plane wiring is intentionally deferred until the replacement baseline is validated"
    )]
    /// Chanel-owned recording service shared with topology observers.
    ///
    /// The service is injected into the topology side so recording can observe
    /// routed media without making router state recording-aware.
    pub(super) recording_service: Arc<RecordingService>,
    /// Process-wide metrics catalog used by room-facing orchestration.
    ///
    /// Keeping this here avoids threading metrics handles through every room
    /// transition call that may want to report lifecycle changes.
    pub(super) metrics: Arc<RuntimeMetrics>,
    /// Staged publish reservations that live across the offer/answer gap.
    ///
    /// This stays outside `RoomState` because it tracks async transport work
    /// that has not become live room state yet A publish only becomes real
    /// chanel state after the later commit path succeeds.
    pub(super) pending_publish_transactions: Mutex<PendingPublishTransactions>,
    /// Pure room state plus room-owned indexes.
    ///
    /// Callers must snapshot what they need and drop this lock before async
    /// transport or websocket work. This keeps room transitions deterministic
    /// and prevents async transport behavior from shaping the state model.
    pub(super) state: RwLock<RoomState>,
}

impl Room {
    /// Build one live room from stable runtime placement, policy and shared services.
    ///
    /// Construction wires the immutable room definition, the room-owned state
    /// model and the recording observer surface together once. After that,
    /// higher-level runtime code should interact with the room through intent
    /// methods such as join, leave, publish, subscribe and stats queries
    ///
    /// This constructor is intentionally explicit because the room boundary has
    /// three distinct input categories:
    ///
    /// - runtime placement in [`RoomRuntimeContext`]
    /// - stable room rules in [`RoomRuntimePolicy`]
    /// - room identity and compatibility config from the edge
    #[allow(
        clippy::too_many_arguments,
        reason = "room construction keeps runtime identity, policy and shared services explicit at the boundary"
    )]
    pub(crate) fn new(
        runtime_context: RoomRuntimeContext,
        runtime_policy: RoomRuntimePolicy,
        issuer: String,
        key: Option<String>,
        config: RoomConfig,
        diagnostics: Arc<DiagnosticsStore>,
        recording_media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let definition = RoomDefinition::new(runtime_context, &runtime_policy, issuer, key, config);
        let recording_media_source: Arc<dyn MediaSource> = recording_media_tap;
        let recording_service = Arc::new(RecordingService::new(
            definition.instance_id(),
            recording_media_source,
            Arc::clone(&metrics),
        ));
        Self {
            diagnostics,
            definition,
            recording_service: Arc::clone(&recording_service),
            metrics,
            pending_publish_transactions: Mutex::new(PendingPublishTransactions::default()),
            state: RwLock::new(RoomState::new(
                runtime_context.router,
                runtime_policy.admission_policy,
                runtime_policy.router_rtp_capabilities,
                recording_service,
            )),
        }
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
    pub(crate) fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.definition.transport_user_key(user_id, connection_id)
    }

    /// Current route state for one consumer or producer pair
    ///
    /// This is mainly used by orchestration and diagnostics code that needs to
    /// know whether a logical room subscription currently resolves to a live,
    /// paused, or otherwise tracked consumer route.
    pub(crate) async fn consumer_route_state(
        &self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_type: StreamType,
    ) -> Option<ConsumerRouteState> {
        self.state.read().await.consumer_route_state(
            consumer_user_id,
            producer_user_id,
            stream_type,
        )
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
    /// Optional room key configured at room creation time.
    ///
    /// This is preserved as imutable room metadata and can later be used by
    /// control-plane or permission flows that need room-scoped secrets
    ///
    /// The room itself does not reinterpret or rotate this value.
    pub fn key(&self) -> Option<&str> {
        self.definition.key()
    }

    #[must_use]
    /// Feature flags this room currently advertises to clients.
    ///
    /// This is the room-facing compatibility view derived from the runtime
    /// policy and room config, not a reflection of every internal capability.
    ///
    /// Outer layers should prefer this method over recostructing feature flags
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

    /// Snapshot of every peer except the requested user.
    ///
    /// This is the room-facing view used when a user needs to learn about
    /// the rest of the room without receiving itself back as a peer entry
    /// The shape is already projected into protocol-facing `PeerSnapshot`
    /// values, so callers do not need to rebuild that view from raw room state.
    pub(crate) async fn peer_snapshots_except(
        &self,
        excluded_user_id: &UserId,
    ) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .peer_snapshots_except(excluded_user_id)
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
        observability_port: &impl ObservabilityPort,
    ) -> RoomUserStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .into_iter()
            .map(|(user_id, connection_id)| self.transport_user_key(&user_id, connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = observability_port.transport_bitrate_snapshot(&session_keys);
        let mut aggregated_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total,
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_type) =
                state.producer_stream_type_for_transport_media_id(transport_media_id)
            else {
                continue;
            };
            match stream_type {
                StreamType::Audio => {
                    aggregated_bitrate.audio = aggregated_bitrate.audio.saturating_add(bits);
                }
                StreamType::Camera => {
                    aggregated_bitrate.camera = aggregated_bitrate.camera.saturating_add(bits);
                }
                StreamType::Screen => {
                    aggregated_bitrate.screen = aggregated_bitrate.screen.saturating_add(bits);
                }
            }
        }
        let (count, camera_count, screen_count) = state.user_stats_counts();
        drop(state);
        RoomUserStatsSnapshot {
            incoming_bitrate: aggregated_bitrate,
            count,
            camera_count,
            screen_count,
        }
    }

    pub(crate) async fn media_counts(&self) -> RoomMediaCounts {
        let state = self.state.read().await;
        RoomMediaCounts {
            publications: state.publication_count(),
            subscriptions: state.subscription_count(),
        }
    }

    #[must_use]
    /// Whether this room advertises RTC support to clients.
    ///
    /// This is a cheap immutable view over room configuration. It tells callers
    /// what the room contract is, not whether one specific user is currently
    /// connected to transport
    pub(crate) fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    #[must_use]
    /// Whether the room was created with recording enabled.
    ///
    /// This reflects room configuration, not whether recording is currently
    /// running. For the live room state, use [`Self::recording_state`].
    pub(crate) fn recording_enabled(&self) -> bool {
        self.definition.recording_enabled()
    }

    #[must_use]
    /// Media worker that owns this room's transport users.
    ///
    /// Runtime diagnostics and transport command paths use this to route work
    /// to the correct worker shard.
    pub(crate) fn media_worker_id(&self) -> usize {
        self.definition.media_worker_id()
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

    /// Merge room state and transport observabillity into diagnostics user views.
    ///
    /// This is richer than [`Self::session_stats_snapshot`] because it builds a
    /// per-user view with transport health and current incoming bitrate.
    pub(crate) async fn diagnostics_user_views(
        &self,
        observability_port: &impl ObservabilityPort,
    ) -> Vec<DiagnosticsUserView> {
        let state = self.state.read().await;
        let session_entries = state.transport_user_entries();
        let session_keys = session_entries
            .iter()
            .map(|(user_id, connection_id)| self.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = observability_port.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_session =
            state.diagnostics_incoming_bitrate_by_session(&transport_snapshot.per_media);
        let transport_by_session = session_entries
            .into_iter()
            .map(|(user_id, connection_id)| {
                let transport = DiagnosticsUserTransport {
                    connection_id: connection_id.as_u64(),
                    health: observability_port
                        .session_transport_health(&self.transport_user_key(&user_id, connection_id))
                        .map(Into::into),
                    media_worker_id: self.definition.media_worker_id(),
                    quality_summary: DiagnosticsQualitySummary {
                        current_incoming_bitrate: incoming_bitrate_by_session
                            .get(&user_id)
                            .cloned()
                            .unwrap_or_default(),
                        sampled_metrics_available: false,
                    },
                };
                (user_id, transport)
            })
            .collect();
        state.diagnostics_user_views(self.definition.media_worker_id(), &transport_by_session)
    }

    /// Builds the live source inventory for operator diagnostics.
    ///
    /// Source descriptors are room-domain objects, while bitrate samples come
    /// from the transport observability port. This method keeps the merge at
    /// the room boundary so diagnostics routes do not inspect room state
    /// or transport internals directly.
    pub(crate) async fn diagnostics_sources(
        &self,
        observability_port: &impl ObservabilityPort,
    ) -> Vec<DiagnosticsSource> {
        let active_speaker_diagnostics = active_speaker_diagnostics_by_media(
            observability_port
                .active_speaker_diagnostic_snapshot()
                .await,
        );
        let state = self.state.read().await;
        let session_keys = state
            .transport_user_entries()
            .iter()
            .map(|(user_id, connection_id)| self.transport_user_key(user_id, *connection_id))
            .collect::<Vec<_>>();
        let transport_snapshot = observability_port.transport_bitrate_snapshot(&session_keys);
        let incoming_bitrate_by_source =
            state.diagnostics_incoming_bitrate_by_source(&transport_snapshot.per_media);
        state.diagnostics_sources(&incoming_bitrate_by_source, &active_speaker_diagnostics)
    }

    /// Resolve a diagnostics request path agaisnt either nummeric or string user ids.
    ///
    /// Diagnostics routes take one raw path segment, but room users may use
    /// either integer or string ids, so this helper normalizes that lookup
    pub(crate) async fn diagnostics_matching_user(
        &self,
        requested_user_id: &str,
        observability_port: &impl ObservabilityPort,
    ) -> Option<(DiagnosticsUserView, UserId)> {
        self.diagnostics_user_views(observability_port)
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

impl fmt::Debug for Room {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Room")
            .field("instance_id", &self.definition.instance_id())
            .field("media_worker_id", &self.definition.media_worker_id())
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
