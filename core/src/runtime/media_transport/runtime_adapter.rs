//! Public media transport handles used by the core and server runtime.
//!
//! The runtime asks for media intent through small transport ports:
//! negotiation, session cleanup, media wiring, observability and source-policy
//! wakeups. This file keeps that caller-facing surface independent from the
//! concrete RTC worker topology. RTC details are owned by [`RtcTransport`] and
//! the shard set below it. The server runtime should depend on
//! [`MediaTransport`] and [`MediaTransportDeps`], not on worker shards or fake
//! test adapters.
//!
//! # Error handling
//!
//! Construction errors are returned before any worker state is created.
//! Runtime transport failures are propagated through the concern traits and
//! logged once at the opaque [`MediaTransport`] boundary so callers do not need
//! to duplicate transport-failure logging.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
use thiserror::Error;
use tracing::warn;

use super::{
    MediaTransportBackend,
    config::{MediaTransportDeps, RtcTransportConfig, RtcTransportShardSetConfig},
    shard_set::RtcTransportShardSet,
};
use crate::{
    CoreOptions,
    runtime::RoomInstanceId,
    transport::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer, ConsumerActivity,
        ConsumerPacketGateUpdate, MediaPort, NegotiationPort, ObservabilityPort, ProducerActivity,
        ReceiverBandwidthSnapshot, SessionOffer, SessionPort, SourcePacketGate, SourcePolicyPort,
        SourcePolicyUpdateSubscription, TransportAdapterError, TransportBitrateSnapshot,
        TransportMediaId, TransportPlacementPressureSnapshot, TransportSessionHealth,
        TransportSessionKey,
    },
};

/// Production media transport backed by the process-local RTC shard set.
///
/// `RtcTransport` owns the actual RTC shard collection. It is a core-owned
/// implementation detail for production media, not the type the server runtime
/// should name in orchestration code. Use [`MediaTransport::from_core_options`]
/// at the runtime boundary unless a targeted transport test needs to construct
/// a real RTC backend directly.
///
/// Cloning this handle is cheap. Clones share the same shard set and therefore
/// the same packet loops, diagnostics state, source-policy signal and relay
/// registrations.
#[derive(Debug, Clone)]
pub struct RtcTransport {
    pub(super) shards: Arc<RtcTransportShardSet>,
}

impl RtcTransport {
    /// Starts named RTC transport construction.
    ///
    /// The builder validates cold-path topology choices such as worker count
    /// and UDP port splitting before the first shard is created.
    #[must_use]
    pub const fn builder() -> RtcTransportBuilder {
        RtcTransportBuilder::new()
    }

    /// Builds a production RTC transport from a prepared builder.
    ///
    /// This associated function exists for call sites that prefer passing the
    /// builder as one value. Normal fluent construction can call
    /// [`RtcTransportBuilder::build`] directly.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when the builder is missing required
    /// inputs or describes an invalid worker topology.
    pub fn build(builder: RtcTransportBuilder) -> Result<Self, RtcTransportBuildError> {
        builder.build()
    }

    fn from_shard_set_config(
        config: &RtcTransportShardSetConfig,
    ) -> Result<Self, RtcTransportBuildError> {
        validate_worker_split(
            config.transport_config().rtc_port_range(),
            config.worker_count(),
        )?;
        Ok(Self::from_unchecked_shard_set_config(config))
    }

    fn from_unchecked_shard_set_config(config: &RtcTransportShardSetConfig) -> Self {
        Self {
            shards: Arc::new(RtcTransportShardSet::new(config)),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn shards(&self) -> &Arc<RtcTransportShardSet> {
        &self.shards
    }
}

/// Named construction input for the production RTC transport.
///
/// Building the RTC transport needs operator policy, process services and
/// worker topology. The builder keeps those inputs named so the runtime does
/// not have to assemble positional shard-set plumbing or know that one shared
/// source-policy signal will be installed into every shard.
///
/// # Validation
///
/// `worker_count` defaults to one. `build` rejects zero workers and rejects
/// worker counts that cannot receive at least one UDP port from the configured
/// range.
#[derive(Debug, Clone)]
pub struct RtcTransportBuilder {
    /// RTC-specific operator policy collected from runtime core options or a
    /// test fixture.
    transport: Option<RtcTransportConfig>,
    /// Process services needed by the transport while it emits diagnostics,
    /// metrics and packet-sink fanout.
    deps: Option<MediaTransportDeps>,
    /// Number of RTC shard workers to construct.
    worker_count: usize,
}

impl RtcTransportBuilder {
    /// Creates a builder with one media worker and no required inputs.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            transport: None,
            deps: None,
            worker_count: 1,
        }
    }

    /// Projects runtime core options into RTC transport policy.
    ///
    /// This is the preferred production construction path because it keeps the
    /// server runtime in terms of media transport policy rather than RTC engine
    /// internals.
    #[must_use]
    pub fn core_options(mut self, options: &CoreOptions) -> Self {
        self.transport = Some(RtcTransportConfig {
            public_ip: options.media.public_ip,
            bitrate_limits: options.media.bitrate_limits,
            video_bitrate_limits: options.media.video_bitrate_limits,
            rtc_port_range: options.media.rtc_port_range,
            codec_flags: options.codecs.flags,
            codec_preferences: options.codecs.preferences,
        });
        self.worker_count = options.routing.media_worker_count;
        self
    }

    /// Provides an already assembled RTC transport config.
    ///
    /// This is mainly useful for targeted tests that need a narrow port range
    /// or codec policy without constructing a full server config.
    #[must_use]
    pub fn transport_config(mut self, config: RtcTransportConfig) -> Self {
        self.transport = Some(config);
        self
    }

    /// Provides the shared process services used by every RTC shard.
    #[must_use]
    pub fn deps(mut self, deps: MediaTransportDeps) -> Self {
        self.deps = Some(deps);
        self
    }

    /// Selects how many RTC worker shards the transport should create.
    ///
    /// The value is validated by [`Self::build`]. Supplying zero or more
    /// workers than available UDP ports is a construction error.
    #[must_use]
    pub const fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    /// Creates the RTC transport and validates worker topology.
    ///
    /// The method is cold-path only. It allocates shard state, creates one
    /// shared source-policy signal for the shard set and does no packet-loop
    /// work by itself.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when transport config or dependency
    /// inputs are missing or when worker placement cannot fit the port range.
    pub fn build(self) -> Result<RtcTransport, RtcTransportBuildError> {
        let transport = self
            .transport
            .ok_or(RtcTransportBuildError::MissingTransportConfig)?;
        let deps = self.deps.ok_or(RtcTransportBuildError::MissingDeps)?;
        RtcTransport::from_shard_set_config(&RtcTransportShardSetConfig::new(
            transport,
            deps,
            self.worker_count,
        ))
    }
}

impl Default for RtcTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid construction inputs for the production RTC transport.
///
/// These errors are configuration failures. They should surface during startup
/// or test fixture creation before any media session is admitted.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum RtcTransportBuildError {
    /// The caller did not provide RTC transport policy.
    #[error("RTC transport configuration is missing")]
    MissingTransportConfig,
    /// The caller did not provide the shared diagnostics, metrics and
    /// packet-sink services needed by the transport.
    #[error("RTC transport dependencies are missing")]
    MissingDeps,
    /// A transport cannot be built without at least one worker shard.
    #[error("RTC transport worker count must be at least one")]
    InvalidWorkerCount,
    /// The configured UDP range cannot be split so every requested worker owns
    /// at least one port.
    #[error(
        "RTC transport cannot split {port_count} UDP ports across {worker_count} media workers"
    )]
    InvalidPortSplit {
        worker_count: usize,
        port_count: u16,
    },
}

fn validate_worker_split(
    rtc_port_range: crate::RtcPortRange,
    worker_count: usize,
) -> Result<(), RtcTransportBuildError> {
    if worker_count == 0 {
        return Err(RtcTransportBuildError::InvalidWorkerCount);
    }
    if worker_count > usize::from(rtc_port_range.port_count()) {
        return Err(RtcTransportBuildError::InvalidPortSplit {
            worker_count,
            port_count: rtc_port_range.port_count(),
        });
    }
    Ok(())
}

/// Opaque runtime media transport handle.
///
/// `MediaTransport` is the type server orchestration and `SfuCore` should hold.
/// It hides whether the active backend is production RTC or a deterministic
/// test transport selected by cfg. Callers express intent through the transport
/// port traits and must not branch on concrete backend variants.
///
/// The handle also centralizes warning logs for failed transport effects. Inner
/// backends return typed errors, while this boundary adds stable diagnostic
/// context such as session keys, media ids and SDP lengths.
#[derive(Debug, Clone)]
pub struct MediaTransport {
    pub(super) backend: MediaTransportBackend,
}

impl MediaTransport {
    /// Builds the runtime media transport from neutral core options and process
    /// dependencies.
    ///
    /// This is the production server construction path. It intentionally
    /// returns the opaque media transport handle so the orchestration layer does
    /// not import RTC-specific construction types.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when the derived RTC transport cannot
    /// be built from the supplied options and dependencies.
    pub fn from_core_options(
        options: &CoreOptions,
        deps: MediaTransportDeps,
    ) -> Result<Self, RtcTransportBuildError> {
        RtcTransport::builder()
            .core_options(options)
            .deps(deps)
            .build()
            .map(Self::from_rtc_transport)
    }

    /// Wraps a production RTC implementation in the opaque media transport
    /// handle.
    ///
    /// This is useful for tests that need real RTC behavior while still passing
    /// the same type that production orchestration uses.
    #[must_use]
    pub const fn from_rtc_transport(transport: RtcTransport) -> Self {
        Self {
            backend: MediaTransportBackend::from_rtc(transport),
        }
    }
}

impl NegotiationPort for MediaTransport {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = self.backend.create_initial_session_offer(session_key).await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "media transport failed to create initial user offer"
            );
        }
        result
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let result = self
            .backend
            .create_session_renegotiation_offer(session_key)
            .await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?error,
                "media transport failed to create renegotiation offer"
            );
        }
        result
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let result = self
            .backend
            .apply_session_answer(session_key, answer_sdp)
            .await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to apply user answer"
            );
        }
        result
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        let result = self
            .backend
            .negotiated_client_rtp_capabilities(answer_sdp, offered_router_capabilities);
        if let Err(error) = &result {
            warn!(
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to derive client RTP capabilities from answer SDP"
            );
        }
        result
    }
}

impl SessionPort for MediaTransport {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let result = self.backend.close_session(session_key).await;
        if let Err(error) = &result {
            warn!(?session_key, ?error, "media transport failed to close user");
        }
        result
    }
}

impl MediaPort for MediaTransport {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = self
            .backend
            .remove_media(session_key, transport_media_id)
            .await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                ?error,
                "media transport failed to remove media"
            );
        }
        result
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = self
            .backend
            .publish_media(session_key, media_kind, rtp_parameters)
            .await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?media_kind,
                mid = rtp_parameters.mid(),
                ?error,
                "media transport failed to declare producer media"
            );
        }
        result
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = self
            .backend
            .consume_media(
                consumer_session_key,
                media_kind,
                source_session_key,
                source_media_id,
                consumer_rtp_parameters,
            )
            .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                ?error,
                "media transport failed to declare consumer media"
            );
        }
        result
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        activity: ProducerActivity,
    ) -> Result<(), TransportAdapterError> {
        let result = self
            .backend
            .set_producer_active(session_key, transport_media_id, activity)
            .await;
        if let Err(error) = &result {
            warn!(
                ?session_key,
                ?transport_media_id,
                active = activity.is_active(),
                ?error,
                "media transport failed to update producer activity"
            );
        }
        result
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        activity: ConsumerActivity,
    ) -> Result<(), TransportAdapterError> {
        let result = self
            .backend
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                activity,
            )
            .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                active = activity.is_active(),
                ?error,
                "media transport failed to update consumer activity"
            );
        }
        result
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        let result = self
            .backend
            .set_consumer_packet_gate(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                packet_gate.clone(),
            )
            .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                ?packet_gate,
                ?error,
                "media transport failed to update consumer packet gate"
            );
        }
        result
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        let results = self.backend.set_consumer_packet_gates(updates).await;
        for (update, result) in updates.iter().zip(results.iter()) {
            if let Err(error) = result {
                warn!(
                    ?error,
                    consumer_session_key = ?update.consumer_session_key(),
                    consumer_transport_media_id = ?update.consumer_transport_media_id(),
                    source_session_key = ?update.source_session_key(),
                    source_transport_media_id = ?update.source_transport_media_id(),
                    packet_gate = ?update.packet_gate(),
                    "media transport failed to update a batched consumer packet gate"
                );
            }
        }
        results
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let result = self
            .backend
            .request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?consumer_transport_media_id,
                ?source_session_key,
                ?source_transport_media_id,
                ?error,
                "media transport failed to request a consumer keyframe refresh"
            );
        }
        result
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.backend
            .transport_media_mid(session_key, transport_media_id)
            .await
    }
}

impl ObservabilityPort for MediaTransport {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        self.backend.transport_bitrate_snapshot(session_keys)
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        self.backend.receiver_bandwidth_snapshot(session_keys)
    }

    fn placement_pressure_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportPlacementPressureSnapshot {
        self.backend.placement_pressure_snapshot(session_keys)
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.backend.active_speaker_source_snapshot().await
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        self.backend.active_speaker_diagnostic_snapshot().await
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        self.backend.next_active_speaker_deadline().await
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.backend
            .expired_active_speaker_room_instance_ids(now)
            .await
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.backend.session_transport_health(session_key)
    }
}

impl SourcePolicyPort for MediaTransport {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.backend.source_policy_subscription()
    }
}
