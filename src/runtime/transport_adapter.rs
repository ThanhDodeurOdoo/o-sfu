use std::{cmp::Reverse, collections::BTreeMap, fmt::Debug, net::IpAddr, sync::Arc, time::Instant};

#[cfg(test)]
use super::rtc_adapter::DebugRouteEntry;
use super::{
    rtc_adapter::{
        RelayCleanup, RtcTransportAdapter, TransportSessionHealth,
        client_rtp_capabilities_from_answer,
    },
    stub_bus::StubWebRtcAdapter,
};
use crate::config::MediaCodecFlags;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::MediaTap;
#[cfg(test)]
use crate::runtime::transport_bootstrap::SessionTransportBootstrap;
#[cfg(test)]
use crate::runtime::transport_connect::{
    TransportConnectDtlsParameters, TransportConnectIceParameters,
};

use crate::config::RtcPortRange;
use crate::signaling::{shared::SessionId, webrtc::MediaKind as SignalingMediaKind};
use o_sfu_router::{MediaCapabilities, RtpParameters as RouterRtpParameters};
use str0m::media::MediaKind as Str0mMediaKind;
#[cfg(test)]
use str0m::media::Mid;

/// Channel-scooped transport-adapter session identity.
///
/// A `SessionId` alone is not unique across the server (sfu can have multiple odoo servers connected to it),
/// the same id can appear in different channels simmultaneously. This composite key allows to uniquely
/// dentify one session with:
///
///   +-- `channel_runtime`    - which channel
///   +-- `media_worker`       - which worker thread / shard
///   +-- `connection`         - which signaling connection
///   +-- `session`            - the session within that connection
///
/// The key is `Ord` so it can be used in `BTreeMap` lookups keyed by shard index.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransportSessionKey {
    /// Monotonic id of the channel runtime that owns this session.
    channel_runtime: u64,
    /// index of the media worker this session is pinned to (determines shard).
    media_worker: usize,
    /// Signaling-layer conection identifier (signaling connection instanance),
    /// this prevents stale connections from being processed (it change if the connection is re-established)
    connection: u64,
    /// Arc-wrapped to allow cheap cloning when the key is stored in multiple maps
    session: Arc<SessionId>,
}

impl TransportSessionKey {
    #[must_use]
    pub(crate) fn new(
        channel_runtime_id: u64,
        media_worker_id: usize,
        connection_id: u64,
        session_id: SessionId,
    ) -> Self {
        Self {
            channel_runtime: channel_runtime_id,
            media_worker: media_worker_id,
            connection: connection_id,
            session: Arc::new(session_id),
        }
    }

    #[must_use]
    pub(crate) fn channel_runtime_id(&self) -> u64 {
        self.channel_runtime
    }

    #[must_use]
    pub(crate) fn media_worker_id(&self) -> usize {
        self.media_worker
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> &SessionId {
        self.session.as_ref()
    }
}

/// Direction of a WebRTC transport from the client's perspective.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TransportConnectDirection {
    /// Client sends media to the SFU (producer / upload transport).
    Upload,
    /// Client receives media from the SFU (consumer / download transport).
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportAdapterError {
    TransportUnavailable,
    InvalidInput,
    UnsupportedFeature,
}

/// Named request for connecting one transport direction with client auth data.
///
/// This keeps the transport boundary readable when optional ICE credentials or
/// transitional SDP validation are present.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransportConnectRequest<'a> {
    direction: TransportConnectDirection,
    dtls_parameters: &'a TransportConnectDtlsParameters,
    ice_parameters: Option<&'a TransportConnectIceParameters>,
    sdp_offer: Option<&'a str>,
}

#[cfg(test)]
impl<'a> TransportConnectRequest<'a> {
    #[must_use]
    pub(crate) fn new(
        direction: TransportConnectDirection,
        dtls_parameters: &'a TransportConnectDtlsParameters,
    ) -> Self {
        Self {
            direction,
            dtls_parameters,
            ice_parameters: None,
            sdp_offer: None,
        }
    }

    #[must_use]
    pub(crate) fn with_ice_parameters(
        mut self,
        ice_parameters: &'a TransportConnectIceParameters,
    ) -> Self {
        self.ice_parameters = Some(ice_parameters);
        self
    }

    #[must_use]
    pub(crate) fn with_sdp_offer(mut self, sdp_offer: &'a str) -> Self {
        self.sdp_offer = Some(sdp_offer);
        self
    }

    #[must_use]
    pub(crate) const fn direction(self) -> TransportConnectDirection {
        self.direction
    }

    #[must_use]
    pub(crate) const fn dtls_parameters(self) -> &'a TransportConnectDtlsParameters {
        self.dtls_parameters
    }

    #[must_use]
    pub(crate) const fn ice_parameters(self) -> Option<&'a TransportConnectIceParameters> {
        self.ice_parameters
    }

    #[must_use]
    pub(crate) const fn sdp_offer(self) -> Option<&'a str> {
        self.sdp_offer
    }
}

/// Point-in-time bitrate measurement aggregated across one or more transport sessions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransportBitrateSnapshot {
    /// Sum of all per-media bitrates (bits/s).
    pub(crate) total: u64,
    /// Individual bitrate for each active media line.
    pub(crate) per_media: Vec<(TransportMediaId, u64)>,
}

/// Opaque identifier for a media line allocated by the transport adapter.
///
/// Wraps the transport-internal representation (e.g. str0m `Mid`) without
/// exposing WebRTC library types to the signaling/channel layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub(crate) struct TransportMediaId(u64);

impl TransportMediaId {
    pub(super) fn new(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Transport-observed active speaker keyed by the producing media source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveSpeakerSource {
    transport_media_id: TransportMediaId,
    observed_at: Instant,
}

impl ActiveSpeakerSource {
    #[must_use]
    pub(crate) const fn new(transport_media_id: TransportMediaId, observed_at: Instant) -> Self {
        Self {
            transport_media_id,
            observed_at,
        }
    }

    #[must_use]
    pub(crate) const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub(crate) const fn observed_at(self) -> Instant {
        self.observed_at
    }
}

/// Channel-owned source-layer choice applied at the transport boundary.
///
/// The room runtime expresses intent in terms of transport-owned media ids and
/// stable layer labels. The RTC adapter translates that intent into its
/// packet-gating primitives without leaking `str0m`-specific control types
/// back into channel orchestration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourcePacketSelection {
    Rid(String),
}

/// Transitional server-authored SDP offer returned by the transport boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionOffer {
    sdp: String,
}

impl SessionOffer {
    #[must_use]
    pub(crate) fn new(sdp: String) -> Self {
        Self { sdp }
    }

    #[must_use]
    pub(crate) fn into_sdp(self) -> String {
        self.sdp
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RtcTransportAdapterConfig {
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
    media_tap: Arc<MediaTap>,
    metrics: Arc<RuntimeMetrics>,
}

impl RtcTransportAdapterConfig {
    #[must_use]
    pub(crate) fn new(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        codec_flags: MediaCodecFlags,
        media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            public_ip,
            rtc_port_range,
            codec_flags,
            media_tap,
            metrics,
        }
    }

    #[must_use]
    fn with_rtc_port_range(&self, rtc_port_range: RtcPortRange) -> Self {
        Self {
            public_ip: self.public_ip,
            rtc_port_range,
            codec_flags: self.codec_flags,
            media_tap: Arc::clone(&self.media_tap),
            metrics: Arc::clone(&self.metrics),
        }
    }

    #[must_use]
    pub(crate) const fn public_ip(&self) -> IpAddr {
        self.public_ip
    }

    #[must_use]
    pub(crate) const fn rtc_port_range(&self) -> RtcPortRange {
        self.rtc_port_range
    }

    #[must_use]
    pub(crate) const fn codec_flags(&self) -> MediaCodecFlags {
        self.codec_flags
    }

    #[must_use]
    pub(crate) fn media_tap(&self) -> Arc<MediaTap> {
        Arc::clone(&self.media_tap)
    }

    #[must_use]
    pub(crate) fn metrics(&self) -> Arc<RuntimeMetrics> {
        Arc::clone(&self.metrics)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RtcTransportAdapterShardSetConfig {
    worker_count: usize,
    adapter: RtcTransportAdapterConfig,
}

impl RtcTransportAdapterShardSetConfig {
    #[must_use]
    pub(crate) fn new(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        worker_count: usize,
        codec_flags: MediaCodecFlags,
        media_tap: Arc<MediaTap>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            worker_count,
            adapter: RtcTransportAdapterConfig::new(
                public_ip,
                rtc_port_range,
                codec_flags,
                media_tap,
                metrics,
            ),
        }
    }

    #[must_use]
    fn worker_count(&self) -> usize {
        self.worker_count
    }

    #[must_use]
    fn adapter_config(&self) -> &RtcTransportAdapterConfig {
        &self.adapter
    }

    #[must_use]
    fn shard_config_with_port_range(
        &self,
        rtc_port_range: RtcPortRange,
    ) -> RtcTransportAdapterConfig {
        self.adapter.with_rtc_port_range(rtc_port_range)
    }
}

#[derive(Debug, Default)]
pub(crate) struct RuntimeTransportAdapterBuilder {
    rtc_config: Option<RtcTransportAdapterShardSetConfig>,
}

impl RuntimeTransportAdapterBuilder {
    #[must_use]
    pub(crate) fn stub(mut self) -> Self {
        self.rtc_config = None;
        self
    }

    #[must_use]
    pub(crate) fn rtc(mut self, config: RtcTransportAdapterShardSetConfig) -> Self {
        self.rtc_config = Some(config);
        self
    }

    #[must_use]
    pub(crate) fn build(self) -> RuntimeTransportAdapter {
        self.rtc_config.map_or_else(
            || RuntimeTransportAdapter::Stub(Arc::new(StubWebRtcAdapter::default())),
            |config| {
                RuntimeTransportAdapter::Rtc(Arc::new(RtcTransportAdapterShardSet::new(&config)))
            },
        )
    }
}

/// Runtime boundary between signaling/session orchestration and transport-specific behavior.
///
/// Implementations provide transport bootstrap payloads and transport connection handling
/// without leaking concrete WebRTC library details into the signaling flow.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeTransportAdapter {
    Stub(Arc<StubWebRtcAdapter>),
    Rtc(Arc<RtcTransportAdapterShardSet>),
}

#[derive(Debug)]
pub(crate) struct RtcTransportAdapterShardSet {
    primary_shard: Arc<RtcTransportAdapter>,
    extra_shards: Vec<Arc<RtcTransportAdapter>>,
}

impl RtcTransportAdapterShardSet {
    fn new(config: &RtcTransportAdapterShardSetConfig) -> Self {
        let Some(shard_ranges) = config
            .adapter_config()
            .rtc_port_range()
            .split_for_workers(config.worker_count())
        else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(config.adapter_config())),
                extra_shards: Vec::new(),
            };
        };
        let mut shard_ranges = shard_ranges.into_iter();
        let Some(primary_range) = shard_ranges.next() else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(config.adapter_config())),
                extra_shards: Vec::new(),
            };
        };
        Self {
            primary_shard: Arc::new(RtcTransportAdapter::new(
                &config.shard_config_with_port_range(primary_range),
            )),
            extra_shards: shard_ranges
                .map(|range| {
                    Arc::new(RtcTransportAdapter::new(
                        &config.shard_config_with_port_range(range),
                    ))
                })
                .collect(),
        }
    }

    fn shard_index_for_media_worker_id(&self, media_worker_id: usize) -> usize {
        let shard_count = self.extra_shards.len().saturating_add(1);
        media_worker_id % shard_count
    }

    fn shard_for_session(&self, session_key: &TransportSessionKey) -> Arc<RtcTransportAdapter> {
        self.shard_for_media_worker_id(session_key.media_worker_id())
    }

    fn shard_for_media_worker_id(&self, media_worker_id: usize) -> Arc<RtcTransportAdapter> {
        self.shard_for_index(self.shard_index_for_media_worker_id(media_worker_id))
    }

    fn relay_registration_shards(
        &self,
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Option<(Arc<RtcTransportAdapter>, Arc<RtcTransportAdapter>)> {
        let consumer_shard = self.shard_for_session(consumer_session_key);
        let source_shard = self.shard_for_session(source_session_key);
        if Arc::ptr_eq(&consumer_shard, &source_shard) {
            return None;
        }
        Some((source_shard, consumer_shard))
    }

    fn release_relay_cleanup(
        &self,
        target_shard: &Arc<RtcTransportAdapter>,
        relay_cleanup: &[RelayCleanup],
    ) {
        for cleanup in relay_cleanup {
            let source_shard = self.shard_for_session(cleanup.source_session_key());
            if Arc::ptr_eq(&source_shard, target_shard) {
                continue;
            }
            source_shard
                .deactivate_relay_route(cleanup.source_transport_media_id(), target_shard.as_ref());
        }
    }

    #[cfg(test)]
    async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        self.shard_for_session(source_session_key)
            .debug_route_entry(source_session_key, source_mid)
            .await
    }

    fn shard_for_index(&self, shard_index: usize) -> Arc<RtcTransportAdapter> {
        if shard_index == 0 {
            return Arc::clone(&self.primary_shard);
        }
        self.extra_shards
            .get(shard_index.saturating_sub(1))
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.primary_shard))
    }

    fn shard_index_for_session(&self, session_key: &TransportSessionKey) -> usize {
        self.shard_index_for_media_worker_id(session_key.media_worker_id())
    }

    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let mut keys_by_shard = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            keys_by_shard
                .entry(self.shard_index_for_session(session_key))
                .or_default()
                .push(session_key.clone());
        }
        let mut snapshot = TransportBitrateSnapshot::default();
        for (shard_index, shard_session_keys) in keys_by_shard {
            let shard = self.shard_for_index(shard_index);
            let shard_snapshot = shard.transport_bitrate_snapshot(&shard_session_keys);
            snapshot.total = snapshot.total.saturating_add(shard_snapshot.total);
            snapshot.per_media.extend(shard_snapshot.per_media);
        }
        snapshot
    }

    async fn active_speaker_source_snapshot(
        &self,
        channel_runtime_id: u64,
    ) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = self
            .primary_shard
            .active_speaker_source_snapshot(channel_runtime_id)
            .await;
        for shard in &self.extra_shards {
            snapshot.extend(
                shard
                    .active_speaker_source_snapshot(channel_runtime_id)
                    .await,
            );
        }
        snapshot.sort_by_key(|source| Reverse(source.observed_at()));
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot
    }
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn builder() -> RuntimeTransportAdapterBuilder {
        RuntimeTransportAdapterBuilder::default()
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_stub_adapter(adapter: Arc<StubWebRtcAdapter>) -> Self {
        Self::Stub(adapter)
    }

    /// Create the first server-authored SDP offer for the native signaling path.
    pub(crate) async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.create_initial_session_offer(session_key).await,
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .create_initial_session_offer(session_key)
                    .await
            }
        }
    }

    /// Create a follow-up renegotiation offer for the native signaling path.
    pub(crate) async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .create_session_renegotiation_offer(session_key)
                    .await
            }
        }
    }

    /// Apply the remote answer to the outstanding native session offer.
    pub(crate) async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.apply_session_answer(session_key, answer_sdp).await,
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .apply_session_answer(session_key, answer_sdp)
                    .await
            }
        }
    }

    pub(crate) fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        offered_router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        match self {
            Self::Stub(_adapter) => StubWebRtcAdapter::compatibility_client_rtp_capabilities(
                answer_sdp,
                offered_router_capabilities,
            ),
            Self::Rtc(_adapter) => client_rtp_capabilities_from_answer(answer_sdp)
                .ok_or(TransportAdapterError::InvalidInput),
        }
    }

    /// Build session transport bootstrap state for transport tests and benchmarks.
    #[cfg(test)]
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<SessionTransportBootstrap, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .transport_bootstrap_payload(session_key, router_capabilities)
                    .await
            }
        }
    }

    /// Release transport-adapter state for a disconnected session.
    pub(crate) async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.close_session(session_key).await,
            Self::Rtc(adapter) => {
                let session_shard = adapter.shard_for_session(session_key);
                let close_outcome = session_shard
                    .close_session_with_outcome(session_key)
                    .await?;
                adapter.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup());
                Ok(())
            }
        }
    }

    /// Remove a previously declared media line owned by `session_id`.
    pub(crate) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => adapter.remove_media(session_key, transport_media_id).await,
            Self::Rtc(adapter) => {
                let session_shard = adapter.shard_for_session(session_key);
                let remove_outcome = session_shard
                    .remove_media_with_outcome(session_key, transport_media_id)
                    .await?;
                if let Some(cleanup) = remove_outcome.relay_cleanup() {
                    let relay_cleanup = [cleanup.clone()];
                    adapter.release_relay_cleanup(&session_shard, &relay_cleanup);
                }
                Ok(())
            }
        }
    }

    #[allow(
        dead_code,
        reason = "native publish commit wiring is staged separately from answered-SDP publication extraction"
    )]
    pub(crate) async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .negotiated_producer_parameters(session_key, transport_media_id)
                    .await
            }
        }
    }

    /// Declare a media line for receiving RTP from a producer session.
    pub(crate) async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: SignalingMediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .publish_media(session_key, media_kind, rtp_parameters)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .add_recv_media(
                        session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        rtp_parameters,
                    )
                    .await
            }
        }
    }

    /// Declare a media line for sending RTP to a consumer session, routed from a producer.
    pub(crate) async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: SignalingMediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .consume_media(
                        consumer_session_key,
                        media_kind,
                        source_session_key,
                        consumer_rtp_parameters,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                if consumer_session_key.channel_runtime_id()
                    != source_session_key.channel_runtime_id()
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
                let relay_route =
                    adapter.relay_registration_shards(consumer_session_key, source_session_key);
                let remote_source_control = relay_route
                    .as_ref()
                    .map(|(source_shard, consumer_shard)| {
                        source_shard.remote_source_control(consumer_shard)
                    })
                    .transpose()?;
                if let Some((source_shard, consumer_shard)) = &relay_route {
                    source_shard.activate_relay_route(
                        source_session_key.channel_runtime_id(),
                        source_media_id,
                        consumer_shard,
                    )?;
                }
                let consumer_shard = adapter.shard_for_session(consumer_session_key);
                let add_result = consumer_shard
                    .add_send_media(
                        consumer_session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        source_session_key,
                        source_media_id,
                        remote_source_control,
                        consumer_rtp_parameters,
                    )
                    .await;
                if let Some((source_shard, consumer_shard)) = relay_route {
                    if add_result.is_ok() {
                        source_shard.set_relay_route_active(source_media_id, &consumer_shard, true);
                    } else {
                        source_shard.deactivate_relay_route(source_media_id, &consumer_shard);
                    }
                }
                add_result
            }
        }
    }

    pub(crate) fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        match self {
            Self::Stub(_adapter) => TransportBitrateSnapshot::default(),
            Self::Rtc(adapter) => adapter.transport_bitrate_snapshot(session_keys),
        }
    }

    pub(crate) async fn active_speaker_source_snapshot(
        &self,
        channel_runtime_id: u64,
    ) -> Vec<ActiveSpeakerSource> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .active_speaker_source_snapshot(channel_runtime_id)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .active_speaker_source_snapshot(channel_runtime_id)
                    .await
            }
        }
    }

    pub(crate) fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        match self {
            Self::Stub(_adapter) => None,
            Self::Rtc(adapter) => adapter
                .shard_for_session(session_key)
                .session_transport_health(session_key),
        }
    }

    /// Update whether a producer media line is allowed to forward packets.
    pub(crate) async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .set_producer_active(session_key, transport_media_id, active)
                    .await
            }
        }
    }

    /// Update whether one consumer route is allowed to forward packets.
    pub(crate) async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        active,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                if consumer_session_key.channel_runtime_id()
                    != source_session_key.channel_runtime_id()
                {
                    return Err(TransportAdapterError::InvalidInput);
                }
                adapter
                    .shard_for_session(consumer_session_key)
                    .set_consumer_active(
                        consumer_session_key,
                        consumer_transport_media_id,
                        source_session_key,
                        source_transport_media_id,
                        active,
                    )
                    .await
            }
        }
    }

    /// Apply a room-owned source-layer choice to one published media source.
    pub(crate) async fn set_source_packet_selection(
        &self,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        selection: Option<SourcePacketSelection>,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .set_source_packet_selection(
                        source_session_key,
                        source_transport_media_id,
                        selection,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(source_session_key)
                    .set_source_packet_selection(
                        source_session_key,
                        source_transport_media_id,
                        selection,
                    )
                    .await
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_set_session_transport_health(
        &self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) {
        if let Self::Rtc(adapter) = self {
            adapter
                .shard_for_session(session_key)
                .debug_set_session_transport_health(session_key, health);
        }
    }

    #[cfg(test)]
    pub(crate) async fn debug_route_entry(
        &self,
        source_session_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self::Stub(_) => None,
            Self::Rtc(adapter) => {
                adapter
                    .debug_route_entry(source_session_key, source_mid)
                    .await
            }
        }
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "test-only route inspection stays available for targeted RTC adapter assertions even when one edit removes its current call sites"
    )]
    pub(crate) async fn debug_route_entry_by_media_id(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<DebugRouteEntry> {
        match self {
            Self::Stub(_) => None,
            Self::Rtc(adapter) => {
                if let Some(entry) = adapter
                    .primary_shard
                    .debug_route_entry_by_media_id(source_transport_media_id)
                    .await
                {
                    return Some(entry);
                }
                for shard in &adapter.extra_shards {
                    if let Some(entry) = shard
                        .debug_route_entry_by_media_id(source_transport_media_id)
                        .await
                    {
                        return Some(entry);
                    }
                }
                None
            }
        }
    }
}

fn signaling_to_str0m_media_kind(kind: SignalingMediaKind) -> Str0mMediaKind {
    match kind {
        SignalingMediaKind::Audio => Str0mMediaKind::Audio,
        SignalingMediaKind::Video => Str0mMediaKind::Video,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use o_sfu_router::{
        MediaCodecCapability, MediaKind as RouterMediaKind, RtpEncoding, RtpParameters,
    };
    use str0m::media::Mid;

    use super::RuntimeTransportAdapter;
    use crate::{
        config::{MediaCodecFlags, RtcPortRange},
        runtime::{
            metrics::RuntimeMetrics,
            recording::MediaTap,
            rtc_adapter::RtcTransportAdapter,
            stub_bus::StubWebRtcAdapter,
            transport_adapter::{
                RtcTransportAdapterShardSetConfig, TransportAdapterError, TransportMediaId,
                TransportSessionKey,
            },
        },
        signaling::{shared::SessionId, webrtc::MediaKind as SignalingMediaKind},
    };
    use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;

    fn empty_router_capabilities() -> RouterRtpCapabilities {
        RouterRtpCapabilities::new(vec![], vec![])
    }

    fn sample_router_capabilities() -> RouterRtpCapabilities {
        RouterRtpCapabilities::new(
            vec![
                MediaCodecCapability::new(RouterMediaKind::Audio, "opus", 48_000)
                    .with_channels(2)
                    .with_preferred_payload_type(111),
            ],
            vec![],
        )
    }

    fn sample_audio_rtp_parameters(mid: &str, ssrc: u32) -> RtpParameters {
        RtpParameters::new(vec![], vec![], vec![RtpEncoding::new().with_ssrc(ssrc)])
            .with_mid(String::from(mid))
    }

    async fn bootstrap_rtc_session(
        adapter: &RuntimeTransportAdapter,
        session_key: &TransportSessionKey,
    ) {
        assert!(
            adapter
                .transport_bootstrap_payload(session_key, &empty_router_capabilities())
                .await
                .is_ok()
        );
    }

    async fn bootstrap_rtc_sessions(
        adapter: &RuntimeTransportAdapter,
        session_keys: &[&TransportSessionKey],
    ) {
        for session_key in session_keys {
            bootstrap_rtc_session(adapter, session_key).await;
        }
    }

    async fn publish_audio(
        adapter: &RuntimeTransportAdapter,
        session_key: &TransportSessionKey,
        rtp_parameters: &RtpParameters,
    ) -> Option<TransportMediaId> {
        let result = adapter
            .publish_media(session_key, SignalingMediaKind::Audio, rtp_parameters)
            .await;
        assert!(result.is_ok());
        result.ok()
    }

    async fn consume_audio(
        adapter: &RuntimeTransportAdapter,
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        rtp_parameters: &RtpParameters,
    ) -> Option<TransportMediaId> {
        let result = adapter
            .consume_media(
                consumer_session_key,
                SignalingMediaKind::Audio,
                source_session_key,
                source_media_id,
                rtp_parameters,
            )
            .await;
        assert!(result.is_ok());
        result.ok()
    }

    fn assert_relay_target_counts(
        source_shard: &RtcTransportAdapter,
        source_media_id: TransportMediaId,
        total: usize,
        active: usize,
    ) {
        assert_eq!(
            source_shard.debug_relay_target_count_for_source(source_media_id),
            total
        );
        assert_eq!(
            source_shard.debug_active_relay_target_count_for_source(source_media_id),
            active
        );
    }

    async fn assert_remote_source_owner(
        consumer_shard: &RtcTransportAdapter,
        source_media_id: TransportMediaId,
        expected: Option<&TransportSessionKey>,
    ) {
        assert_eq!(
            consumer_shard
                .debug_remote_source_owner(source_media_id)
                .await,
            expected.cloned()
        );
    }

    async fn assert_local_route_active(
        source_shard: &RtcTransportAdapter,
        source_session: &TransportSessionKey,
        local_consumer_session: &TransportSessionKey,
        local_consumer_media_id: TransportMediaId,
    ) {
        let local_route_entry = source_shard
            .debug_route_entry(source_session, Mid::from("aud-up"))
            .await;
        assert!(local_route_entry.is_some());
        let Some(local_route_entry) = local_route_entry else {
            return;
        };
        assert!(local_route_entry.destinations.iter().any(|destination| {
            destination.dest_session == *local_consumer_session
                && destination.dest_transport_media_id == local_consumer_media_id
                && destination.active
        }));
    }

    async fn assert_remote_route_activity(
        consumer_shard: &RtcTransportAdapter,
        source_media_id: TransportMediaId,
        remote_consumer_session: &TransportSessionKey,
        remote_consumer_media_id: TransportMediaId,
        active: bool,
    ) {
        let remote_route_entry = consumer_shard
            .debug_route_entry_by_media_id(source_media_id)
            .await;
        assert!(remote_route_entry.is_some());
        let Some(remote_route_entry) = remote_route_entry else {
            return;
        };
        assert!(remote_route_entry.destinations.iter().any(|destination| {
            destination.dest_session == *remote_consumer_session
                && destination.dest_transport_media_id == remote_consumer_media_id
                && destination.active == active
        }));
    }

    fn test_rtc_adapter(
        worker_count: usize,
        rtc_port_range: RtcPortRange,
    ) -> RuntimeTransportAdapter {
        RuntimeTransportAdapter::builder()
            .rtc(RtcTransportAdapterShardSetConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                rtc_port_range,
                worker_count,
                MediaCodecFlags::default(),
                Arc::new(MediaTap::default()),
                Arc::new(RuntimeMetrics::default()),
            ))
            .build()
    }

    #[test]
    fn stub_adapter_uses_explicit_compatibility_capability_projection() {
        let adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::new(StubWebRtcAdapter::default()));
        let offered = sample_router_capabilities();

        let projected =
            adapter.negotiated_client_rtp_capabilities("v=0\r\ns=stub-answer\r\n", &offered);

        assert_eq!(projected, Ok(offered));
    }

    #[test]
    fn stub_adapter_rejects_answers_without_minimal_sdp_shape() {
        let adapter =
            RuntimeTransportAdapter::from_stub_adapter(Arc::new(StubWebRtcAdapter::default()));

        let projected = adapter
            .negotiated_client_rtp_capabilities("invalid-answer", &sample_router_capabilities());

        assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
    }

    #[test]
    fn rtc_adapter_rejects_answers_without_projectable_client_capabilities() {
        let adapter = RuntimeTransportAdapter::builder()
            .rtc(RtcTransportAdapterShardSetConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                RtcPortRange::new(46_100, 46_199),
                1,
                MediaCodecFlags::default(),
                Arc::new(MediaTap::default()),
                Arc::new(RuntimeMetrics::default()),
            ))
            .build();

        let projected = adapter.negotiated_client_rtp_capabilities(
            "v=0\r\ns=invalid-answer\r\n",
            &sample_router_capabilities(),
        );

        assert_eq!(projected, Err(TransportAdapterError::InvalidInput));
    }

    #[tokio::test]
    async fn rtc_adapter_shards_channel_bootstrap_by_explicit_media_worker() {
        let adapter = RuntimeTransportAdapter::builder()
            .rtc(RtcTransportAdapterShardSetConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                RtcPortRange::new(46_000, 46_003),
                2,
                MediaCodecFlags::default(),
                Arc::new(MediaTap::default()),
                Arc::new(RuntimeMetrics::default()),
            ))
            .build();
        let first_channel_session = TransportSessionKey::new(10, 0, 1, SessionId::Integer(1));
        let second_channel_session = TransportSessionKey::new(11, 1, 1, SessionId::Integer(2));
        let same_shard_session = TransportSessionKey::new(12, 0, 1, SessionId::Integer(3));

        let first_payload = adapter
            .transport_bootstrap_payload(&first_channel_session, &empty_router_capabilities())
            .await;
        let second_payload = adapter
            .transport_bootstrap_payload(&second_channel_session, &empty_router_capabilities())
            .await;
        let same_shard_payload = adapter
            .transport_bootstrap_payload(&same_shard_session, &empty_router_capabilities())
            .await;
        assert!(first_payload.is_ok());
        assert!(second_payload.is_ok());
        assert!(same_shard_payload.is_ok());
        let Some(first_payload) = first_payload.ok() else {
            return;
        };
        let Some(second_payload) = second_payload.ok() else {
            return;
        };
        let Some(same_shard_payload) = same_shard_payload.ok() else {
            return;
        };

        let Some(first_candidate) = first_payload.download_transport.ice_candidates.first() else {
            return;
        };
        let Some(second_candidate) = second_payload.download_transport.ice_candidates.first()
        else {
            return;
        };
        let Some(same_shard_candidate) =
            same_shard_payload.download_transport.ice_candidates.first()
        else {
            return;
        };
        let first_port = first_candidate.port;
        let second_port = second_candidate.port;
        let same_shard_port = same_shard_candidate.port;

        assert!((46_000..=46_001).contains(&first_port));
        assert!((46_002..=46_003).contains(&second_port));
        assert_eq!(same_shard_port, first_port);
    }

    #[tokio::test]
    async fn rtc_adapter_registers_and_prunes_cross_worker_remote_sources() {
        let adapter = test_rtc_adapter(2, RtcPortRange::new(46_200, 46_299));
        let source_session = TransportSessionKey::new(20, 0, 1, SessionId::Integer(1));
        let consumer_session = TransportSessionKey::new(20, 1, 2, SessionId::Integer(2));
        let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 41_000);
        let consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down", 42_000);

        bootstrap_rtc_session(&adapter, &source_session).await;
        bootstrap_rtc_session(&adapter, &consumer_session).await;

        let Some(source_media_id) =
            publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
        else {
            return;
        };
        let Some(consumer_media_id) = consume_audio(
            &adapter,
            &consumer_session,
            &source_session,
            source_media_id,
            &consumer_rtp_parameters,
        )
        .await
        else {
            return;
        };
        let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
            return;
        };
        let source_shard = shards.shard_for_session(&source_session);
        let consumer_shard = shards.shard_for_session(&consumer_session);

        assert_eq!(
            source_shard.debug_relay_target_count_for_source(source_media_id),
            1
        );
        assert_eq!(
            source_shard.debug_active_relay_target_count_for_source(source_media_id),
            1
        );
        assert_eq!(
            consumer_shard
                .debug_remote_source_owner(source_media_id)
                .await,
            Some(source_session.clone())
        );
        let route_entry = consumer_shard
            .debug_route_entry_by_media_id(source_media_id)
            .await;
        assert!(route_entry.is_some());
        let Some(route_entry) = route_entry else {
            return;
        };
        assert!(route_entry.source_active);
        assert!(route_entry.destinations.iter().any(|destination| {
            destination.dest_session == consumer_session
                && destination.dest_transport_media_id == consumer_media_id
        }));

        assert!(
            adapter
                .remove_media(&consumer_session, consumer_media_id)
                .await
                .is_ok()
        );
        assert_eq!(
            consumer_shard
                .debug_remote_source_owner(source_media_id)
                .await,
            None
        );
        assert!(
            consumer_shard
                .debug_route_entry_by_media_id(source_media_id)
                .await
                .is_none()
        );
        assert_eq!(
            source_shard.debug_relay_target_count_for_source(source_media_id),
            0
        );
        assert_eq!(
            source_shard.debug_active_relay_target_count_for_source(source_media_id),
            0
        );
    }

    #[tokio::test]
    async fn rtc_adapter_keeps_independent_relay_targets_per_remote_worker() {
        let adapter = RuntimeTransportAdapter::builder()
            .rtc(RtcTransportAdapterShardSetConfig::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                RtcPortRange::new(46_300, 46_599),
                3,
                MediaCodecFlags::default(),
                Arc::new(MediaTap::default()),
                Arc::new(RuntimeMetrics::default()),
            ))
            .build();
        let source_session = TransportSessionKey::new(30, 0, 1, SessionId::Integer(1));
        let first_consumer_session = TransportSessionKey::new(30, 1, 2, SessionId::Integer(2));
        let second_consumer_session = TransportSessionKey::new(30, 2, 3, SessionId::Integer(3));
        let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 51_000);
        let first_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-1", 52_000);
        let second_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-2", 53_000);

        bootstrap_rtc_sessions(
            &adapter,
            &[
                &source_session,
                &first_consumer_session,
                &second_consumer_session,
            ],
        )
        .await;

        let Some(source_media_id) =
            publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
        else {
            return;
        };
        let Some(first_consumer_media_id) = consume_audio(
            &adapter,
            &first_consumer_session,
            &source_session,
            source_media_id,
            &first_consumer_rtp_parameters,
        )
        .await
        else {
            return;
        };
        let Some(second_consumer_media_id) = consume_audio(
            &adapter,
            &second_consumer_session,
            &source_session,
            source_media_id,
            &second_consumer_rtp_parameters,
        )
        .await
        else {
            return;
        };

        let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
            return;
        };
        let source_shard = shards.shard_for_session(&source_session);
        let first_consumer_shard = shards.shard_for_session(&first_consumer_session);
        let second_consumer_shard = shards.shard_for_session(&second_consumer_session);

        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 2, 2);
        assert_remote_source_owner(
            first_consumer_shard.as_ref(),
            source_media_id,
            Some(&source_session),
        )
        .await;
        assert_remote_source_owner(
            second_consumer_shard.as_ref(),
            source_media_id,
            Some(&source_session),
        )
        .await;

        assert!(
            adapter
                .remove_media(&first_consumer_session, first_consumer_media_id)
                .await
                .is_ok()
        );
        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);
        assert_remote_source_owner(
            second_consumer_shard.as_ref(),
            source_media_id,
            Some(&source_session),
        )
        .await;

        assert!(
            adapter
                .remove_media(&second_consumer_session, second_consumer_media_id)
                .await
                .is_ok()
        );
        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 0, 0);
    }

    #[tokio::test]
    async fn rtc_adapter_gates_remote_relay_mailboxes_without_touching_local_routes() {
        let adapter = test_rtc_adapter(2, RtcPortRange::new(46_600, 46_699));
        let source_session = TransportSessionKey::new(40, 0, 1, SessionId::Integer(1));
        let local_consumer_session = TransportSessionKey::new(40, 0, 2, SessionId::Integer(2));
        let remote_consumer_session = TransportSessionKey::new(40, 1, 3, SessionId::Integer(3));
        let producer_rtp_parameters = sample_audio_rtp_parameters("aud-up", 61_000);
        let local_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-local", 62_000);
        let remote_consumer_rtp_parameters = sample_audio_rtp_parameters("aud-down-remote", 63_000);

        bootstrap_rtc_sessions(
            &adapter,
            &[
                &source_session,
                &local_consumer_session,
                &remote_consumer_session,
            ],
        )
        .await;

        let Some(source_media_id) =
            publish_audio(&adapter, &source_session, &producer_rtp_parameters).await
        else {
            return;
        };
        let Some(local_consumer_media_id) = consume_audio(
            &adapter,
            &local_consumer_session,
            &source_session,
            source_media_id,
            &local_consumer_rtp_parameters,
        )
        .await
        else {
            return;
        };
        let Some(remote_consumer_media_id) = consume_audio(
            &adapter,
            &remote_consumer_session,
            &source_session,
            source_media_id,
            &remote_consumer_rtp_parameters,
        )
        .await
        else {
            return;
        };

        let RuntimeTransportAdapter::Rtc(shards) = &adapter else {
            return;
        };
        let source_shard = shards.shard_for_session(&source_session);
        let remote_consumer_shard = shards.shard_for_session(&remote_consumer_session);

        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);

        assert!(
            adapter
                .set_consumer_active(
                    &remote_consumer_session,
                    remote_consumer_media_id,
                    &source_session,
                    source_media_id,
                    false,
                )
                .await
                .is_ok()
        );

        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 0);
        assert_local_route_active(
            source_shard.as_ref(),
            &source_session,
            &local_consumer_session,
            local_consumer_media_id,
        )
        .await;
        assert_remote_route_activity(
            remote_consumer_shard.as_ref(),
            source_media_id,
            &remote_consumer_session,
            remote_consumer_media_id,
            false,
        )
        .await;

        assert!(
            adapter
                .set_consumer_active(
                    &remote_consumer_session,
                    remote_consumer_media_id,
                    &source_session,
                    source_media_id,
                    true,
                )
                .await
                .is_ok()
        );
        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 1, 1);

        assert!(
            adapter
                .remove_media(&remote_consumer_session, remote_consumer_media_id)
                .await
                .is_ok()
        );
        assert_relay_target_counts(source_shard.as_ref(), source_media_id, 0, 0);
    }
}
