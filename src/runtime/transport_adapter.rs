use std::{collections::BTreeMap, fmt::Debug, net::IpAddr, sync::Arc};

use super::{rtc_adapter::RtcTransportAdapter, stub_bus::StubWebRtcAdapter};
use crate::runtime::recording::MediaTap;

use crate::config::RtcPortRange;
use crate::signaling::{
    current_protocol::CurrentTransportBootstrapPayload,
    shared::SessionId,
    webrtc::{DtlsParameters, IceParameters, MediaKind as SignalingMediaKind},
};
use o_sfu_router::RtpParameters as RouterRtpParameters;
use str0m::media::MediaKind as Str0mMediaKind;

/// Channel-scoped transport-adapter session identity.
///
/// the transport boundary must distinguish identical session ids that are active in
/// different channels at the same time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TransportSessionKey {
    channel_runtime: u64,
    media_worker: usize,
    connection: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TransportConnectDirection {
    Upload,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportAdapterError {
    TransportUnavailable,
    InvalidInput,
    UnsupportedFeature,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TransportBitrateSnapshot {
    pub(crate) total: u64,
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
    fn new(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        worker_count: usize,
        media_tap: Arc<MediaTap>,
    ) -> Self {
        let Some(shard_ranges) = rtc_port_range.split_for_workers(worker_count) else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(
                    public_ip,
                    rtc_port_range,
                    media_tap,
                )),
                extra_shards: Vec::new(),
            };
        };
        let mut shard_ranges = shard_ranges.into_iter();
        let Some(primary_range) = shard_ranges.next() else {
            return Self {
                primary_shard: Arc::new(RtcTransportAdapter::new(
                    public_ip,
                    rtc_port_range,
                    media_tap,
                )),
                extra_shards: Vec::new(),
            };
        };
        Self {
            primary_shard: Arc::new(RtcTransportAdapter::new(
                public_ip,
                primary_range,
                Arc::clone(&media_tap),
            )),
            extra_shards: shard_ranges
                .map(|range| {
                    Arc::new(RtcTransportAdapter::new(
                        public_ip,
                        range,
                        Arc::clone(&media_tap),
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
}

impl RuntimeTransportAdapter {
    #[must_use]
    pub(crate) fn stub() -> Self {
        Self::Stub(Arc::new(StubWebRtcAdapter::default()))
    }

    #[must_use]
    pub(crate) fn rtc(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        worker_count: usize,
        media_tap: Arc<MediaTap>,
    ) -> Self {
        Self::Rtc(Arc::new(RtcTransportAdapterShardSet::new(
            public_ip,
            rtc_port_range,
            worker_count,
            media_tap,
        )))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn from_stub_adapter(adapter: Arc<StubWebRtcAdapter>) -> Self {
        Self::Stub(adapter)
    }

    /// Build the `INIT_TRANSPORTS` payload for a newly authenticated session.
    pub(crate) async fn transport_bootstrap_payload(
        &self,
        session_key: &TransportSessionKey,
        router_capabilities: &o_sfu_router::RtpCapabilities,
    ) -> Result<CurrentTransportBootstrapPayload, TransportAdapterError> {
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

    /// Connect one direction transport with client DTLS parameters.
    pub(crate) async fn connect_transport(
        &self,
        session_key: &TransportSessionKey,
        direction: TransportConnectDirection,
        dtls_parameters: &DtlsParameters,
        ice_parameters: Option<&IceParameters>,
        sdp_offer: Option<&str>,
    ) -> Result<(), TransportAdapterError> {
        match self {
            Self::Stub(adapter) => {
                adapter
                    .connect_transport(
                        session_key,
                        direction,
                        dtls_parameters,
                        ice_parameters,
                        sdp_offer,
                    )
                    .await
            }
            Self::Rtc(adapter) => {
                adapter
                    .shard_for_session(session_key)
                    .connect_transport(
                        session_key,
                        direction,
                        dtls_parameters,
                        ice_parameters,
                        sdp_offer,
                    )
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
                adapter
                    .shard_for_session(session_key)
                    .close_session(session_key)
                    .await
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
                adapter
                    .shard_for_session(session_key)
                    .remove_media(session_key, transport_media_id)
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
                adapter
                    .shard_for_session(consumer_session_key)
                    .add_send_media(
                        consumer_session_key,
                        signaling_to_str0m_media_kind(media_kind),
                        source_session_key,
                        source_media_id,
                        consumer_rtp_parameters,
                    )
                    .await
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

    use super::RuntimeTransportAdapter;
    use crate::{
        config::RtcPortRange,
        runtime::{recording::MediaTap, transport_adapter::TransportSessionKey},
        signaling::shared::SessionId,
    };
    use o_sfu_router::RtpCapabilities as RouterRtpCapabilities;

    fn empty_router_capabilities() -> RouterRtpCapabilities {
        RouterRtpCapabilities::new(vec![], vec![])
    }

    #[tokio::test]
    async fn rtc_adapter_shards_channel_bootstrap_by_explicit_media_worker() {
        let adapter = RuntimeTransportAdapter::rtc(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            RtcPortRange::new(46_000, 46_003),
            2,
            Arc::new(MediaTap::default()),
        );
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
}
