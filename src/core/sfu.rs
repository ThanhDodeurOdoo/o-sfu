use std::sync::Arc;

use o_sfu_protocol::shared::{DownloadStates, StreamType, UserId, UserInfo};
use o_sfu_router::MediaCapabilities;

use crate::runtime::{
    AppliedSessionAnswer, ConnectionId, NegotiationPort, ObservabilityPort,
    RuntimeTransportAdapter, SessionOffer, SessionUploadEncoding, SessionUploadSlot,
    TransportAdapterError, TransportSessionHealth, room::Room,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OfferedMediaCapabilities(MediaCapabilities);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MediaNegotiationOffer {
    pub(crate) sdp: String,
    pub(crate) upload_slots: Vec<MediaUploadSlot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MediaUploadSlot {
    pub(crate) mid: String,
    pub(crate) kind: o_sfu_router::MediaKind,
    pub(crate) codecs: Vec<String>,
    pub(crate) simulcast_encodings: Vec<MediaUploadEncoding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MediaUploadEncoding {
    pub(crate) rid: String,
    pub(crate) max_bitrate: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaEndpointHealth {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SfuCoreError {
    Transport(TransportAdapterError),
    CapabilityProjection(TransportAdapterError),
    UserStateCommitRejected,
    UserStateRefreshRejected,
}

#[derive(Debug, Clone)]
pub(crate) struct SfuCore {
    room: Arc<Room>,
    transport_adapter: RuntimeTransportAdapter,
}

impl SfuCore {
    pub(crate) fn new(room: Arc<Room>, transport_adapter: RuntimeTransportAdapter) -> Self {
        Self {
            room,
            transport_adapter,
        }
    }

    pub(crate) fn endpoint_health(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<MediaEndpointHealth> {
        let session_key = self.room.transport_user_key(user_id, connection_id);
        self.transport_adapter
            .session_transport_health(&session_key)
            .map(|health| match health {
                TransportSessionHealth::Connected => MediaEndpointHealth::Connected,
                TransportSessionHealth::Disconnected => MediaEndpointHealth::Disconnected,
            })
    }

    pub(crate) async fn create_initial_offer(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Result<(MediaNegotiationOffer, OfferedMediaCapabilities), SfuCoreError> {
        let offered_capabilities =
            OfferedMediaCapabilities(self.room.router_rtp_capabilities().await);
        let session_key = self.room.transport_user_key(user_id, connection_id);
        let offer = self
            .transport_adapter
            .create_initial_session_offer(&session_key)
            .await
            .map_err(SfuCoreError::Transport)?;
        Ok((MediaNegotiationOffer::from(offer), offered_capabilities))
    }

    pub(crate) async fn create_renegotiation_offer(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Result<Option<MediaNegotiationOffer>, SfuCoreError> {
        let session_key = self.room.transport_user_key(user_id, connection_id);
        match self
            .transport_adapter
            .create_session_renegotiation_offer(&session_key)
            .await
        {
            Ok(offer) => Ok(Some(MediaNegotiationOffer::from(offer))),
            Err(TransportAdapterError::UnsupportedFeature) => Ok(None),
            Err(error) => Err(SfuCoreError::Transport(error)),
        }
    }

    pub(crate) async fn apply_initial_answer(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
        offered_capabilities: &OfferedMediaCapabilities,
    ) -> Result<(), SfuCoreError> {
        let applied_answer = self
            .apply_transport_answer(user_id, connection_id, answer_sdp)
            .await?;
        let client_capabilities = self
            .transport_adapter
            .negotiated_client_rtp_capabilities(answer_sdp, &offered_capabilities.0)
            .map_err(SfuCoreError::CapabilityProjection)?;
        if !self
            .room
            .apply_session_negotiated(
                user_id,
                connection_id,
                client_capabilities,
                &self.transport_adapter,
            )
            .await
        {
            return Err(SfuCoreError::UserStateCommitRejected);
        }
        self.commit_staged_publishes(user_id, connection_id, &applied_answer)
            .await;
        Ok(())
    }

    pub(crate) async fn apply_renegotiation_answer(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
    ) -> Result<(), SfuCoreError> {
        let applied_answer = self
            .apply_transport_answer(user_id, connection_id, answer_sdp)
            .await?;
        if !self
            .room
            .apply_session_refreshed(user_id, connection_id, &self.transport_adapter)
            .await
        {
            return Err(SfuCoreError::UserStateRefreshRejected);
        }
        self.commit_staged_publishes(user_id, connection_id, &applied_answer)
            .await;
        Ok(())
    }

    pub(crate) async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.room
            .has_staged_publish(user_id, connection_id, stream_type)
            .await
    }

    pub(crate) async fn is_stream_published(
        &self,
        user_id: &UserId,
        stream_type: StreamType,
    ) -> bool {
        self.room.is_stream_published(user_id, stream_type).await
    }

    pub(crate) async fn set_publication_active(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
        active: bool,
    ) {
        self.room
            .set_publication_active_runtime(
                user_id,
                connection_id,
                stream_type,
                active,
                &self.transport_adapter,
            )
            .await;
    }

    pub(crate) async fn update_subscription(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        target_user_id: &UserId,
        states: &DownloadStates,
    ) {
        self.room
            .update_subscription_runtime(
                user_id,
                connection_id,
                target_user_id,
                states,
                &self.transport_adapter,
            )
            .await;
    }

    pub(crate) async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        info: UserInfo,
        need_refresh: bool,
    ) {
        self.room
            .update_user_info_runtime_for_connection(
                user_id,
                connection_id,
                info,
                need_refresh,
                &self.transport_adapter,
            )
            .await;
    }

    pub(crate) async fn stage_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.room
            .stage_negotiated_publish(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    pub(crate) async fn rollback_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.room
            .rollback_staged_publish(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    pub(crate) async fn rollback_connection_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) {
        self.room
            .rollback_staged_publishes_for_connection(
                user_id,
                connection_id,
                &self.transport_adapter,
            )
            .await;
    }

    pub(crate) async fn unpublish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> bool {
        self.room
            .unpublish_track(user_id, connection_id, stream_type, &self.transport_adapter)
            .await
    }

    async fn apply_transport_answer(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, SfuCoreError> {
        let session_key = self.room.transport_user_key(user_id, connection_id);
        self.transport_adapter
            .apply_session_answer(&session_key, answer_sdp)
            .await
            .map_err(SfuCoreError::Transport)
    }

    async fn commit_staged_publishes(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        applied_answer: &AppliedSessionAnswer,
    ) {
        self.room
            .commit_staged_publishes(
                user_id,
                connection_id,
                applied_answer,
                &self.transport_adapter,
                &self.transport_adapter,
            )
            .await;
    }
}

impl From<SessionOffer> for MediaNegotiationOffer {
    fn from(offer: SessionOffer) -> Self {
        let (sdp, upload_slots) = offer.into_parts();
        Self {
            sdp,
            upload_slots: upload_slots
                .into_iter()
                .map(MediaUploadSlot::from)
                .collect(),
        }
    }
}

impl From<SessionUploadSlot> for MediaUploadSlot {
    fn from(slot: SessionUploadSlot) -> Self {
        Self {
            mid: slot.mid,
            kind: slot.kind,
            codecs: slot.codecs,
            simulcast_encodings: slot
                .simulcast_encodings
                .into_iter()
                .map(MediaUploadEncoding::from)
                .collect(),
        }
    }
}

impl From<SessionUploadEncoding> for MediaUploadEncoding {
    fn from(encoding: SessionUploadEncoding) -> Self {
        Self {
            rid: encoding.rid,
            max_bitrate: encoding.max_bitrate,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Instant,
    };

    use o_sfu_protocol::shared::UserId;
    use str0m::{Candidate, Rtc, change::SdpOffer};

    use super::MediaNegotiationOffer;
    use crate::{
        config::{MediaCodecFlags, RtcPortRange},
        runtime::{
            DiagnosticsStore, MediaTap, NegotiationPort, RtcTransportAdapterShardSetConfig,
            RuntimeMetrics, RuntimeTransportAdapter, SessionBitrateLimits, TransportAdapterError,
            test_transport_session_key,
        },
    };

    fn build_real_rtc_transport_adapter(port_min: u16) -> RuntimeTransportAdapter {
        RuntimeTransportAdapter::rtc(&RtcTransportAdapterShardSetConfig::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            SessionBitrateLimits::new(8_000_000, 10_000_000),
            RtcPortRange::new(port_min, port_min.saturating_add(99)),
            1,
            MediaCodecFlags::default(),
            Arc::new(DiagnosticsStore::default()),
            Arc::new(MediaTap::default()),
            Arc::new(RuntimeMetrics::default()),
        ))
    }

    fn answer_offer(offer_sdp: &str, port: u16) -> Option<String> {
        let mut rtc = Rtc::new(Instant::now());
        rtc.add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp").ok()?,
        )?;
        let answer = rtc
            .sdp_api()
            .accept_offer(SdpOffer::from_sdp_string(offer_sdp).ok()?)
            .ok()?;
        Some(answer.to_sdp_string())
    }

    #[tokio::test]
    async fn transport_renegotiation_offer_reports_unsupported_without_pending_offer() {
        let transport_adapter = build_real_rtc_transport_adapter(58_100);
        let session_key = test_transport_session_key(7, 0, 11, UserId::Integer(19));
        let initial_offer_result = transport_adapter
            .create_initial_session_offer(&session_key)
            .await;
        assert!(
            initial_offer_result.is_ok(),
            "expected initial rtc offer, got {initial_offer_result:?}"
        );
        let Ok(initial_offer) = initial_offer_result else {
            return;
        };
        let initial_offer_sdp = initial_offer.into_sdp();
        let answer_sdp = answer_offer(&initial_offer_sdp, 58_300);
        assert!(
            answer_sdp.is_some(),
            "expected answerer to accept the initial rtc offer"
        );
        let Some(answer_sdp) = answer_sdp else {
            return;
        };
        assert!(
            transport_adapter
                .apply_session_answer(&session_key, &answer_sdp)
                .await
                .is_ok()
        );

        let renegotiation_offer = transport_adapter
            .create_session_renegotiation_offer(&session_key)
            .await;

        assert_eq!(
            renegotiation_offer.map(MediaNegotiationOffer::from),
            Err(TransportAdapterError::UnsupportedFeature)
        );
    }

    #[tokio::test]
    async fn transport_renegotiation_offer_keeps_missing_session_fatal() {
        let transport_adapter = build_real_rtc_transport_adapter(58_400);
        let missing_session_key = test_transport_session_key(8, 0, 12, UserId::Integer(20));

        let renegotiation_offer = transport_adapter
            .create_session_renegotiation_offer(&missing_session_key)
            .await;

        assert_eq!(
            renegotiation_offer.map(MediaNegotiationOffer::from),
            Err(TransportAdapterError::TransportUnavailable)
        );
    }
}
