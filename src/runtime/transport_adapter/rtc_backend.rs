use std::{collections::BTreeSet, time::Instant};

use o_sfu_router::{MediaCapabilities, MediaKind, MediaStream as RouterRtpParameters};
use str0m::media::MediaKind as Str0mMediaKind;

use super::{
    ports::{MediaPort, NegotiationPort, ObservabilityPort, SessionPort, SourcePolicyPort},
    shard_set::RtcTransportAdapterShardSet,
    types::{
        ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, AppliedSessionAnswer,
        ConsumerPacketGateUpdate, ReceiverBandwidthSnapshot, SessionOffer, SourcePacketGate,
        TransportAdapterError, TransportBitrateSnapshot, TransportMediaId, TransportSessionKey,
    },
};
use crate::runtime::{
    RoomInstanceId,
    rtc_adapter::{TransportSessionHealth, client_rtp_capabilities_from_answer},
    transport_adapter::SourcePolicyUpdateSubscription,
};

impl NegotiationPort for RtcTransportAdapterShardSet {
    async fn create_initial_session_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_initial_session_offer(session_key)
            .await
    }

    async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .create_session_renegotiation_offer(session_key)
            .await
    }

    async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        self.shard_for_user(session_key)
            .negotiation()
            .apply_session_answer(session_key, answer_sdp)
            .await
    }

    fn negotiated_client_rtp_capabilities(
        &self,
        answer_sdp: &str,
        _offered_router_capabilities: &MediaCapabilities,
    ) -> Result<MediaCapabilities, TransportAdapterError> {
        client_rtp_capabilities_from_answer(answer_sdp).ok_or(TransportAdapterError::InvalidInput)
    }
}

impl SessionPort for RtcTransportAdapterShardSet {
    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let session_shard = self.shard_for_user(session_key);
        let close_outcome = session_shard
            .users()
            .close_session_with_outcome(session_key)
            .await?;
        self.release_relay_cleanup(&session_shard, close_outcome.relay_cleanup());
        Ok(())
    }
}

impl MediaPort for RtcTransportAdapterShardSet {
    async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let session_shard = self.shard_for_user(session_key);
        let remove_outcome = session_shard
            .media()
            .remove_media_with_outcome(session_key, transport_media_id)
            .await?;
        if let Some(cleanup) = remove_outcome.relay_cleanup() {
            let relay_cleanup = [cleanup.clone()];
            self.release_relay_cleanup(&session_shard, &relay_cleanup);
        }
        Ok(())
    }

    async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.shard_for_user(session_key)
            .media()
            .add_recv_media(
                session_key,
                signaling_to_str0m_media_kind(media_kind),
                rtp_parameters,
            )
            .await
    }

    async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        let relay_route = self.relay_registration_shards(consumer_session_key, source_session_key);
        let remote_source_control = relay_route
            .as_ref()
            .map(|(source_shard, consumer_shard)| {
                source_shard
                    .media()
                    .remote_source_control(consumer_shard.as_ref())
            })
            .transpose()?;
        if let Some((source_shard, consumer_shard)) = &relay_route {
            source_shard
                .media()
                .activate_relay_route(source_media_id, consumer_shard.as_ref())?;
        }
        let consumer_shard = self.shard_for_user(consumer_session_key);
        let add_result = consumer_shard
            .media()
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
                source_shard.media().set_relay_route_active(
                    source_media_id,
                    consumer_shard.as_ref(),
                    true,
                );
            } else {
                source_shard
                    .media()
                    .deactivate_relay_route(source_media_id, consumer_shard.as_ref());
            }
        }
        add_result
    }

    async fn set_producer_active(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.shard_for_user(session_key)
            .media()
            .set_producer_active(session_key, transport_media_id, active)
            .await
    }

    async fn set_consumer_active(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .set_consumer_active(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                active,
            )
            .await
    }

    async fn set_consumer_packet_gate(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        packet_gate: SourcePacketGate,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .set_consumer_packet_gate(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
                packet_gate,
            )
            .await
    }

    async fn set_consumer_packet_gates(
        &self,
        updates: &[ConsumerPacketGateUpdate],
    ) -> Vec<Result<(), TransportAdapterError>> {
        self.set_consumer_packet_gates(updates).await
    }

    async fn request_consumer_keyframe(
        &self,
        consumer_session_key: &TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source_session_key: &TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        ensure_same_room_instance(consumer_session_key, source_session_key)?;
        self.shard_for_user(consumer_session_key)
            .media()
            .request_consumer_keyframe(
                consumer_session_key,
                consumer_transport_media_id,
                source_session_key,
                source_transport_media_id,
            )
            .await
    }

    async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.shard_for_user(session_key)
            .media()
            .transport_media_mid(transport_media_id)
            .await
            .ok()
            .flatten()
    }
}

impl ObservabilityPort for RtcTransportAdapterShardSet {
    fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        Self::transport_bitrate_snapshot(self, session_keys)
    }

    fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        Self::receiver_bandwidth_snapshot(self, session_keys)
    }

    async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        Self::active_speaker_source_snapshot(self).await
    }

    async fn active_speaker_diagnostic_snapshot(&self) -> Vec<ActiveSpeakerSourceDiagnostic> {
        Self::active_speaker_diagnostic_snapshot(self).await
    }

    async fn next_active_speaker_deadline(&self) -> Option<Instant> {
        Self::next_active_speaker_deadline(self).await
    }

    async fn expired_active_speaker_room_instance_ids(
        &self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        Self::expired_active_speaker_room_instance_ids(self, now).await
    }

    fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.shard_for_user(session_key)
            .observability()
            .session_transport_health(session_key)
    }
}

impl SourcePolicyPort for RtcTransportAdapterShardSet {
    fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        Self::source_policy_subscription(self)
    }
}

fn ensure_same_room_instance(
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
) -> Result<(), TransportAdapterError> {
    if consumer_session_key.room_instance_id() == source_session_key.room_instance_id() {
        return Ok(());
    }
    Err(TransportAdapterError::InvalidInput)
}

fn signaling_to_str0m_media_kind(kind: MediaKind) -> Str0mMediaKind {
    match kind {
        MediaKind::Audio => Str0mMediaKind::Audio,
        MediaKind::Video => Str0mMediaKind::Video,
    }
}
