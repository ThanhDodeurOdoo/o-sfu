use o_sfu_router::{
    ConsumerCapability, MediaKind as RouterMediaKind, RtpParameters as RouterRtpParameters,
    can_consume, negotiate_consumer_rtp_parameters,
};
use tracing::{error, warn};

use crate::runtime::transport_adapter::TransportMediaId;
use o_sfu_protocol::shared::{SessionId, StreamType};

use super::super::{
    super::{ChannelEventRequest, outbound::OutboundSender, topology::RoutedProducerId},
    ids::{ConsumerRuntimeId, ProducerRuntimeId},
    shared::{ChannelState, ConsumerKey, ConsumerState},
};
use super::router_stream_type::to_router_stream_type;

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_session_id: SessionId,
    pub(super) consumer_connection_id: u64,
    pub(super) producer_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_id: ProducerRuntimeId,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: RouterMediaKind,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PreparedConsumerBootstrap {
    consumer_rtp_parameters: RouterRtpParameters,
    sender: OutboundSender,
    consumer_active: bool,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: RouterMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone)]
pub(in crate::runtime::channel) struct PendingConsumerBootstrap {
    consumer_key: ConsumerKey,
    sender: OutboundSender,
    bootstrap: RemoteTrackBootstrap,
    consumer_active: bool,
    producer_owner_session_id: SessionId,
    producer_connection_id: u64,
    producer_stream_type: StreamType,
    producer_media_kind: RouterMediaKind,
    producer_routed_id: RoutedProducerId,
    producer_id: ProducerRuntimeId,
    producer_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteTrackBootstrap {
    consumer_id: ConsumerRuntimeId,
    media_kind: RouterMediaKind,
    mid: String,
    producer_id: ProducerRuntimeId,
    rtp_parameters: RouterRtpParameters,
    session_id: SessionId,
    active: bool,
    stream_type: StreamType,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime::channel) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
}

impl ChannelState {
    pub(in crate::runtime::channel) fn missing_consumer_targets_for_connection(
        &self,
        session_id: &SessionId,
        connection_id: u64,
    ) -> Option<Vec<PendingConsumerBootstrapTarget>> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != connection_id {
            return None;
        }
        if !session.negotiation.can_consume() {
            return Some(Vec::new());
        }
        Some(self.collect_missing_consumer_targets(session_id, connection_id))
    }

    pub(super) fn collect_missing_consumer_targets(
        &self,
        session_id: &SessionId,
        consumer_connection_id: u64,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                if producer.owner_session_id == *session_id {
                    return None;
                }
                let consumer_key =
                    ConsumerKey::new(session_id, &producer.owner_session_id, producer.stream_type);
                if self.consumer_bootstrap_exists(&consumer_key) {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id,
                    producer_session_id: producer.owner_session_id.clone(),
                    producer_connection_id: producer.owner_connection_id,
                    producer_id: *producer_id,
                    stream_type: producer.stream_type,
                    media_kind: producer.media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(in crate::runtime::channel) fn prepare_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PreparedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let session = self.sessions.get(&target.consumer_session_id)?;
            if session.connection_id != target.consumer_connection_id
                || !session.negotiation.can_consume()
            {
                return None;
            }
            (
                session.sender.clone(),
                session.parsed_client_rtp_capabilities.as_ref()?,
            )
        };
        let producer = self.producers.get(&target.producer_id)?;
        if producer.owner_session_id != target.producer_session_id
            || producer.owner_connection_id != target.producer_connection_id
            || producer.stream_type != target.stream_type
            || producer.media_kind != target.media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
        {
            return None;
        }
        let producer_owner_session_id = producer.owner_session_id.clone();
        let producer_stream_type = producer.stream_type;
        let producer_media_kind = producer.media_kind;
        let producer_routed_id = producer.routed_producer_id;
        let producer_consumable_rtp_parameters = producer.consumable_rtp_parameters.clone();
        let producer_active = producer.active;
        let consumer_active = self.desired_download_active(
            &target.consumer_session_id,
            &target.producer_session_id,
            target.stream_type,
        );

        if !can_consume(&producer_consumable_rtp_parameters, client_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            client_capabilities,
        )
        .ok()?;
        Some(PreparedConsumerBootstrap {
            consumer_rtp_parameters: negotiated_rtp_parameters,
            sender,
            consumer_active,
            producer_owner_session_id,
            producer_connection_id: producer.owner_connection_id,
            producer_stream_type,
            producer_media_kind,
            producer_routed_id,
            producer_id: target.producer_id,
            producer_active,
        })
    }

    pub(in crate::runtime::channel) fn prepare_consumer_bootstrap_transaction(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
    ) -> Option<PendingConsumerBootstrap> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&prepared.producer_id)?;
        if producer.owner_session_id != prepared.producer_owner_session_id
            || producer.owner_connection_id != prepared.producer_connection_id
            || producer.stream_type != prepared.producer_stream_type
            || producer.media_kind != prepared.producer_media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
            || producer.routed_producer_id != prepared.producer_routed_id
            || producer.active != prepared.producer_active
        {
            return None;
        }
        let consumer_key = ConsumerKey::new(
            &target.consumer_session_id,
            &target.producer_session_id,
            target.stream_type,
        );
        if self.consumer_bootstrap_exists(&consumer_key) {
            return None;
        }
        self.pending_consumer_bootstraps
            .insert(consumer_key.clone());
        let consumer_id = ConsumerRuntimeId::allocate(&mut self.next_consumer_id);
        Some(PendingConsumerBootstrap {
            consumer_key,
            sender: prepared.sender.clone(),
            bootstrap: RemoteTrackBootstrap {
                consumer_id,
                media_kind: prepared.producer_media_kind,
                mid: prepared
                    .consumer_rtp_parameters
                    .mid()
                    .map_or_else(|| consumer_id.into_wire_id(), ToOwned::to_owned),
                producer_id: prepared.producer_id,
                rtp_parameters: prepared.consumer_rtp_parameters.clone(),
                session_id: prepared.producer_owner_session_id.clone(),
                active: prepared.producer_active,
                stream_type: prepared.producer_stream_type,
            },
            consumer_active: prepared.consumer_active,
            producer_owner_session_id: prepared.producer_owner_session_id.clone(),
            producer_connection_id: prepared.producer_connection_id,
            producer_stream_type: prepared.producer_stream_type,
            producer_media_kind: prepared.producer_media_kind,
            producer_routed_id: prepared.producer_routed_id,
            producer_id: prepared.producer_id,
            producer_active: prepared.producer_active,
        })
    }

    pub(in crate::runtime::channel) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        mut pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
        consumer_mid: Option<String>,
    ) -> Option<(OutboundSender, RemoteTrackBootstrap, bool)> {
        self.pending_consumer_bootstraps
            .remove(&pending.consumer_key);
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.negotiation.can_consume()
        {
            return None;
        }
        let producer = self.producers.get(&pending.producer_id)?;
        if producer.owner_session_id != pending.producer_owner_session_id
            || producer.owner_connection_id != pending.producer_connection_id
            || producer.stream_type != pending.producer_stream_type
            || producer.media_kind != pending.producer_media_kind
            || producer.transport_media_id != Some(target.transport_media_id)
            || producer.routed_producer_id != pending.producer_routed_id
            || producer.active != pending.producer_active
        {
            return None;
        }
        if self.consumer_index.contains_key(&pending.consumer_key) {
            return None;
        }
        let routed_consumer_id = match self.topology.add_consumer(
            &target.consumer_session_id,
            pending.producer_routed_id,
            pending.producer_media_kind,
            to_router_stream_type(pending.producer_stream_type),
            ConsumerCapability::Compatible,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %pending.producer_id,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        if let Some(consumer_mid) = consumer_mid {
            pending.bootstrap.mid = consumer_mid;
        }
        if !pending.consumer_active
            && self
                .topology
                .set_consumer_paused(routed_consumer_id, true)
                .is_err()
        {
            error!(
                consumer_session_id = ?target.consumer_session_id,
                producer_id = %pending.producer_id,
                "failed to mirror initial consumer pause state into channel router"
            );
            return None;
        }
        self.consumer_index.insert(
            pending.consumer_key,
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer_connection_id,
                source_media: target.transport_media_id,
                consumer_media: consumer_transport_media_id,
            },
        );
        Some((pending.sender, pending.bootstrap, pending.consumer_active))
    }

    pub(in crate::runtime::channel) fn release_pending_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
    ) {
        self.pending_consumer_bootstraps.remove(&ConsumerKey::new(
            &target.consumer_session_id,
            &target.producer_session_id,
            target.stream_type,
        ));
    }

    pub(in crate::runtime::channel) fn consumer_bootstrap_exists(
        &self,
        consumer_key: &ConsumerKey,
    ) -> bool {
        self.consumer_index.contains_key(consumer_key)
            || self.pending_consumer_bootstraps.contains(consumer_key)
    }
}

impl PendingConsumerBootstrapTarget {
    pub(in crate::runtime::channel) const fn consumer_connection_id(&self) -> u64 {
        self.consumer_connection_id
    }

    pub(in crate::runtime::channel) fn consumer_session_id(&self) -> &SessionId {
        &self.consumer_session_id
    }

    pub(in crate::runtime::channel) const fn media_kind(&self) -> RouterMediaKind {
        self.media_kind
    }

    pub(in crate::runtime::channel) const fn producer_connection_id(&self) -> u64 {
        self.producer_connection_id
    }

    pub(in crate::runtime::channel) fn producer_session_id(&self) -> &SessionId {
        &self.producer_session_id
    }

    pub(in crate::runtime::channel) const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }
}

impl PreparedConsumerBootstrap {
    pub(in crate::runtime::channel) fn consumer_rtp_parameters(&self) -> &RouterRtpParameters {
        &self.consumer_rtp_parameters
    }
}

impl RemoteTrackBootstrap {
    pub(crate) fn mid(&self) -> &str {
        &self.mid
    }

    #[cfg(test)]
    pub(crate) fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub(crate) const fn active(&self) -> bool {
        self.active
    }

    pub(crate) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(crate) fn into_channel_event_request(self) -> ChannelEventRequest {
        ChannelEventRequest::BootstrapRemoteTrack(self)
    }
}
