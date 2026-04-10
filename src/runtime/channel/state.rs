use std::collections::BTreeMap;

use o_sfu_router::{
    MediaKind as RouterMediaKind, RouterId, RtpParameters as RouterRtpParameters,
    StreamType as RouterStreamType, can_consume, negotiate_consumer_rtp_parameters,
};
use tokio::sync::mpsc;
use tracing::{error, warn};

use crate::signaling::{
    current_protocol::{CurrentRemoteTrackBootstrapPayload, CurrentServerRequest},
    ortc_mapper,
    shared::{RecordingState, SessionId, SessionInfo, SessionPermissions, StreamType},
    webrtc::{
        MediaKind as SignalingMediaKind, RtpCapabilities as SignalingRtpCapabilities, RtpParameters,
    },
};

use super::{
    SessionOutbound,
    topology::{ChannelTopology, RoutedConsumerId, RoutedProducerId},
};
use crate::runtime::transport_adapter::TransportMediaId;

#[derive(Debug)]
pub(super) struct ChannelState {
    pub(super) sessions: BTreeMap<SessionId, ActiveSession>,
    pub(super) next_connection_id: u64,
    next_producer_id: u64,
    next_consumer_id: u64,
    pub(super) recording_state: RecordingState,
    pub(super) producers: BTreeMap<String, PublishedProducer>,
    pub(super) consumer_index: BTreeMap<ConsumerKey, ConsumerState>,
    pub(super) topology: ChannelTopology,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ConsumerKey {
    pub(super) consumer_session_id: SessionId,
    pub(super) producer_session_id: SessionId,
    pub(super) stream_type: StreamType,
}

#[derive(Debug)]
pub(super) struct ActiveSession {
    #[allow(
        dead_code,
        reason = "stored for future session display and recording metadata"
    )]
    pub(super) label: Option<String>,
    #[allow(dead_code, reason = "stored for future permission-gated actions")]
    pub(super) permissions: SessionPermissions,
    pub(super) info: SessionInfo,
    pub(super) client_rtp_capabilities: Option<SignalingRtpCapabilities>,
    pub(super) upload_transport_connected: bool,
    pub(super) download_transport_connected: bool,
    pub(super) connection_id: u64,
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
}

#[derive(Debug, Clone)]
pub(super) struct PublishedProducer {
    pub(super) owner_session_id: SessionId,
    pub(super) owner_connection_id: u64,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) consumable_rtp_parameters: RouterRtpParameters,
    pub(super) routed_producer_id: RoutedProducerId,
    pub(super) transport_media_id: Option<TransportMediaId>,
    pub(super) active: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ConsumerState {
    pub(super) routed_consumer_id: RoutedConsumerId,
    pub(super) consumer_connection_id: u64,
    pub(super) source_connection_id: u64,
    pub(super) source_media: TransportMediaId,
    pub(super) consumer_media: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(super) struct PendingConsumerBootstrapTarget {
    pub(super) consumer_session_id: SessionId,
    pub(super) consumer_connection_id: u64,
    pub(super) producer_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_wire_id: String,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedConsumerBootstrap {
    pub(super) consumer_rtp_parameters: RouterRtpParameters,
    pub(super) consumer_wire_rtp_parameters: RtpParameters,
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
    pub(super) producer_owner_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_stream_type: StreamType,
    pub(super) producer_media_kind: SignalingMediaKind,
    pub(super) producer_routed_id: RoutedProducerId,
    pub(super) producer_wire_id: String,
    pub(super) producer_active: bool,
}

#[derive(Debug, Clone)]
pub(super) struct PendingPublishedTrack {
    pub(super) producer_wire_id: String,
    pub(super) owner_session_id: SessionId,
    pub(super) owner_connection_id: u64,
    pub(super) stream_type: StreamType,
    pub(super) media_kind: SignalingMediaKind,
    pub(super) consumable_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug, Clone)]
pub(super) struct PendingConsumerBootstrap {
    pub(super) sender: mpsc::UnboundedSender<SessionOutbound>,
    pub(super) request: CurrentServerRequest,
    pub(super) producer_owner_session_id: SessionId,
    pub(super) producer_connection_id: u64,
    pub(super) producer_stream_type: StreamType,
    pub(super) producer_media_kind: SignalingMediaKind,
    pub(super) producer_routed_id: RoutedProducerId,
    pub(super) producer_wire_id: String,
    pub(super) producer_active: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ConsumerBootstrapOrigin {
    LateJoin,
    Publish,
}

impl ChannelState {
    pub(super) fn new(router_id: RouterId) -> Self {
        Self {
            sessions: BTreeMap::new(),
            next_connection_id: 0,
            next_producer_id: 1,
            next_consumer_id: 1,
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            producers: BTreeMap::new(),
            consumer_index: BTreeMap::new(),
            topology: ChannelTopology::new(router_id),
        }
    }

    pub(super) fn purge_session_media_state(&mut self, session_id: &SessionId) {
        self.producers
            .retain(|_wire_id, producer| producer.owner_session_id != *session_id);
        self.consumer_index.retain(|key, _consumer_id| {
            key.consumer_session_id != *session_id && key.producer_session_id != *session_id
        });
    }

    pub(super) fn late_join_consumer_targets(
        &self,
        session_id: &SessionId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        let Some(session) = self.sessions.get(session_id) else {
            return Vec::new();
        };
        if !session.download_transport_connected || session.client_rtp_capabilities.is_none() {
            return Vec::new();
        }

        self.producers
            .iter()
            .filter_map(|(producer_wire_id, producer)| {
                let transport_media_id = producer.transport_media_id?;
                if producer.owner_session_id == *session_id {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: session_id.clone(),
                    consumer_connection_id: session.connection_id,
                    producer_session_id: producer.owner_session_id.clone(),
                    producer_connection_id: producer.owner_connection_id,
                    producer_wire_id: producer_wire_id.clone(),
                    stream_type: producer.stream_type,
                    media_kind: producer.media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(super) fn publish_consumer_targets(
        &self,
        producer_session_id: &SessionId,
        producer_connection_id: u64,
        producer_wire_id: &str,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        transport_media_id: TransportMediaId,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.sessions
            .iter()
            .filter_map(|(peer_session_id, peer_session)| {
                if peer_session_id == producer_session_id
                    || !peer_session.download_transport_connected
                    || peer_session.client_rtp_capabilities.is_none()
                {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget {
                    consumer_session_id: peer_session_id.clone(),
                    consumer_connection_id: peer_session.connection_id,
                    producer_session_id: producer_session_id.clone(),
                    producer_connection_id,
                    producer_wire_id: producer_wire_id.to_owned(),
                    stream_type,
                    media_kind,
                    transport_media_id,
                })
            })
            .collect()
    }

    pub(super) fn prepare_published_track(
        &mut self,
        session_id: &SessionId,
        publisher_connection_id: u64,
        stream_type: StreamType,
        media_kind: SignalingMediaKind,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> Option<PendingPublishedTrack> {
        let session = self.sessions.get(session_id)?;
        if session.connection_id != publisher_connection_id || !session.upload_transport_connected {
            return None;
        }
        Some(PendingPublishedTrack {
            producer_wire_id: allocate_wire_producer_id(&mut self.next_producer_id),
            owner_session_id: session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type,
            media_kind,
            consumable_rtp_parameters,
        })
    }

    pub(super) fn commit_published_track(
        &mut self,
        pending: PendingPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(String, Vec<PendingConsumerBootstrapTarget>)> {
        let session = self.sessions.get(&pending.owner_session_id)?;
        if session.connection_id != pending.owner_connection_id
            || !session.upload_transport_connected
        {
            return None;
        }
        let routed_producer_id = match self.topology.add_producer(
            &pending.owner_session_id,
            to_router_media_kind(pending.media_kind),
            to_router_stream_type(pending.stream_type),
        ) {
            Ok(producer_id) => producer_id,
            Err(_error) => {
                error!(
                    session_id = ?pending.owner_session_id,
                    "failed to mirror publish request into channel router producer state"
                );
                return None;
            }
        };
        self.producers.insert(
            pending.producer_wire_id.clone(),
            PublishedProducer {
                owner_session_id: pending.owner_session_id.clone(),
                owner_connection_id: pending.owner_connection_id,
                stream_type: pending.stream_type,
                media_kind: pending.media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
        );
        let consumer_targets = self.publish_consumer_targets(
            &pending.owner_session_id,
            pending.owner_connection_id,
            &pending.producer_wire_id,
            pending.stream_type,
            pending.media_kind,
            transport_media_id,
        );
        Some((pending.producer_wire_id, consumer_targets))
    }

    pub(super) fn prepare_consumer_bootstrap_transaction(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        prepared: &PreparedConsumerBootstrap,
    ) -> Option<PendingConsumerBootstrap> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.download_transport_connected
        {
            return None;
        }
        let producer = self.producers.get(&prepared.producer_wire_id)?;
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
        let consumer_id = allocate_wire_consumer_id(&mut self.next_consumer_id);
        Some(PendingConsumerBootstrap {
            sender: prepared.sender.clone(),
            request: CurrentServerRequest::BootstrapRemoteTrack(
                CurrentRemoteTrackBootstrapPayload {
                    id: consumer_id,
                    media_kind: prepared.producer_media_kind,
                    source_id: prepared.producer_wire_id.clone(),
                    rtp_parameters: prepared.consumer_wire_rtp_parameters.clone(),
                    session_id: prepared.producer_owner_session_id.clone(),
                    active: prepared.producer_active,
                    stream_type: prepared.producer_stream_type,
                },
            ),
            producer_owner_session_id: prepared.producer_owner_session_id.clone(),
            producer_connection_id: prepared.producer_connection_id,
            producer_stream_type: prepared.producer_stream_type,
            producer_media_kind: prepared.producer_media_kind,
            producer_routed_id: prepared.producer_routed_id,
            producer_wire_id: prepared.producer_wire_id.clone(),
            producer_active: prepared.producer_active,
        })
    }

    pub(super) fn prepare_consumer_bootstrap(
        &self,
        target: &PendingConsumerBootstrapTarget,
    ) -> Option<PreparedConsumerBootstrap> {
        let (sender, client_capabilities) = {
            let session = self.sessions.get(&target.consumer_session_id)?;
            if session.connection_id != target.consumer_connection_id
                || !session.download_transport_connected
            {
                return None;
            }
            (
                session.sender.clone(),
                session.client_rtp_capabilities.clone()?,
            )
        };
        let producer = self.producers.get(&target.producer_wire_id)?;
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

        let parsed_capabilities = ortc_mapper::parse_rtp_capabilities(&client_capabilities.0)?;
        if !can_consume(&producer_consumable_rtp_parameters, &parsed_capabilities) {
            return None;
        }
        let negotiated_rtp_parameters = negotiate_consumer_rtp_parameters(
            &producer_consumable_rtp_parameters,
            &parsed_capabilities,
        )
        .ok()?;
        let consumer_wire_rtp_parameters = RtpParameters(ortc_mapper::serialize_rtp_parameters(
            &negotiated_rtp_parameters,
        ));
        Some(PreparedConsumerBootstrap {
            consumer_rtp_parameters: negotiated_rtp_parameters,
            consumer_wire_rtp_parameters,
            sender,
            producer_owner_session_id,
            producer_connection_id: producer.owner_connection_id,
            producer_stream_type,
            producer_media_kind,
            producer_routed_id,
            producer_wire_id: target.producer_wire_id.clone(),
            producer_active,
        })
    }

    pub(super) fn commit_consumer_bootstrap(
        &mut self,
        target: &PendingConsumerBootstrapTarget,
        pending: PendingConsumerBootstrap,
        consumer_transport_media_id: TransportMediaId,
    ) -> Option<(mpsc::UnboundedSender<SessionOutbound>, CurrentServerRequest)> {
        let session = self.sessions.get(&target.consumer_session_id)?;
        if session.connection_id != target.consumer_connection_id
            || !session.download_transport_connected
        {
            return None;
        }
        let producer = self.producers.get(&pending.producer_wire_id)?;
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
        let routed_consumer_id = match self.topology.add_consumer(
            &target.consumer_session_id,
            pending.producer_routed_id,
            to_router_media_kind(pending.producer_media_kind),
            to_router_stream_type(pending.producer_stream_type),
            true,
        ) {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    consumer_session_id = ?target.consumer_session_id,
                    producer_id = %pending.producer_wire_id,
                    "router rejected consumer creation"
                );
                return None;
            }
        };
        self.consumer_index.insert(
            ConsumerKey {
                consumer_session_id: target.consumer_session_id.clone(),
                producer_session_id: pending.producer_owner_session_id.clone(),
                stream_type: pending.producer_stream_type,
            },
            ConsumerState {
                routed_consumer_id,
                consumer_connection_id: target.consumer_connection_id,
                source_connection_id: pending.producer_connection_id,
                source_media: target.transport_media_id,
                consumer_media: consumer_transport_media_id,
            },
        );
        Some((pending.sender, pending.request))
    }

    #[cfg(test)]
    pub(super) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<o_sfu_router::SessionPermissions> {
        self.topology.session_permissions(session_id)
    }
}

pub(super) fn allocate_wire_producer_id(next_producer_id: &mut u64) -> String {
    let current = *next_producer_id;
    *next_producer_id = next_producer_id.saturating_add(1);
    format!("producer-{current}")
}

pub(super) fn allocate_wire_consumer_id(next_consumer_id: &mut u64) -> String {
    let current = *next_consumer_id;
    *next_consumer_id = next_consumer_id.saturating_add(1);
    format!("consumer-{current}")
}

fn to_router_media_kind(media_kind: SignalingMediaKind) -> RouterMediaKind {
    match media_kind {
        SignalingMediaKind::Audio => RouterMediaKind::Audio,
        SignalingMediaKind::Video => RouterMediaKind::Video,
    }
}

fn to_router_stream_type(stream_type: StreamType) -> RouterStreamType {
    match stream_type {
        StreamType::Audio => RouterStreamType::Audio,
        StreamType::Camera => RouterStreamType::Camera,
        StreamType::Screen => RouterStreamType::Screen,
    }
}
