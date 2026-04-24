//! Producer-side channel state transitions.
//!
//! the pure state work around publishing, unpublishing and producer activity changes
//! after transport negotiation is done elsewhere.
//! The main rule here is that channel state only commits a producer once the
//! caller has a validated session and a real transport media id to attach.

use std::collections::BTreeMap;

use o_sfu_protocol::shared::{SessionId, SessionInfo, StreamType};
use o_sfu_router::{
    MediaFormat, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, Mid, Rid, Ssrc,
};
use tracing::{error, warn};

use super::{
    super::{
        super::{
            ChannelEventMessage, SessionOutbound, TrackBindingUpdate,
            outbound::{MessageFanout, OutboundSender},
            topology::RoutedProducerId,
        },
        ids::ProducerRuntimeId,
        shared::{
            ChannelState, ConsumerKey, PublishedProducer, SourceKey,
            SourceTransportMediaIndexEntry, TransportMediaRemoval,
        },
    },
    router_stream_type::to_router_stream_type,
    subscription::{ConsumerBootstrapProducerSnapshot, PendingConsumerBootstrapTarget},
};
use crate::runtime::{
    ConnectionId,
    source_model::{
        PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
        PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceModelError, SourceTransportBinding,
    },
    transport_adapter::TransportMediaId,
};

#[allow(
    clippy::struct_field_names,
    reason = "postfix _id is intentional because the fields are all identity values"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
/// Stable handle for a live published track.
///
/// Callers resolve this from current chanel state right before mutating a
/// producer. useful so so stale replacement callbacks can be rejected
/// without guessing which layer drifted.
pub(in crate::runtime::channel) struct ProducerRouteTarget {
    source_id: PublishedSourceId,
    producer_id: ProducerRuntimeId,
    owner_connection_id: ConnectionId,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
/// Publish request that passed the current session-level checks.
///
/// This exists so transport work can happen after validation without carrying
/// a mutable channel borrow.
/// It only proves that the session was publish-ready at validation time.
/// Commit must re-check the same ownership and readiness before state changes land.
pub(in crate::runtime::channel) struct ValidatedPublishDescriptor {
    owner_session_id: SessionId,
    owner_connection_id: ConnectionId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
}

#[derive(Debug, Clone)]
/// Publish payload that is ready to be committed into channel state.
///
/// This is the last pure state input before a producer becomes live. The
/// transport layer already allocated the media handle and the caller already
/// derived consumable router parameters, so commit can stay a small
/// all ornothing state transition.
pub(in crate::runtime::channel) struct PreparedPublishedTrack {
    owner_session_id: SessionId,
    owner_connection_id: ConnectionId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
    consumable_rtp_parameters: RouterRtpParameters,
}

#[derive(Debug)]
struct PublishedSourceInstall {
    source_key: SourceKey,
    source_descriptor: PublishedSourceDescriptor,
    source_encoding_ids: Vec<SourceEncodingId>,
    producer_id: ProducerRuntimeId,
    routed_producer_id: RoutedProducerId,
    pending: PreparedPublishedTrack,
    transport_media_id: TransportMediaId,
}

#[derive(Debug)]
/// Result of toggling a producer between active and paused.
///
/// The chanel commits router pause state first and only then stages the
/// outward fanout. That keeps session info updates aligned with the lasting
/// router state instead of reporting a local pause change that never stuck.
pub(in crate::runtime::channel) struct ProducerActivityOutcome {
    pub(in crate::runtime::channel) transport_media_id: TransportMediaId,
    pub(in crate::runtime::channel) active: bool,
    pub(in crate::runtime::channel) fanout: MessageFanout,
}

#[derive(Debug)]
/// Deferred side effects for explicit unpublish.
///
/// The state transition removes the producer and all dependent consumer
/// bookkeeping first. Emmiting the track removal and optional session-info
/// update stays outside the state lock.
pub(in crate::runtime::channel) struct UnpublishTrackOutcome {
    recipients: Vec<OutboundSender>,
    session_info_snapshot: Option<BTreeMap<SessionId, SessionInfo>>,
}

impl ChannelState {
    /// Validates that a session may start a negotiated publish right now.
    ///
    /// This is the pure state gate in front of the staged publish flow. The
    /// caller may use the returned descriptor to drive transport work, but it
    /// must still commit through `commit_published_track` because replacement,
    /// disconnect or negotiation rollback can make the descriptor stale.
    pub(in crate::runtime::channel) fn validate_publish_descriptor(
        &self,
        session_id: &SessionId,
        publisher_connection_id: ConnectionId,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
    ) -> Option<ValidatedPublishDescriptor> {
        let Some(session) = self.sessions.get(session_id) else {
            warn!(
                ?session_id,
                publisher_connection_id = ?publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the session is missing from channel state"
            );
            return None;
        };
        if session.connection_id != publisher_connection_id {
            warn!(
                ?session_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?session.connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the connection is stale"
            );
            return None;
        }
        if !session.negotiation.can_publish() {
            warn!(
                ?session_id,
                publisher_connection_id = ?publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the session is not publish-ready"
            );
            return None;
        }
        Some(ValidatedPublishDescriptor {
            owner_session_id: session_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_type,
            media_kind,
        })
    }

    /// Commits a negotiated publish after transport setup already succeeded.
    ///
    /// The commit re-checks ownership and publish readiness because the
    /// transport step happen outside the lock and stale callbacks are normal
    /// during replacement or disconnect. On success this install every
    /// producer-facing index in one place, including the `TransportMediaId`
    /// ownership index used by room policy and diagnostics.
    pub(in crate::runtime::channel) fn commit_published_track(
        &mut self,
        pending: PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(ProducerRuntimeId, Vec<PendingConsumerBootstrapTarget>)> {
        let source_key = self.validate_publish_commit(&pending, transport_media_id)?;
        let source_descriptor = self
            .source_descriptor_for_publish(&pending, transport_media_id)
            .map_err(|error| {
                error!(
                    session_id = ?pending.owner_session_id,
                    owner_connection_id = ?pending.owner_connection_id,
                    ?pending.stream_type,
                    ?transport_media_id,
                    ?error,
                    "failed to build source descriptor for negotiated publish"
                );
            })
            .ok()?;
        let source_encoding_ids = source_descriptor
            .encodings()
            .map(SourceEncodingDescriptor::encoding_id)
            .collect::<Vec<_>>();
        let producer_id = ProducerRuntimeId::allocate(&mut self.next_producer_id);
        let routed_producer_id = self.add_routed_producer_for_publish(&pending)?;
        let owner_session_id = pending.owner_session_id.clone();
        let owner_connection_id = pending.owner_connection_id;
        let stream_type = pending.stream_type;
        let media_kind = pending.media_kind;
        let source_id = source_descriptor.source_id();

        self.install_published_source(PublishedSourceInstall {
            source_key,
            source_descriptor,
            source_encoding_ids,
            producer_id,
            routed_producer_id,
            pending,
            transport_media_id,
        });
        let consumer_snapshot = ConsumerBootstrapProducerSnapshot::pending(
            source_id,
            owner_session_id,
            owner_connection_id,
            producer_id,
            stream_type,
            media_kind,
            transport_media_id,
        );
        let consumer_targets = self.publish_consumer_targets(&consumer_snapshot);
        Some((producer_id, consumer_targets))
    }

    fn validate_publish_commit(
        &self,
        pending: &PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<SourceKey> {
        let Some(session) = self.sessions.get(&pending.owner_session_id) else {
            warn!(
                session_id = ?pending.owner_session_id,
                owner_connection_id = ?pending.owner_connection_id,
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the session is missing from channel state"
            );
            return None;
        };
        if session.connection_id != pending.owner_connection_id
            || !session.negotiation.can_publish()
        {
            warn!(
                session_id = ?pending.owner_session_id,
                owner_connection_id = ?pending.owner_connection_id,
                current_connection_id = ?session.connection_id,
                publish_ready = session.negotiation.can_publish(),
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the session state changed before commit"
            );
            return None;
        }
        let source_key = SourceKey::new(&pending.owner_session_id, pending.stream_type);
        if self.source_ids_by_owner_stream.contains_key(&source_key) {
            warn!(
                session_id = ?pending.owner_session_id,
                owner_connection_id = ?pending.owner_connection_id,
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because a source already exists for this compatibility stream"
            );
            return None;
        }
        Some(source_key)
    }

    fn add_routed_producer_for_publish(
        &mut self,
        pending: &PreparedPublishedTrack,
    ) -> Option<RoutedProducerId> {
        match self.topology.add_producer(
            &pending.owner_session_id,
            pending.media_kind,
            to_router_stream_type(pending.stream_type),
        ) {
            Ok(producer_id) => Some(producer_id),
            Err(error) => {
                error!(
                    session_id = ?pending.owner_session_id,
                    ?error,
                    "failed to mirror publish request into channel router producer state"
                );
                None
            }
        }
    }

    fn install_published_source(&mut self, install: PublishedSourceInstall) {
        let PublishedSourceInstall {
            source_key,
            source_descriptor,
            source_encoding_ids,
            producer_id,
            routed_producer_id,
            pending,
            transport_media_id,
        } = install;
        let source_id = source_descriptor.source_id();
        self.producers.insert(
            producer_id,
            PublishedProducer {
                source_id,
                owner_session_id: pending.owner_session_id.clone(),
                owner_connection_id: pending.owner_connection_id,
                stream_type: pending.stream_type,
                media_kind: pending.media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                source_packet_selection: None,
                active: true,
            },
        );
        self.sources.insert(source_id, source_descriptor);
        self.source_ids_by_owner_stream
            .insert(source_key, source_id);
        self.producer_id_by_source_id.insert(source_id, producer_id);
        self.source_transport_media_index.insert(
            transport_media_id,
            SourceTransportMediaIndexEntry::new(
                source_id,
                source_encoding_ids,
                pending.owner_session_id.clone(),
                pending.owner_connection_id,
                pending.stream_type,
            ),
        );
    }

    fn source_descriptor_for_publish(
        &mut self,
        pending: &PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::allocate(&mut self.next_source_id);
        let encodings = pending
            .consumable_rtp_parameters
            .encodings()
            .map(|binding| {
                let encoding_id = SourceEncodingId::allocate(&mut self.next_source_encoding_id);
                SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                    encoding_id,
                    source_id,
                    rid: binding.rid().map(Rid::new),
                    primary_ssrc: binding.ssrc().map(Ssrc::new),
                    repair_ssrc: None,
                    max_bitrate: binding.max_bitrate(),
                    negotiated_format: negotiated_format_for_binding(
                        &pending.consumable_rtp_parameters,
                        binding.payload_type(),
                    ),
                    transport_binding: Some(SourceTransportBinding::new(transport_media_id)),
                })
            })
            .collect::<Vec<_>>();
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(
                pending.owner_session_id.clone(),
                pending.owner_connection_id,
            ),
            stream_type: pending.stream_type,
            media_kind: pending.media_kind,
            mid: pending.consumable_rtp_parameters.mid().map(Mid::new),
            encodings,
        })
    }

    /// Plans bootstrap work for peers that should consume a newly published track
    ///
    /// This only computes the next transport-facing work. It does not mutate
    /// consumer state yet, so callers can still stop out cleanly if the later
    /// transport bootstrap fails.
    pub(in crate::runtime::channel) fn publish_consumer_targets(
        &self,
        producer: &ConsumerBootstrapProducerSnapshot,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.sessions
            .iter()
            .filter_map(|(peer_session_id, peer_session)| {
                if peer_session_id == producer.owner_session_id()
                    || !peer_session.negotiation.can_consume()
                {
                    return None;
                }
                if self.consumer_bootstrap_exists(&ConsumerKey::new(
                    peer_session_id,
                    producer.source_id(),
                )) {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget::new(
                    peer_session_id.clone(),
                    peer_session.connection_id,
                    (*producer).clone(),
                ))
            })
            .collect()
    }

    #[must_use]
    pub(in crate::runtime::channel) fn producer_stream_type_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.source_transport_media_index
            .get(&transport_media_id)
            .map(SourceTransportMediaIndexEntry::stream_type)
    }

    #[must_use]
    pub(in crate::runtime::channel) fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.source_transport_media_index.get(&transport_media_id)
    }

    #[must_use]
    pub(in crate::runtime::channel) fn producer_route_target(
        &self,
        owner_session_id: &SessionId,
        owner_connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let producer_id =
            self.producer_id_for_source_key(&SourceKey::new(owner_session_id, stream_type))?;
        let producer = self.producers.get(&producer_id)?;
        if producer.owner_connection_id != owner_connection_id {
            return None;
        }
        let transport_media_id = producer.transport_media_id?;
        Some(ProducerRouteTarget {
            source_id: producer.source_id,
            producer_id,
            owner_connection_id: producer.owner_connection_id,
            routed_producer_id: producer.routed_producer_id,
            transport_media_id,
        })
    }

    pub(in crate::runtime::channel) fn producer_route_target_for_session(
        &self,
        session_id: &SessionId,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let connection_id = self.session_connection_id(session_id)?;
        self.producer_route_target(session_id, connection_id, stream_type)
    }

    /// Lists the transport media that must be removed for an explicit unpublish.
    ///
    /// The producer media go first, then every dependent consumer media for
    /// the same source stream. Callers use this before mutating state so
    /// cleanup can still target the live transport ids that matched the
    /// current produceur ownership.
    pub(in crate::runtime::channel) fn unpublish_transport_removals(
        &self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<Vec<TransportMediaRemoval>> {
        let producer_target = self.producer_route_target(session_id, connection_id, stream_type)?;
        let mut transport_removals = vec![TransportMediaRemoval {
            session: session_id.clone(),
            connection: connection_id,
            transport_media: producer_target.transport_media_id,
        }];
        transport_removals.extend(self.consumer_index.iter().filter_map(
            |(key, consumer_state)| {
                if key.source_id != producer_target.source_id {
                    return None;
                }
                Some(TransportMediaRemoval {
                    session: key.consumer_session_id.clone(),
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            },
        ));
        Some(transport_removals)
    }

    /// Removes one published stream and its dependent consumer bookkeeping.
    ///
    /// This is the explicit unpublish state transition. It removes the router
    /// producer, drops the producer and pending-consumer indexes for that
    /// stream and clears the `TransportMediaId` ownership entry in the same
    /// step so later diagnostics or room policy updates cannot resolve stale
    /// ownership
    pub(in crate::runtime::channel) fn unpublish_track(
        &mut self,
        session_id: &SessionId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<UnpublishTrackOutcome> {
        let producer_target = self.producer_route_target(session_id, connection_id, stream_type)?;
        if self
            .topology
            .remove_producer(producer_target.routed_producer_id)
            .is_err()
        {
            error!(
                ?session_id,
                ?stream_type,
                "failed to remove published track from channel router"
            );
            return None;
        }
        self.remove_source_registry_entry(producer_target.source_id)?;
        let session_info_snapshot = match stream_type {
            StreamType::Camera | StreamType::Screen => {
                Some(BTreeMap::from([self.session_info_snapshot(session_id)?]))
            }
            StreamType::Audio => None,
        };
        Some(UnpublishTrackOutcome {
            recipients: self
                .sessions
                .values()
                .map(|session| session.sender.clone())
                .collect(),
            session_info_snapshot,
        })
    }

    /// Applies a publish-side active or paused transition for an existing producer.
    ///
    /// The `ProducerRouteTarget` comes from a fresh state lookup and is used as
    /// a stale-callback guard. If any ownership field drifted since that lookup
    /// the update becomes a no-op instead of mutating the wrong replacement
    /// session.
    pub(in crate::runtime::channel) fn apply_producer_activity(
        &mut self,
        session_id: &SessionId,
        producer_target: &ProducerRouteTarget,
        stream_type: StreamType,
        active: bool,
    ) -> Option<ProducerActivityOutcome> {
        let current_connection_id = self.session_connection_id(session_id);
        let producer = self.producers.get(&producer_target.producer_id)?;
        if producer.source_id != producer_target.source_id
            || producer.owner_connection_id != producer_target.owner_connection_id
            || Some(producer.owner_connection_id) != current_connection_id
            || producer.routed_producer_id != producer_target.routed_producer_id
            || producer.transport_media_id != Some(producer_target.transport_media_id)
        {
            return None;
        }
        let paused = !active;
        if self
            .topology
            .set_producer_paused(producer_target.routed_producer_id, paused)
            .is_err()
        {
            error!(
                ?session_id,
                ?stream_type,
                "failed to set producer pause state in channel router"
            );
            return None;
        }
        {
            let producer = self.producers.get_mut(&producer_target.producer_id)?;
            producer.active = active;
        }
        let snapshot = BTreeMap::from([self.session_info_snapshot(session_id)?]);
        Some(ProducerActivityOutcome {
            transport_media_id: producer_target.transport_media_id,
            active,
            fanout: self.fanout_all(&ChannelEventMessage::SessionInfoChanged(snapshot)),
        })
    }
}

impl ValidatedPublishDescriptor {
    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub(in crate::runtime::channel) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::channel) fn owner_session_id(&self) -> &SessionId {
        &self.owner_session_id
    }

    /// Freezes the validated publish inputs together with router-ready RTP data.
    pub(in crate::runtime::channel) fn into_prepared_track(
        self,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> PreparedPublishedTrack {
        PreparedPublishedTrack {
            owner_session_id: self.owner_session_id,
            owner_connection_id: self.owner_connection_id,
            stream_type: self.stream_type,
            media_kind: self.media_kind,
            consumable_rtp_parameters,
        }
    }
}

impl ProducerRouteTarget {
    pub(in crate::runtime::channel) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }
}

impl UnpublishTrackOutcome {
    /// Emits the unpublish side effects after state cleanup already landed
    ///
    /// Recipients always get the track binding removal. Camera and screen
    /// unpublish also fan out a session-info snapshot so clients can clear the
    /// visible publication flag without rebuilding their own projection.
    pub(in crate::runtime::channel) fn emit(self, session_id: &SessionId, stream_type: StreamType) {
        let track_update = SessionOutbound::TrackBindingUpdate(TrackBindingUpdate {
            session_id: session_id.clone(),
            stream_type,
            active: None,
        });
        for recipient in self.recipients {
            let _ = recipient.send(track_update.clone());
            if let Some(snapshot) = self.session_info_snapshot.as_ref() {
                let _ = recipient.send(SessionOutbound::Message(
                    ChannelEventMessage::SessionInfoChanged(snapshot.clone()),
                ));
            }
        }
    }
}

fn negotiated_format_for_binding(
    parameters: &RouterRtpParameters,
    payload_type: Option<u8>,
) -> Option<MediaFormat> {
    if let Some(payload_type) = payload_type
        && let Some(format) = parameters
            .formats()
            .find(|format| format.payload_type() == payload_type)
    {
        return Some(format.clone());
    }
    parameters
        .formats()
        .find(|format| !format.codec().is_rtx())
        .or_else(|| parameters.formats().next())
        .cloned()
}
