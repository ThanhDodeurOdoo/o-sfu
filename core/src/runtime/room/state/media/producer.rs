//! Producer-side room state transitions.
//!
//! the pure state work around publishing, unpublishing and producer activity changes
//! after transport negotiation is done elsewhere.
//! The main rule here is that room state only commits a producer once the
//! caller has a validated user and a real transport media id to attach.

use std::collections::BTreeMap;

use o_sfu_router::{
    MediaFormat, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, Mid,
    ProducerRouteState, Rid, Ssrc,
};
use tracing::{error, warn};

use super::{
    super::{
        super::{
            RoomEventMessage, TrackBindingUpdate, UserOutbound,
            outbound::{MessageFanout, OutboundSender},
            topology::RoutedProducerId,
        },
        ids::ProducerRuntimeId,
        shared::{
            ConsumerKey, PublishedProducer, RoomState, SourceKey, SourceTransportMediaIndexEntry,
            TransportMediaRemoval,
        },
    },
    subscription::{ConsumerBootstrapProducerSnapshot, PendingConsumerBootstrapTarget},
};
use crate::runtime::{
    ConnectionId, StreamType, UserId, UserInfo,
    source_model::{
        PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
        PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
        SourceEncodingId, SourceModelError, UploadLayerPolicyRole,
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
/// Callers resolve this from current room state right before mutating a
/// producer. This lets stale replacement callbacks be rejected
/// without guessing which layer drifted.
pub(in crate::runtime::room) struct ProducerRouteTarget {
    source_id: PublishedSourceId,
    producer_id: ProducerRuntimeId,
    owner_connection_id: ConnectionId,
    routed_producer_id: RoutedProducerId,
    transport_media_id: TransportMediaId,
}

#[derive(Debug, Clone)]
/// Publish request that passed the current user-level checks.
///
/// This exists so transport work can happen after validation without carrying
/// a mutable room borrow.
/// It only proves that the user was publish-ready at validation time.
/// Commit must re-check the same ownership and readiness before state changes land.
pub(in crate::runtime::room) struct ValidatedPublishDescriptor {
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_type: StreamType,
    media_kind: RouterMediaKind,
}

#[derive(Debug, Clone)]
/// Publish payload that is ready to be committed into room state.
///
/// This is the last pure state input before a producer becomes live. The
/// transport layer already allocated the media handle and the caller already
/// derived consumable router parameters, so commit can stay a small
/// all-or-nothing state transition.
pub(in crate::runtime::room) struct PreparedPublishedTrack {
    owner_user_id: UserId,
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
/// The room commits router route state first and only then stages the
/// outward fanout. That keeps user info updates aligned with the lasting
/// router state instead of reporting a producer activity change that never
/// stuck.
pub(in crate::runtime::room) struct ProducerActivityOutcome {
    pub(in crate::runtime::room) transport_media_id: TransportMediaId,
    pub(in crate::runtime::room) active: bool,
    pub(in crate::runtime::room) fanout: MessageFanout,
}

#[derive(Debug)]
/// Deferred side effects for explicit unpublish.
///
/// The state transition removes the producer and all dependent consumer
/// bookkeeping first. Emitting the track removal and optional user-info
/// update stays outside the state lock.
pub(in crate::runtime::room) struct UnpublishTrackOutcome {
    recipients: Vec<OutboundSender>,
    user_info_snapshot: Option<BTreeMap<UserId, UserInfo>>,
}

impl RoomState {
    /// Validates that a user may start a negotiated publish right now.
    ///
    /// This is the pure state gate in front of the staged publish flow. The
    /// caller may use the returned descriptor to drive transport work, but it
    /// must still commit through `commit_published_track` because replacement,
    /// disconnect or negotiation rollback can make the descriptor stale.
    pub(in crate::runtime::room) fn validate_publish_descriptor(
        &self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        stream_type: StreamType,
        media_kind: RouterMediaKind,
    ) -> Option<ValidatedPublishDescriptor> {
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the user is missing from room state"
            );
            return None;
        };
        if user.connection_id != publisher_connection_id {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?user.connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the connection is stale"
            );
            return None;
        }
        if !user.negotiation.can_publish() {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                ?stream_type,
                "cannot prepare negotiated publish because the user is not publish-ready"
            );
            return None;
        }
        Some(ValidatedPublishDescriptor {
            owner_user_id: user_id.clone(),
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
    pub(in crate::runtime::room) fn commit_published_track(
        &mut self,
        pending: PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(ProducerRuntimeId, Vec<PendingConsumerBootstrapTarget>)> {
        let source_key = self.validate_publish_commit(&pending, transport_media_id)?;
        let source_descriptor = self
            .source_descriptor_for_publish(&pending)
            .map_err(|error| {
                error!(
                    user_id = ?pending.owner_user_id,
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
        let owner_user_id = pending.owner_user_id.clone();
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
            owner_user_id,
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
        let Some(user) = self.users.get(&pending.owner_user_id) else {
            warn!(
                user_id = ?pending.owner_user_id,
                owner_connection_id = ?pending.owner_connection_id,
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the user is missing from room state"
            );
            return None;
        };
        if user.connection_id != pending.owner_connection_id || !user.negotiation.can_publish() {
            warn!(
                user_id = ?pending.owner_user_id,
                owner_connection_id = ?pending.owner_connection_id,
                current_connection_id = ?user.connection_id,
                publish_ready = user.negotiation.can_publish(),
                ?pending.stream_type,
                ?transport_media_id,
                "cannot commit negotiated publish because the user state changed before commit"
            );
            return None;
        }
        let source_key = SourceKey::new(&pending.owner_user_id, pending.stream_type);
        if self.source_ids_by_owner_stream.contains_key(&source_key) {
            warn!(
                user_id = ?pending.owner_user_id,
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
        match self
            .topology
            .add_producer(&pending.owner_user_id, pending.media_kind)
        {
            Ok(producer_id) => Some(producer_id),
            Err(error) => {
                error!(
                    user_id = ?pending.owner_user_id,
                    ?error,
                    "failed to mirror publish request into room router producer state"
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
                owner_user_id: pending.owner_user_id.clone(),
                owner_connection_id: pending.owner_connection_id,
                stream_type: pending.stream_type,
                media_kind: pending.media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
        );
        self.sources.insert(source_id, source_descriptor);
        self.source_ids_by_owner_stream
            .insert(source_key, source_id);
        self.producer_id_by_source_id.insert(source_id, producer_id);
        self.register_source_owner(&pending.owner_user_id, source_id);
        self.register_producer_owner(&pending.owner_user_id, producer_id);
        self.source_transport_media_index.insert(
            transport_media_id,
            SourceTransportMediaIndexEntry::new(
                source_id,
                source_encoding_ids,
                pending.owner_user_id.clone(),
                pending.owner_connection_id,
                pending.stream_type,
            ),
        );
    }

    fn source_descriptor_for_publish(
        &mut self,
        pending: &PreparedPublishedTrack,
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::allocate(&mut self.next_source_id);
        let encodings = pending
            .consumable_rtp_parameters
            .encodings()
            .map(|binding| {
                let encoding_id = SourceEncodingId::allocate(&mut self.next_source_encoding_id);
                let upload_profile = upload_profile_for_rid(binding.rid());
                SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                    encoding_id,
                    source_id,
                    rid: binding.rid().map(Rid::new),
                    primary_ssrc: binding.ssrc().map(Ssrc::new),
                    repair_ssrc: None,
                    max_bitrate: binding.max_bitrate(),
                    resolution_scale: upload_profile.map(|profile| profile.resolution_scale),
                    max_framerate: upload_profile.and_then(|profile| profile.max_framerate),
                    policy_role: upload_profile.map(|profile| profile.policy_role),
                    max_temporal_layer_id: None,
                    negotiated_format: negotiated_format_for_binding(
                        &pending.consumable_rtp_parameters,
                        binding.payload_type(),
                    ),
                })
            })
            .collect::<Vec<_>>();
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(pending.owner_user_id.clone()),
            stream_type: pending.stream_type,
            media_kind: pending.media_kind,
            mid: pending.consumable_rtp_parameters.mid().map(Mid::new),
            encodings,
        })
    }

    /// Plans bootstrap work for users that should consume a newly published track.
    ///
    /// This only computes the next transport-facing work. It does not mutate
    /// consumer state yet, so callers can still stop out cleanly if the later
    /// transport bootstrap fails.
    pub(in crate::runtime::room) fn publish_consumer_targets(
        &self,
        producer: &ConsumerBootstrapProducerSnapshot,
    ) -> Vec<PendingConsumerBootstrapTarget> {
        self.users
            .iter()
            .filter_map(|(remote_user_id, remote_user)| {
                if remote_user_id == producer.owner_user_id()
                    || !remote_user.negotiation.can_consume()
                {
                    return None;
                }
                if self.consumer_bootstrap_exists(&ConsumerKey::new(
                    remote_user_id,
                    producer.source_id(),
                )) {
                    return None;
                }
                Some(PendingConsumerBootstrapTarget::new(
                    remote_user_id.clone(),
                    remote_user.connection_id,
                    (*producer).clone(),
                ))
            })
            .collect()
    }

    #[must_use]
    pub(in crate::runtime::room) fn producer_stream_type_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<StreamType> {
        self.source_transport_media_index
            .get(&transport_media_id)
            .map(SourceTransportMediaIndexEntry::stream_type)
    }

    #[must_use]
    pub(in crate::runtime::room) fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.source_transport_media_index.get(&transport_media_id)
    }

    #[must_use]
    pub(in crate::runtime::room) fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let producer_id =
            self.producer_id_for_source_key(&SourceKey::new(owner_user_id, stream_type))?;
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

    pub(in crate::runtime::room) fn producer_route_target_for_user(
        &self,
        user_id: &UserId,
        stream_type: StreamType,
    ) -> Option<ProducerRouteTarget> {
        let connection_id = self.user_connection_id(user_id)?;
        self.producer_route_target(user_id, connection_id, stream_type)
    }

    /// Lists the transport media that must be removed for an explicit unpublish.
    ///
    /// The producer media go first, then every dependent consumer media for
    /// the same source stream. Callers use this before mutating state so
    /// cleanup can still target the live transport ids that matched the
    /// current producer ownership.
    pub(in crate::runtime::room) fn unpublish_transport_removals(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<Vec<TransportMediaRemoval>> {
        let producer_target = self.producer_route_target(user_id, connection_id, stream_type)?;
        let mut transport_removals = vec![TransportMediaRemoval {
            user: user_id.clone(),
            connection: connection_id,
            transport_media: producer_target.transport_media_id,
        }];
        let consumer_removals = self
            .consumer_keys_for_source(producer_target.source_id)
            .into_iter()
            .filter_map(|key| {
                let consumer_state = self.consumer_index.get(&key)?;
                Some(TransportMediaRemoval {
                    user: key.consumer_user_id,
                    connection: consumer_state.consumer_connection_id,
                    transport_media: consumer_state.consumer_media,
                })
            });
        transport_removals.extend(consumer_removals);
        Some(transport_removals)
    }

    /// Removes one published stream and its dependent consumer bookkeeping.
    ///
    /// This is the explicit unpublish state transition. It removes the router
    /// producer, drops the producer and pending-consumer indexes for that
    /// stream and clears the `TransportMediaId` ownership entry in the same
    /// step so later diagnostics or room policy updates cannot resolve stale
    /// ownership
    pub(in crate::runtime::room) fn unpublish_track(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: StreamType,
    ) -> Option<UnpublishTrackOutcome> {
        let producer_target = self.producer_route_target(user_id, connection_id, stream_type)?;
        if self
            .topology
            .remove_producer(producer_target.routed_producer_id)
            .is_err()
        {
            error!(
                ?user_id,
                ?stream_type,
                "failed to remove published track from room router"
            );
            return None;
        }
        self.remove_source_registry_entry(producer_target.source_id)?;
        let user_info_snapshot = match stream_type {
            StreamType::Camera | StreamType::Screen => {
                Some(BTreeMap::from([self.user_info_snapshot(user_id)?]))
            }
            StreamType::Audio => None,
        };
        Some(UnpublishTrackOutcome {
            recipients: self
                .users
                .values()
                .map(|user| user.sender.clone())
                .collect(),
            user_info_snapshot,
        })
    }

    /// Applies a publish-side active or paused transition for an existing producer.
    ///
    /// The `ProducerRouteTarget` comes from a fresh state lookup and is used as
    /// a stale-callback guard. If any ownership field drifted since that lookup
    /// the update becomes a no-op instead of mutating the wrong replacement
    /// user.
    ///
    /// The pure router is updated before the room-level producer flag so a
    /// failed router mutation cannot leave outbound user-info fanout ahead of
    /// authoritative route state.
    pub(in crate::runtime::room) fn apply_producer_activity(
        &mut self,
        user_id: &UserId,
        producer_target: &ProducerRouteTarget,
        stream_type: StreamType,
        active: bool,
    ) -> Option<ProducerActivityOutcome> {
        let current_connection_id = self.user_connection_id(user_id);
        let producer = self.producers.get(&producer_target.producer_id)?;
        if producer.source_id != producer_target.source_id
            || producer.owner_connection_id != producer_target.owner_connection_id
            || Some(producer.owner_connection_id) != current_connection_id
            || producer.routed_producer_id != producer_target.routed_producer_id
            || producer.transport_media_id != Some(producer_target.transport_media_id)
        {
            return None;
        }
        let route_state = if active {
            ProducerRouteState::Active
        } else {
            ProducerRouteState::Paused
        };
        if self
            .topology
            .set_producer_route_state(producer_target.routed_producer_id, route_state)
            .is_err()
        {
            error!(
                ?user_id,
                ?stream_type,
                "failed to set producer pause state in room router"
            );
            return None;
        }
        {
            let producer = self.producers.get_mut(&producer_target.producer_id)?;
            producer.active = active;
        }
        let snapshot = BTreeMap::from([self.user_info_snapshot(user_id)?]);
        Some(ProducerActivityOutcome {
            transport_media_id: producer_target.transport_media_id,
            active,
            fanout: self.fanout_all(&RoomEventMessage::UserInfoChanged(snapshot)),
        })
    }
}

impl ValidatedPublishDescriptor {
    pub(in crate::runtime::room) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub(in crate::runtime::room) const fn stream_type(&self) -> StreamType {
        self.stream_type
    }

    pub(in crate::runtime::room) fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    /// Freezes the validated publish inputs together with router-ready RTP data.
    pub(in crate::runtime::room) fn into_prepared_track(
        self,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> PreparedPublishedTrack {
        PreparedPublishedTrack {
            owner_user_id: self.owner_user_id,
            owner_connection_id: self.owner_connection_id,
            stream_type: self.stream_type,
            media_kind: self.media_kind,
            consumable_rtp_parameters,
        }
    }
}

impl ProducerRouteTarget {
    pub(in crate::runtime::room) const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }
}

impl UnpublishTrackOutcome {
    /// Emits the unpublish side effects after state cleanup already landed
    ///
    /// Recipients always get the track binding removal. Camera and screen
    /// unpublish also fan out a user-info snapshot so clients can clear the
    /// visible publication flag without rebuilding their own projection.
    pub(in crate::runtime::room) fn emit(self, user_id: &UserId, stream_type: StreamType) {
        let track_update = UserOutbound::TrackBindingUpdate(TrackBindingUpdate {
            user_id: user_id.clone(),
            stream_type,
            active: None,
        });
        for recipient in self.recipients {
            let _ = recipient.send(track_update.clone());
            if let Some(snapshot) = self.user_info_snapshot.as_ref() {
                let _ = recipient.send(UserOutbound::Message(RoomEventMessage::UserInfoChanged(
                    snapshot.clone(),
                )));
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UploadLayerProfile {
    resolution_scale: u16,
    max_framerate: Option<u16>,
    policy_role: UploadLayerPolicyRole,
}

fn upload_profile_for_rid(rid: Option<&str>) -> Option<UploadLayerProfile> {
    match rid {
        Some("lo") => Some(UploadLayerProfile {
            resolution_scale: 2,
            max_framerate: None,
            policy_role: UploadLayerPolicyRole::Thumbnail,
        }),
        Some("hi") => Some(UploadLayerProfile {
            resolution_scale: 1,
            max_framerate: None,
            policy_role: UploadLayerPolicyRole::Featured,
        }),
        _ => None,
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
