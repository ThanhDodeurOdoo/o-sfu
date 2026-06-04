//! Producer-side room state transitions.
//!
//! This file owns the pure state work around publish, unpublish and producer
//! activity changes after transport negotiation has happened elsewhere. The
//! main rule is that room state commits a producer only after the caller has a
//! current user connection, a source publish intent and a real transport
//! media id to attach.
//!
//! Source policy is copied from [`SourcePublishIntent`] into the committed
//! source descriptor. The policy then drives layout and BWE decisions without
//! requiring this module to know product stream names.

use o_sfu_router::{
    MediaFormat, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters, Mid,
    ProducerRouteState, Rid, Ssrc,
};
use tracing::{error, warn};

use super::{
    super::{
        TrackBindingUpdate, outbound::OutboundSender, routing::RoutedProducerId, state::RoomState,
    },
    ConsumerKey, ProducerRouteTarget, ProducerRuntimeId, PublishedProducer, PublishedSourceInstall,
    SourceTransportMediaIndexEntry, TransportMediaRemoval,
    route_graph::RelayRouteEffect,
    subscription::{ConsumerBootstrapProducerSnapshot, PendingConsumerBootstrapTarget},
};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, UserId,
        media_transport::{SessionUploadEncoding, TransportMediaId},
        source_model::{
            PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
            PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
            SourceEncodingId, SourceModelError, SourcePolicy, SourcePublishIntent,
            UploadLayerPolicyRole, UserStreamId,
        },
    },
};

#[derive(Debug, Clone)]
pub(in crate::engine::room) struct ValidatedPublishDescriptor {
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_id: UserStreamId,
    media_kind: RouterMediaKind,
    policy: SourcePolicy,
}

#[derive(Debug, Clone)]
pub(in crate::engine::room) struct PreparedPublishedTrack {
    owner_user_id: UserId,
    owner_connection_id: ConnectionId,
    stream_id: UserStreamId,
    media_kind: RouterMediaKind,
    policy: SourcePolicy,
    consumable_rtp_parameters: RouterRtpParameters,
    upload_encodings: Vec<SessionUploadEncoding>,
}

#[derive(Debug)]
pub(in crate::engine::room) struct ProducerActivityOutcome {
    recipients: Vec<OutboundSender>,
    update: TrackBindingUpdate,
}

#[derive(Debug)]
pub(in crate::engine::room) struct UnpublishTrackOutcome {
    recipients: Vec<OutboundSender>,
    update: TrackBindingUpdate,
    relay_effects: Vec<RelayRouteEffect>,
    transport_removals: Vec<TransportMediaRemoval>,
}

impl RoomState {
    pub fn validate_publish_descriptor(
        &self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        intent: &SourcePublishIntent,
    ) -> Option<ValidatedPublishDescriptor> {
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the user is missing from room state"
            );
            return None;
        };
        if user.connection_id != publisher_connection_id {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?user.connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the connection is stale"
            );
            return None;
        }
        if !user.negotiation.can_publish() {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot prepare negotiated publish because the user is not publish-ready"
            );
            return None;
        }
        Some(ValidatedPublishDescriptor {
            owner_user_id: user_id.clone(),
            owner_connection_id: publisher_connection_id,
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
        })
    }

    pub fn commit_publish_reservation(
        &mut self,
        pending: PreparedPublishedTrack,
        transport_media_id: TransportMediaId,
    ) -> Option<(ProducerRuntimeId, Vec<PendingConsumerBootstrapTarget>)> {
        self.validate_publish_commit(&pending, transport_media_id)?;
        let source_descriptor = self
            .source_descriptor_for_publish(&pending)
            .map_err(|error| {
                error!(
                    user_id = ?pending.owner_user_id,
                    owner_connection_id = ?pending.owner_connection_id,
                    stream_id = %pending.stream_id,
                    ?transport_media_id,
                    ?error,
                    "failed to build source descriptor for negotiated publish"
                );
            })
            .ok()?;
        let producer_id = ProducerRuntimeId::allocate(&mut self.next_producer_id);
        let routed_producer_id = self.add_routed_producer_for_publish(&pending)?;
        let owner_user_id = pending.owner_user_id.clone();
        let owner_connection_id = pending.owner_connection_id;
        let stream_id = pending.stream_id.clone();
        let media_kind = pending.media_kind;
        let source_id = source_descriptor.source_id();

        self.media.install_source(PublishedSourceInstall {
            source_descriptor,
            producer_id,
            producer: PublishedProducer {
                source_id,
                owner_user_id: owner_user_id.clone(),
                owner_connection_id,
                stream_id: stream_id.clone(),
                media_kind,
                consumable_rtp_parameters: pending.consumable_rtp_parameters,
                routed_producer_id,
                transport_media_id: Some(transport_media_id),
                active: true,
            },
            transport_media_id,
        });
        let consumer_snapshot = ConsumerBootstrapProducerSnapshot::pending(
            source_id,
            owner_user_id,
            owner_connection_id,
            producer_id,
            stream_id,
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
    ) -> Option<()> {
        let Some(user) = self.users.get(&pending.owner_user_id) else {
            warn!(
                user_id = ?pending.owner_user_id,
                owner_connection_id = ?pending.owner_connection_id,
                stream_id = %pending.stream_id,
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
                stream_id = %pending.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because the user state changed before commit"
            );
            return None;
        }
        if self
            .media
            .has_source_for_owner_stream(&pending.owner_user_id, &pending.stream_id)
        {
            warn!(
                user_id = ?pending.owner_user_id,
                owner_connection_id = ?pending.owner_connection_id,
                stream_id = %pending.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because a source already exists for this stream"
            );
            return None;
        }
        Some(())
    }

    fn add_routed_producer_for_publish(
        &mut self,
        pending: &PreparedPublishedTrack,
    ) -> Option<RoutedProducerId> {
        match self
            .routing
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

    fn source_descriptor_for_publish(
        &mut self,
        pending: &PreparedPublishedTrack,
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::allocate(&mut self.next_source_id);
        let encodings = pending
            .consumable_rtp_parameters
            .bindings()
            .map(|binding| {
                let encoding_id = SourceEncodingId::allocate(&mut self.next_source_encoding_id);
                let upload_profile =
                    upload_profile_for_rid(&pending.upload_encodings, binding.rid());
                let policy_role =
                    upload_profile.map(|profile| upload_layer_policy_role_for_rank(profile.rank));
                SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
                    encoding_id,
                    source_id,
                    rid: binding.rid().map(Rid::new),
                    primary_ssrc: binding.ssrc().map(Ssrc::new),
                    repair_ssrc: None,
                    max_bitrate: binding
                        .max_bitrate()
                        .map(Bitrate::from_bps)
                        .or_else(|| upload_profile.and_then(MatchedUploadEncoding::max_bitrate)),
                    resolution_scale: upload_profile
                        .and_then(MatchedUploadEncoding::resolution_scale),
                    max_framerate: upload_profile.and_then(MatchedUploadEncoding::max_framerate),
                    policy_role,
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
            stream_id: pending.stream_id.clone(),
            media_kind: pending.media_kind,
            policy: pending.policy,
            mid: pending.consumable_rtp_parameters.mid().map(Mid::new),
            encodings,
        })
    }

    pub fn publish_consumer_targets(
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
                if self.media.consumer_bootstrap_exists(&ConsumerKey::new(
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
    pub fn producer_stream_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserStreamId> {
        self.media
            .producer_stream_id_for_transport_media_id(transport_media_id)
    }

    #[must_use]
    pub fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.media.source_transport_media_entry(transport_media_id)
    }

    #[must_use]
    pub fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        self.media
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
    }

    pub fn producer_route_target_for_user(
        &self,
        user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        let connection_id = self.user_connection_id(user_id)?;
        self.producer_route_target(user_id, connection_id, stream_id)
    }

    pub fn unpublish_track(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<UnpublishTrackOutcome> {
        let producer_target = self.producer_route_target(user_id, connection_id, stream_id)?;
        let transport_removals = self
            .media
            .transport_removals_for_producer_target(user_id, &producer_target);
        let affected_consumers = self
            .media
            .routed_consumer_ids_for_source(producer_target.source_id());
        if self
            .routing
            .remove_producer(producer_target.routed_producer_id(), affected_consumers)
            .is_err()
        {
            error!(
                ?user_id,
                stream_id = %stream_id,
                "repaired published track room state after router producer teardown failed"
            );
        }
        let (_producer, relay_effects) = self.media.remove_source(producer_target.source_id())?;
        Some(UnpublishTrackOutcome {
            recipients: self
                .users
                .values()
                .map(|user| user.sender.clone())
                .collect(),
            update: TrackBindingUpdate {
                user_id: user_id.clone(),
                stream_id: stream_id.clone(),
                active: None,
            },
            relay_effects,
            transport_removals,
        })
    }

    pub fn apply_producer_activity(
        &mut self,
        user_id: &UserId,
        producer_target: &ProducerRouteTarget,
        stream_id: &UserStreamId,
        active: bool,
    ) -> Option<ProducerActivityOutcome> {
        let current_connection_id = self.user_connection_id(user_id);
        let producer = self
            .media
            .producer_for_route_target(producer_target, current_connection_id)?;
        if producer.active == active {
            return None;
        }
        let route_state = if active {
            ProducerRouteState::Active
        } else {
            ProducerRouteState::Paused
        };
        if self
            .routing
            .set_producer_route_state(producer_target.routed_producer_id(), route_state)
            .is_err()
        {
            error!(
                ?user_id,
                stream_id = %stream_id,
                "failed to set producer pause state in room router"
            );
            return None;
        }
        self.media.set_producer_active(producer_target, active);
        Some(ProducerActivityOutcome {
            recipients: self
                .users
                .values()
                .map(|user| user.sender.clone())
                .collect(),
            update: TrackBindingUpdate {
                user_id: user_id.clone(),
                stream_id: stream_id.clone(),
                active: Some(active),
            },
        })
    }
}

impl ValidatedPublishDescriptor {
    pub const fn owner_connection_id(&self) -> ConnectionId {
        self.owner_connection_id
    }

    pub const fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    pub fn owner_user_id(&self) -> &UserId {
        &self.owner_user_id
    }

    #[allow(
        dead_code,
        reason = "room state tests use this helper when upload-profile metadata is irrelevant"
    )]
    pub fn into_prepared_track(
        self,
        consumable_rtp_parameters: RouterRtpParameters,
    ) -> PreparedPublishedTrack {
        self.into_prepared_track_with_upload_encodings(consumable_rtp_parameters, Vec::new())
    }

    pub fn into_prepared_track_with_upload_encodings(
        self,
        consumable_rtp_parameters: RouterRtpParameters,
        upload_encodings: Vec<SessionUploadEncoding>,
    ) -> PreparedPublishedTrack {
        PreparedPublishedTrack {
            owner_user_id: self.owner_user_id,
            owner_connection_id: self.owner_connection_id,
            stream_id: self.stream_id,
            media_kind: self.media_kind,
            policy: self.policy,
            consumable_rtp_parameters,
            upload_encodings,
        }
    }
}

impl ProducerActivityOutcome {
    pub fn into_track_binding_update(self) -> (Vec<OutboundSender>, TrackBindingUpdate) {
        (self.recipients, self.update)
    }
}

impl UnpublishTrackOutcome {
    pub fn into_parts(
        self,
    ) -> (
        Vec<OutboundSender>,
        TrackBindingUpdate,
        Vec<RelayRouteEffect>,
        Vec<TransportMediaRemoval>,
    ) {
        (
            self.recipients,
            self.update,
            self.relay_effects,
            self.transport_removals,
        )
    }
}

fn upload_profile_for_rid<'a>(
    upload_encodings: &'a [SessionUploadEncoding],
    rid: Option<&str>,
) -> Option<MatchedUploadEncoding<'a>> {
    let rid = rid?;
    upload_encodings
        .iter()
        .enumerate()
        .find(|(_rank, encoding)| encoding.rid == rid)
        .map(|(rank, encoding)| MatchedUploadEncoding { rank, encoding })
}

#[derive(Debug, Clone, Copy)]
struct MatchedUploadEncoding<'a> {
    rank: usize,
    encoding: &'a SessionUploadEncoding,
}

impl MatchedUploadEncoding<'_> {
    const fn max_bitrate(self) -> Option<Bitrate> {
        self.encoding.max_bitrate
    }

    const fn resolution_scale(self) -> Option<u16> {
        self.encoding.resolution_scale
    }

    const fn max_framerate(self) -> Option<u16> {
        self.encoding.max_framerate
    }
}

const fn upload_layer_policy_role_for_rank(rank: usize) -> UploadLayerPolicyRole {
    if rank == 0 {
        UploadLayerPolicyRole::Thumbnail
    } else {
        UploadLayerPolicyRole::Featured
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
