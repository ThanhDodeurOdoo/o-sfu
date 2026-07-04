use std::collections::BTreeSet;

use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, Mid, Rid, Ssrc},
};
use tracing::{error, warn};

use super::{
    super::{
        RoomMediaCounts,
        effects::transport::RoomTransportPlan,
        outbound::{OutboundSender, RemoteSourceSnapshot},
        state::{PresenceCommit, RemoteSourceRefresh, RoomState},
    },
    ProducerRouteTarget, ProducerRuntimeId, ReceiverRouteWork, SourceTransportMediaIndexEntry,
    subscription::ReceiverRouteScope,
};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, MediaWorkerId, UserId, UserInfo,
        media_transport::{
            SessionUploadEncoding, TransportMediaId, TransportSessionKey, TransportSourceKey,
        },
        source_model::{
            PublishedSourceDescriptor, PublishedSourceDescriptorParts, PublishedSourceId,
            PublishedSourceOwner, SourceEncodingDescriptor, SourceEncodingDescriptorParts,
            SourceEncodingId, SourceModelError, SourcePolicy, SourcePublishIntent,
            SourceUnpublishIntent, UploadLayerPolicyRole, UserStreamId,
        },
    },
};

#[derive(Debug, Clone)]
pub struct ValidatedPublish {
    pub owner_user_id: UserId,
    pub owner_connection_id: ConnectionId,
    pub session_key: TransportSessionKey,
    pub stream_id: UserStreamId,
    pub media_kind: RouterMediaKind,
    pub policy: SourcePolicy,
    pub presence: Option<UserInfo>,
}

#[derive(Debug)]
pub struct PublishCommit {
    pub user: UserId,
    pub connection: ConnectionId,
    pub worker: MediaWorkerId,
    pub media: TransportMediaId,
    pub publish_before: RoomMediaCounts,
    pub publish_after: RoomMediaCounts,
    pub setup_before: RoomMediaCounts,
    pub setup_after: RoomMediaCounts,
    pub receiver_route_work: ReceiverRouteWork,
    pub presence: Option<PresenceCommit>,
}

#[derive(Debug)]
pub enum PublishIntentPlan {
    Activate(ProducerActivityCommit),
    Noop,
    Queue,
    Stage(ValidatedPublish),
}

#[derive(Debug)]
pub struct ProducerActivityCommit {
    pub source: TransportSourceKey,
    pub stream_id: UserStreamId,
    pub active: bool,
    pub source_snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
    pub presence: Option<PresenceCommit>,
}

#[derive(Debug)]
pub enum ProducerActivityRejection {
    MissingPublication,
    StalePublication,
}

#[derive(Debug)]
pub struct UnpublishCommit {
    pub before: RoomMediaCounts,
    pub after: RoomMediaCounts,
    pub source_snapshots: Vec<(OutboundSender, RemoteSourceSnapshot)>,
    pub transport_plan: RoomTransportPlan,
    pub presence: Option<PresenceCommit>,
}

impl RoomState {
    pub fn apply_publish_intent(
        &mut self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> PublishIntentPlan {
        let Some(user) = self.users.get(user_id) else {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                stream_id = %intent.stream_id(),
                "cannot start publish because the user is missing from room state"
            );
            return PublishIntentPlan::Noop;
        };
        if user.connection_id != publisher_connection_id {
            warn!(
                ?user_id,
                publisher_connection_id = ?publisher_connection_id,
                current_connection_id = ?user.connection_id,
                stream_id = %intent.stream_id(),
                "cannot start publish because the connection is stale"
            );
            return PublishIntentPlan::Noop;
        }
        if self
            .producer_route_target(user_id, publisher_connection_id, intent.stream_id())
            .is_some()
        {
            return self
                .apply_publication_activity(
                    user_id,
                    publisher_connection_id,
                    intent.stream_id(),
                    true,
                    intent.presence(),
                )
                .map_or_else(|_| PublishIntentPlan::Noop, PublishIntentPlan::Activate);
        }
        if !can_stage {
            return PublishIntentPlan::Queue;
        }
        self.validate_publish(user_id, publisher_connection_id, intent)
            .map_or(PublishIntentPlan::Noop, PublishIntentPlan::Stage)
    }

    pub fn validate_publish(
        &self,
        user_id: &UserId,
        publisher_connection_id: ConnectionId,
        intent: &SourcePublishIntent,
    ) -> Option<ValidatedPublish> {
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
        Some(ValidatedPublish {
            owner_user_id: user_id.clone(),
            owner_connection_id: publisher_connection_id,
            session_key: self.transport_user_key(user_id, publisher_connection_id),
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
            presence: intent.presence().cloned(),
        })
    }

    pub fn commit_publish_reservation(
        &mut self,
        publish: ValidatedPublish,
        consumable_rtp_parameters: RouterRtpParameters,
        upload_encodings: &[SessionUploadEncoding],
        transport_media_id: TransportMediaId,
    ) -> Option<PublishCommit> {
        self.validate_publish_commit(&publish, transport_media_id)?;
        let owner_user_id = publish.owner_user_id.clone();
        let owner_connection_id = publish.owner_connection_id;
        let stream_id = publish.stream_id.clone();
        let media_worker_id = publish.session_key.media_worker_id();
        let presence = publish.presence.clone();
        let source_descriptor = self
            .source_descriptor_for_publish(&publish, &consumable_rtp_parameters, upload_encodings)
            .map_err(|error| {
                error!(
                    user_id = ?publish.owner_user_id,
                    owner_connection_id = ?publish.owner_connection_id,
                    stream_id = %publish.stream_id,
                    ?transport_media_id,
                    ?error,
                    "failed to build source descriptor for negotiated publish"
                );
            })
            .ok()?;
        let publish_before = self.media_counts();
        let producer_id = ProducerRuntimeId::allocate(&mut self.next_producer_id);
        if let Err(error) = self.topology.publish_source(
            publish,
            producer_id,
            source_descriptor,
            consumable_rtp_parameters,
            transport_media_id,
        ) {
            error!(
                user_id = ?owner_user_id,
                ?owner_connection_id,
                stream_id = %stream_id,
                ?transport_media_id,
                ?error,
                "failed to mirror publish request into room router producer state"
            );
            return None;
        }
        let publish_after = self.media_counts();
        let setup_before = publish_after;
        let receiver_route_work =
            self.plan_missing_receiver_routes(ReceiverRouteScope::Producer(producer_id));
        let setup_after = self.media_counts();
        let presence = presence.and_then(|info| {
            self.apply_presence_update(
                &owner_user_id,
                owner_connection_id,
                &info,
                RemoteSourceRefresh::Skip,
            )
        });
        Some(PublishCommit {
            user: owner_user_id,
            connection: owner_connection_id,
            worker: media_worker_id,
            media: transport_media_id,
            publish_before,
            publish_after,
            setup_before,
            setup_after,
            receiver_route_work,
            presence,
        })
    }

    pub(in crate::engine::room) fn validate_publish_commit(
        &self,
        publish: &ValidatedPublish,
        transport_media_id: TransportMediaId,
    ) -> Option<()> {
        let Some(user) = self.users.get(&publish.owner_user_id) else {
            warn!(
                user_id = ?publish.owner_user_id,
                owner_connection_id = ?publish.owner_connection_id,
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because the user is missing from room state"
            );
            return None;
        };
        if user.connection_id != publish.owner_connection_id || !user.negotiation.can_publish() {
            warn!(
                user_id = ?publish.owner_user_id,
                owner_connection_id = ?publish.owner_connection_id,
                current_connection_id = ?user.connection_id,
                publish_ready = user.negotiation.can_publish(),
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because the user state changed before commit"
            );
            return None;
        }
        if self
            .topology
            .source_id_for_owner_stream(&publish.owner_user_id, &publish.stream_id)
            .is_some()
        {
            warn!(
                user_id = ?publish.owner_user_id,
                owner_connection_id = ?publish.owner_connection_id,
                stream_id = %publish.stream_id,
                ?transport_media_id,
                "cannot commit negotiated publish because a source already exists for this stream"
            );
            return None;
        }
        Some(())
    }

    fn source_descriptor_for_publish(
        &mut self,
        publish: &ValidatedPublish,
        consumable_rtp_parameters: &RouterRtpParameters,
        upload_encodings: &[SessionUploadEncoding],
    ) -> Result<PublishedSourceDescriptor, SourceModelError> {
        let source_id = PublishedSourceId::allocate(&mut self.next_source_id);
        let encodings = consumable_rtp_parameters
            .bindings()
            .map(|binding| {
                let encoding_id = SourceEncodingId::allocate(&mut self.next_source_encoding_id);
                let upload_profile = upload_profile_for_rid(upload_encodings, binding.rid());
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
                    negotiated_format: negotiated_format_for_binding(
                        consumable_rtp_parameters,
                        binding.payload_type(),
                    ),
                })
            })
            .collect::<Vec<_>>();
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(publish.owner_user_id.clone()),
            stream_id: publish.stream_id.clone(),
            media_kind: publish.media_kind,
            policy: publish.policy,
            mid: consumable_rtp_parameters.mid().map(Mid::new),
            encodings,
        })
    }

    #[must_use]
    pub fn producer_stream_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<UserStreamId> {
        self.source_transport_media_entry(transport_media_id)
            .map(|entry| entry.stream.clone())
    }

    #[must_use]
    pub fn source_transport_media_entry(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&SourceTransportMediaIndexEntry> {
        self.topology
            .source_transport_media_entry(transport_media_id)
    }

    #[must_use]
    pub fn producer_route_target(
        &self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> Option<ProducerRouteTarget> {
        self.topology
            .producer_route_target(owner_user_id, owner_connection_id, stream_id)
    }

    #[cfg(any(test, feature = "testing-transport"))]
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
        intent: &SourceUnpublishIntent,
    ) -> Option<UnpublishCommit> {
        let producer_target =
            self.producer_route_target(user_id, connection_id, intent.stream_id())?;
        let source_recipients = self
            .topology
            .committed_consumer_user_ids_for_source(producer_target.source_id);
        let before = self.media_counts();
        let transport_plan = self.topology.unpublish_source(user_id, &producer_target)?;
        let after = self.media_counts();
        Some(UnpublishCommit {
            before,
            after,
            source_snapshots: self.remote_source_snapshots_for_users(source_recipients, true),
            transport_plan,
            presence: intent.presence().and_then(|info| {
                self.apply_presence_update(user_id, connection_id, info, RemoteSourceRefresh::Skip)
            }),
        })
    }

    pub fn apply_producer_activity(
        &mut self,
        user_id: &UserId,
        producer_target: &ProducerRouteTarget,
        stream_id: &UserStreamId,
        active: bool,
    ) -> Option<BTreeSet<UserId>> {
        let current_connection_id = self.user_connection_id(user_id);
        let changed = match self.topology.set_published_source_activity(
            producer_target,
            current_connection_id,
            active,
        ) {
            Ok(changed) => changed,
            Err(error) => {
                error!(
                    ?user_id,
                    stream_id = %stream_id,
                    ?error,
                    "failed to set producer pause state in room router"
                );
                return None;
            }
        };
        if !changed {
            return None;
        }
        Some(
            self.topology
                .committed_consumer_user_ids_for_source(producer_target.source_id),
        )
    }

    pub fn apply_publication_activity(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
        active: bool,
        presence: Option<&UserInfo>,
    ) -> Result<ProducerActivityCommit, ProducerActivityRejection> {
        let producer_target = self
            .producer_route_target(user_id, connection_id, stream_id)
            .ok_or(ProducerActivityRejection::MissingPublication)?;
        let Some(transport_user_key) =
            self.committed_transport_user_key(user_id, producer_target.owner_connection_id)
        else {
            return Err(ProducerActivityRejection::MissingPublication);
        };
        let Some(source_recipients) =
            self.apply_producer_activity(user_id, &producer_target, stream_id, active)
        else {
            return Err(ProducerActivityRejection::StalePublication);
        };
        let presence = presence.and_then(|info| {
            self.apply_presence_update(user_id, connection_id, info, RemoteSourceRefresh::Skip)
        });
        let source_snapshots = self.remote_source_snapshots_for_users(source_recipients, false);
        Ok(ProducerActivityCommit {
            source: TransportSourceKey::new(transport_user_key, producer_target.transport_media_id),
            stream_id: stream_id.clone(),
            active,
            source_snapshots,
            presence,
        })
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
