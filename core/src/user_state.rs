use std::{
    collections::{BTreeMap, BTreeSet},
    mem::take,
};

use o_sfu_protocol::{
    shared::{StreamType, UserId, UserInfo},
    signaling::{
        PeerInfoPayload, PeerLeftPayload, RequestId, ServerBroadcastPayload, ServerMessage,
        ServerRequest, SourceDescriptor, SourceEncodingDescriptor, TrackBinding,
    },
};

use crate::{
    OfferedMediaCapabilities,
    runtime::{
        room::{RemoteTrackBootstrap, RoomEventMessage, TrackBindingUpdate},
        source_model::{PublishedSourceDescriptor, SourceTemporalLayerId},
    },
};

#[derive(Debug, Default)]
pub(super) struct UserState {
    request_ids: UserRequestIds,
    pub(super) negotiation: UserNegotiationState,
    pub(super) tracks: RemoteTrackProjection,
}

impl UserState {
    pub(super) fn next_request_id(&mut self) -> RequestId {
        self.request_ids.next()
    }
}

#[derive(Debug, Default)]
struct UserRequestIds {
    next_request_counter: u64,
}

impl UserRequestIds {
    fn next(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PendingUserAction {
    EstablishSession {
        offered_capabilities: OfferedMediaCapabilities,
    },
    RefreshSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingUserRequest {
    pub(super) request_id: RequestId,
    pub(super) request: ServerRequest,
    pub(super) action: PendingUserAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RenegotiationDisposition {
    Skip,
    QueueOnly,
    SendNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedUserNegotiation {
    pub(super) pending: PendingUserRequest,
    pub(super) queued_renegotiation: bool,
}

#[derive(Debug)]
pub(super) enum UserNegotiationState {
    BeforeInitialOffer {
        queued_publish_streams: BTreeSet<StreamType>,
    },
    Stable {
        queued_publish_streams: BTreeSet<StreamType>,
    },
    Negotiating {
        pending: PendingUserRequest,
        queued_publish_streams: BTreeSet<StreamType>,
        queued_renegotiation: bool,
    },
}

impl Default for UserNegotiationState {
    fn default() -> Self {
        Self::BeforeInitialOffer {
            queued_publish_streams: BTreeSet::default(),
        }
    }
}

impl UserNegotiationState {
    pub(super) const fn awaiting_answer(&self) -> bool {
        matches!(self, Self::Negotiating { .. })
    }

    pub(super) fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_streams().contains(&stream_type)
    }

    pub(super) fn queue_publish_stream(&mut self, stream_type: StreamType) {
        self.queued_publish_streams_mut().insert(stream_type);
    }

    pub(super) fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_streams_mut().remove(&stream_type)
    }

    pub(super) fn take_queued_publish_streams(&mut self) -> Vec<StreamType> {
        let queued_publish_streams = self.queued_publish_streams().iter().copied().collect();
        self.queued_publish_streams_mut().clear();
        queued_publish_streams
    }

    pub(super) fn issue(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
        action: PendingUserAction,
    ) {
        let queued_publish_streams = self.take_phase_queue();
        *self = Self::Negotiating {
            pending: PendingUserRequest {
                request_id,
                request,
                action,
            },
            queued_publish_streams,
            queued_renegotiation: false,
        };
    }

    pub(super) fn request_renegotiation(&mut self) -> RenegotiationDisposition {
        match self {
            Self::BeforeInitialOffer { .. } => RenegotiationDisposition::Skip,
            Self::Stable { .. } => RenegotiationDisposition::SendNow,
            Self::Negotiating {
                queued_renegotiation,
                ..
            } => {
                *queued_renegotiation = true;
                RenegotiationDisposition::QueueOnly
            }
        }
    }

    pub(super) fn resolve_answer(
        &mut self,
        response_to: &RequestId,
    ) -> Option<ResolvedUserNegotiation> {
        let previous = take(self);
        match previous {
            Self::Negotiating {
                pending,
                queued_publish_streams,
                queued_renegotiation,
            } if pending.request_id == *response_to => {
                *self = Self::Stable {
                    queued_publish_streams,
                };
                Some(ResolvedUserNegotiation {
                    pending,
                    queued_renegotiation,
                })
            }
            other => {
                *self = other;
                None
            }
        }
    }

    fn queued_publish_streams(&self) -> &BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }

    fn queued_publish_streams_mut(&mut self) -> &mut BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }

    fn take_phase_queue(&mut self) -> BTreeSet<StreamType> {
        match take(self) {
            Self::BeforeInitialOffer {
                queued_publish_streams,
            }
            | Self::Stable {
                queued_publish_streams,
            }
            | Self::Negotiating {
                queued_publish_streams,
                ..
            } => queued_publish_streams,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TranslatedRoomMessage {
    pub(super) messages: Vec<ServerMessage>,
    pub(super) needs_renegotiation: bool,
}

impl TranslatedRoomMessage {
    fn messages(messages: Vec<ServerMessage>) -> Self {
        Self {
            messages,
            needs_renegotiation: false,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RemoteTrackProjection {
    bindings_by_mid: BTreeMap<String, TrackBinding>,
}

impl RemoteTrackProjection {
    pub(super) fn translate_room_message(
        &mut self,
        message: RoomEventMessage,
    ) -> TranslatedRoomMessage {
        match message {
            RoomEventMessage::Broadcast { sender_id, message } => {
                TranslatedRoomMessage::messages(vec![ServerMessage::Broadcast(
                    ServerBroadcastPayload { sender_id, message },
                )])
            }
            RoomEventMessage::UserJoined { user_id, info } => {
                TranslatedRoomMessage::messages(vec![ServerMessage::PeerJoined(PeerInfoPayload {
                    user_id,
                    info,
                })])
            }
            RoomEventMessage::UserDeparted { user_id } => {
                let removed_tracks = self
                    .bindings_by_mid
                    .values()
                    .any(|binding| binding.user_id == user_id);
                self.bindings_by_mid
                    .retain(|_mid, binding| binding.user_id != user_id);
                TranslatedRoomMessage {
                    messages: vec![ServerMessage::PeerLeft(PeerLeftPayload { user_id })],
                    needs_renegotiation: removed_tracks,
                }
            }
            RoomEventMessage::UserInfoChanged(snapshot) => {
                self.translate_user_info_snapshot(snapshot)
            }
            RoomEventMessage::RecordingStateChanged(state) => {
                TranslatedRoomMessage::messages(vec![ServerMessage::RecordingChange(state)])
            }
        }
    }

    pub(super) fn apply_remote_track_bootstrap(&mut self, payload: &RemoteTrackBootstrap) {
        let mid = payload.mid().to_owned();
        self.apply_track_binding(
            mid,
            payload.user_id().clone(),
            payload.stream_type(),
            payload.active(),
            payload.source_descriptor(),
        );
    }

    pub(super) fn snapshot(&self) -> Vec<TrackBinding> {
        self.bindings_by_mid.values().cloned().collect()
    }

    pub(super) fn translate_track_binding_update(
        &mut self,
        update: &TrackBindingUpdate,
    ) -> TranslatedRoomMessage {
        let changed = match update.active {
            Some(active) => self.set_track_active(&update.user_id, update.stream_type, active),
            None => self.remove_track_binding(&update.user_id, update.stream_type),
        };
        if !changed {
            return TranslatedRoomMessage::messages(Vec::new());
        }
        TranslatedRoomMessage {
            messages: vec![ServerMessage::Tracks(self.snapshot())],
            needs_renegotiation: update.active.is_none(),
        }
    }

    fn apply_track_binding(
        &mut self,
        mid: String,
        user_id: UserId,
        stream_type: StreamType,
        active: bool,
        source: &PublishedSourceDescriptor,
    ) {
        self.bindings_by_mid.insert(
            mid.clone(),
            TrackBinding {
                mid,
                user_id: user_id.clone(),
                stream_type,
                active,
                source: Some(source_descriptor_from_source(
                    source,
                    user_id,
                    stream_type,
                    active,
                )),
            },
        );
    }

    fn translate_user_info_snapshot(
        &mut self,
        snapshot: BTreeMap<UserId, UserInfo>,
    ) -> TranslatedRoomMessage {
        let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
        let mut track_snapshot_changed = false;
        for (user_id, info) in snapshot {
            track_snapshot_changed |= self.apply_user_info_to_tracks(&user_id, &info);
            messages.push(ServerMessage::PeerInfo(PeerInfoPayload { user_id, info }));
        }
        if track_snapshot_changed {
            messages.push(ServerMessage::Tracks(self.snapshot()));
        }
        TranslatedRoomMessage {
            messages,
            needs_renegotiation: false,
        }
    }

    fn apply_user_info_to_tracks(&mut self, user_id: &UserId, info: &UserInfo) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.user_id != user_id {
                continue;
            }
            let next_active = match binding.stream_type {
                StreamType::Camera => info.is_camera_on,
                StreamType::Screen => info.is_screen_sharing_on,
                StreamType::Audio => None,
            };
            let Some(next_active) = next_active else {
                continue;
            };
            if binding.active != next_active {
                binding.active = next_active;
                if let Some(source) = binding.source.as_mut() {
                    source.active = next_active;
                }
                changed = true;
            }
        }
        changed
    }

    fn set_track_active(
        &mut self,
        user_id: &UserId,
        stream_type: StreamType,
        active: bool,
    ) -> bool {
        let mut changed = false;
        for binding in self.bindings_by_mid.values_mut() {
            if &binding.user_id != user_id || binding.stream_type != stream_type {
                continue;
            }
            if binding.active != active {
                binding.active = active;
                if let Some(source) = binding.source.as_mut() {
                    source.active = active;
                }
                changed = true;
            }
        }
        changed
    }

    fn remove_track_binding(&mut self, user_id: &UserId, stream_type: StreamType) -> bool {
        let binding_count = self.bindings_by_mid.len();
        self.bindings_by_mid.retain(|_mid, binding| {
            &binding.user_id != user_id || binding.stream_type != stream_type
        });
        self.bindings_by_mid.len() != binding_count
    }
}

fn source_descriptor_from_source(
    source: &PublishedSourceDescriptor,
    user_id: UserId,
    stream_type: StreamType,
    active: bool,
) -> SourceDescriptor {
    SourceDescriptor {
        source_id: source.source_id().to_string(),
        user_id,
        stream_type,
        active,
        mid: source.mid().map(|mid| mid.as_str().to_owned()),
        encodings: source_encodings(source),
    }
}

fn source_encodings(source: &PublishedSourceDescriptor) -> Vec<SourceEncodingDescriptor> {
    source
        .encodings()
        .map(|encoding| SourceEncodingDescriptor {
            encoding_id: encoding.encoding_id().to_string(),
            rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
            max_bitrate: encoding.max_bitrate(),
            max_temporal_layer_id: encoding
                .max_temporal_layer_id()
                .map(SourceTemporalLayerId::as_u8),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        reason = "test assertions use expect for direct fixture failures"
    )]

    use o_sfu_protocol::{
        shared::StreamType,
        signaling::{RequestId, ServerRequest, SessionDescriptionPayload},
    };
    use o_sfu_router::{MediaKind, Mid, Rid};

    use super::*;
    use crate::runtime::source_model::{
        PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
        SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
    };

    #[test]
    fn queued_publish_streams_are_unique() {
        let mut state = UserNegotiationState::default();

        state.queue_publish_stream(StreamType::Camera);
        state.queue_publish_stream(StreamType::Camera);

        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
    }

    #[test]
    fn resolving_answer_keeps_queued_publish_streams_for_follow_up_staging() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = UserNegotiationState::default();
        state.queue_publish_stream(StreamType::Camera);
        state.issue(
            request_id.clone(),
            ServerRequest::Offer(SessionDescriptionPayload {
                sdp: String::from("v=0"),
                upload_slots: Vec::new(),
            }),
            PendingUserAction::EstablishSession {
                offered_capabilities: OfferedMediaCapabilities::default(),
            },
        );

        let resolved = state.resolve_answer(&request_id);

        assert!(resolved.is_some());
        assert!(matches!(
            state.request_renegotiation(),
            RenegotiationDisposition::SendNow
        ));
        assert_eq!(
            state.take_queued_publish_streams(),
            vec![StreamType::Camera]
        );
    }

    #[test]
    fn stale_answers_keep_the_current_pending_request() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = UserNegotiationState::default();
        state.issue(
            request_id,
            ServerRequest::Renegotiate(SessionDescriptionPayload {
                sdp: String::from("v=0"),
                upload_slots: Vec::new(),
            }),
            PendingUserAction::RefreshSession,
        );

        assert!(
            state
                .resolve_answer(&RequestId::new(String::from("server-2")))
                .is_none()
        );
        assert!(state.awaiting_answer());
        assert!(matches!(
            state.request_renegotiation(),
            RenegotiationDisposition::QueueOnly
        ));
    }

    #[test]
    fn source_descriptor_mid_uses_published_source_mid_not_consumer_binding_mid() {
        let source = published_source("published-cam-0");
        let mut projection = RemoteTrackProjection::default();

        projection.apply_track_binding(
            "subscriber-down-0".to_owned(),
            UserId::Integer(7),
            StreamType::Camera,
            true,
            &source,
        );

        let snapshot = projection.snapshot();
        let binding = snapshot
            .first()
            .expect("projection should contain the inserted track binding");
        assert_eq!(binding.mid, "subscriber-down-0");
        let source = binding
            .source
            .as_ref()
            .expect("track binding should carry a source descriptor");
        assert_eq!(source.mid.as_deref(), Some("published-cam-0"));
    }

    fn published_source(mid: &str) -> PublishedSourceDescriptor {
        let source_id = PublishedSourceId::from_raw(1);
        let encoding = SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
            encoding_id: SourceEncodingId::from_raw(2),
            source_id,
            rid: Some(Rid::new("hi")),
            primary_ssrc: None,
            repair_ssrc: None,
            max_bitrate: Some(900_000),
            max_temporal_layer_id: None,
            negotiated_format: None,
        });
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(UserId::Integer(7)),
            stream_type: StreamType::Camera,
            media_kind: MediaKind::Video,
            mid: Some(Mid::new(mid)),
            encodings: vec![encoding],
        })
        .expect("test source descriptor should satisfy source graph invariants")
    }
}
