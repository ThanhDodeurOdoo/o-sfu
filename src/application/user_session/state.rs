//! Signaling and compatibility state for one websocket connection.
//!
//! this module owns the pure state machines used to sequence server-authored
//! requests and track the browser-facing track snapshot, it has no knowledge of
//! transport resources or media core transactions, which keeps the transition
//! logic deterministic and easy to test.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::take,
};

use o_sfu_protocol::{
    shared::{StreamType, UserId, UserInfo},
    signaling::{
        PeerInfoPayload, PeerLeftPayload, RequestId, ServerBroadcastPayload, ServerMessage,
        ServerRequest, TrackBinding,
    },
};

use super::projection::source_descriptor_from_source;
use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::{OfferedMediaCapabilities, server::source_model::PublishedSourceDescriptor},
    runtime::room::{RemoteTrackBootstrap, RoomEventMessage, TrackBindingUpdate},
};

/// Connection-scoped signaling and compatibility state.
#[derive(Debug, Default)]
pub(super) struct UserState {
    request_id_sequencer: UserRequestIdSequencer,
    pub(super) negotiation_state: UserNegotiationState,
    /// track and peer snapshot for the compatibility layer.
    pub(super) wire_state: UserWireState,
}

impl UserState {
    pub(super) fn next_request_id(&mut self) -> RequestId {
        self.request_id_sequencer.next()
    }
}

/// Generator for monotonic server-authored request ids.
#[derive(Debug, Default)]
struct UserRequestIdSequencer {
    next_request_counter: u64,
}

impl UserRequestIdSequencer {
    fn next(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PendingUserAction {
    /// the request expects to establish the first transport session.
    EstablishSession {
        /// capabilities offered by the server for initial codec negotiation.
        offered_capabilities: OfferedMediaCapabilities,
    },
    /// the request only refreshes an existing transport session.
    RefreshSession,
}

/// metadata for one unresolved server-authored request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingUserRequest {
    /// unique id sent to the client.
    pub(super) request_id: RequestId,
    /// original request payload used for re-validation on answer.
    pub(super) request: ServerRequest,
    /// orchestration intent that triggered the request.
    pub(super) action: PendingUserAction,
}

/// Command returned to the orchestrator after a renegotiation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RenegotiationDisposition {
    /// no negotiation work is needed.
    Skip,
    /// a request is already pending, the intent was queued for a follow-up.
    QueueOnly,
    /// he session is stable and ready to issue a new offer immediately
    SendNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedUserNegotiation {
    pub(super) pending: PendingUserRequest,
    pub(super) queued_renegotiation: bool,
}

/// Negotiation state machine for one browser session.
///
/// the machine ensures that only one server offer is pending at a time, it
/// handles queuing for publish intents and room events that arrive while an
/// answer is outstanding.
#[derive(Debug)]
pub(super) enum UserNegotiationState {
    /// the connection is authenticated but the first offer is not yet issued.
    BeforeInitialOffer {
        /// publish intents received before startup.
        queued_publish_slots: BTreeSet<StreamType>,
    },
    /// the session is active and no server requests are pending.
    Stable {
        /// follow-up publish intents waiting for the next stable window.
        queued_publish_slots: BTreeSet<StreamType>,
    },
    /// an offer or renegotiation request has been sent to the browser.
    Negotiating {
        /// metadata needed to apply the browser's answer.
        pending: PendingUserRequest,
        /// follow-up publish intents received while this request is pending.
        queued_publish_slots: BTreeSet<StreamType>,
        /// whether a generic renegotiation is needed after this answer.
        queued_renegotiation: bool,
    },
}

impl Default for UserNegotiationState {
    fn default() -> Self {
        Self::BeforeInitialOffer {
            queued_publish_slots: BTreeSet::default(),
        }
    }
}

impl UserNegotiationState {
    pub(super) const fn awaiting_answer(&self) -> bool {
        matches!(self, Self::Negotiating { .. })
    }

    pub(super) fn has_queued_publish(&self, stream_type: StreamType) -> bool {
        self.queued_publish_slots().contains(&stream_type)
    }

    pub(super) fn queue_publish_slot(&mut self, stream_type: StreamType) {
        self.queued_publish_slots_mut().insert(stream_type);
    }

    pub(super) fn clear_queued_publish(&mut self, stream_type: StreamType) -> bool {
        self.queued_publish_slots_mut().remove(&stream_type)
    }

    pub(super) fn take_queued_publish_slots(&mut self) -> Vec<StreamType> {
        let queued_publish_slots = self.queued_publish_slots().iter().copied().collect();
        self.queued_publish_slots_mut().clear();
        queued_publish_slots
    }

    /// record a newly issued server request in the state machine.
    ///
    /// this moves the session to [`Self::Negotiating`] and preserves any existing
    /// publish queue.
    pub(super) fn issue(
        &mut self,
        request_id: RequestId,
        request: ServerRequest,
        action: PendingUserAction,
    ) {
        let queued_publish_slots = self.take_phase_queue();
        *self = Self::Negotiating {
            pending: PendingUserRequest {
                request_id,
                request,
                action,
            },
            queued_publish_slots,
            queued_renegotiation: false,
        };
    }

    /// assess whether a new renegotiation offer can be sent right now.
    ///
    /// returns [`RenegotiationDisposition::SendNow`] if the state is stable,
    /// otherwise it flags the machine to trigger a follow-up after the current
    /// answer arrives.
    pub(super) fn schedule_renegotiation(&mut self) -> RenegotiationDisposition {
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

    /// resolve a browser answer and return to stable state.
    ///
    /// returns the pending request metadata if the id matches, otherwise it
    /// returns `None` and preserves the current state.
    pub(super) fn resolve_answer(
        &mut self,
        response_to: &RequestId,
    ) -> Option<ResolvedUserNegotiation> {
        let previous = take(self);
        match previous {
            Self::Negotiating {
                pending,
                queued_publish_slots,
                queued_renegotiation,
            } if pending.request_id == *response_to => {
                *self = Self::Stable {
                    queued_publish_slots,
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

    fn queued_publish_slots(&self) -> &BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_slots,
            }
            | Self::Stable {
                queued_publish_slots,
            }
            | Self::Negotiating {
                queued_publish_slots,
                ..
            } => queued_publish_slots,
        }
    }

    fn queued_publish_slots_mut(&mut self) -> &mut BTreeSet<StreamType> {
        match self {
            Self::BeforeInitialOffer {
                queued_publish_slots,
            }
            | Self::Stable {
                queued_publish_slots,
            }
            | Self::Negotiating {
                queued_publish_slots,
                ..
            } => queued_publish_slots,
        }
    }

    fn take_phase_queue(&mut self) -> BTreeSet<StreamType> {
        match take(self) {
            Self::BeforeInitialOffer {
                queued_publish_slots,
            }
            | Self::Stable {
                queued_publish_slots,
            }
            | Self::Negotiating {
                queued_publish_slots,
                ..
            } => queued_publish_slots,
        }
    }
}

/// Envelope for ordered server messages produced by one room event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UserWireMessages {
    /// messages to be batched and sent through the websocket.
    pub(super) messages: Vec<ServerMessage>,
    /// whether the event invalidated the browser's current track snapshot.
    pub(super) needs_renegotiation: bool,
}

impl UserWireMessages {
    fn messages(messages: Vec<ServerMessage>) -> Self {
        Self {
            messages,
            needs_renegotiation: false,
        }
    }
}

/// Per-connection state used to build compatibility server messages.
///
/// This state owns the remote track snapshot because some room events, such as
/// peer departure or user-info changes, must also update track bindings
/// before the websocket edge serializes the resulting messages.
#[derive(Debug, Default)]
pub(super) struct UserWireState {
    bindings_by_mid: BTreeMap<String, TrackBinding>,
}

impl UserWireState {
    /// build compatibility signaling for one authoritative room message.
    ///
    /// this method updates the local track snapshot and returns the ordered
    /// signals that the browser needs to see the transition.
    pub(super) fn messages_for_room_event(
        &mut self,
        message: RoomEventMessage,
    ) -> UserWireMessages {
        match message {
            RoomEventMessage::Broadcast { sender_id, message } => {
                UserWireMessages::messages(vec![ServerMessage::Broadcast(ServerBroadcastPayload {
                    sender_id,
                    message,
                })])
            }
            RoomEventMessage::UserJoined { user_id, info } => {
                UserWireMessages::messages(vec![ServerMessage::PeerJoined(PeerInfoPayload {
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
                UserWireMessages {
                    messages: vec![ServerMessage::PeerLeft(PeerLeftPayload { user_id })],
                    needs_renegotiation: removed_tracks,
                }
            }
            RoomEventMessage::UserInfoChanged(snapshot) => {
                let mut messages = Vec::with_capacity(snapshot.len().saturating_add(1));
                let mut track_snapshot_changed = false;
                for (user_id, info) in snapshot {
                    track_snapshot_changed |= self.apply_user_info_to_tracks(&user_id, &info);
                    messages.push(ServerMessage::PeerInfo(PeerInfoPayload { user_id, info }));
                }
                if track_snapshot_changed {
                    messages.push(ServerMessage::Tracks(self.snapshot()));
                }
                UserWireMessages {
                    messages,
                    needs_renegotiation: false,
                }
            }
            RoomEventMessage::RecordingStateChanged(state) => {
                UserWireMessages::messages(vec![ServerMessage::RecordingChange(state)])
            }
        }
    }

    /// bootstrap one newly visible remote track for the browser.
    pub(super) fn apply_remote_track_bootstrap(&mut self, track: &RemoteTrackBootstrap) {
        let Some(stream_type) = stream_type_for_stream_id(track.stream_id()) else {
            return;
        };
        self.apply_track_binding(
            track.mid().to_owned(),
            track.user_id().clone(),
            stream_type,
            track.active(),
            track.source_descriptor(),
        );
    }

    /// build a full snapshot of all current track bindings.
    pub(super) fn snapshot(&self) -> Vec<TrackBinding> {
        self.bindings_by_mid.values().cloned().collect()
    }

    /// apply a track-level binding update and return the resulting signals.
    pub(super) fn messages_for_track_binding_update(
        &mut self,
        update: &TrackBindingUpdate,
    ) -> UserWireMessages {
        let user_id = update.user_id.clone();
        let Some(stream_type) = stream_type_for_stream_id(&update.stream_id) else {
            return UserWireMessages::messages(Vec::new());
        };
        let changed = match update.active {
            Some(active) => self.set_track_active(&user_id, stream_type, active),
            None => self.remove_track_binding(&user_id, stream_type),
        };
        if !changed {
            return UserWireMessages::messages(Vec::new());
        }
        UserWireMessages {
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
    use o_sfu_router::{Mid, Rid};

    use super::*;
    use crate::{
        application::stream_catalog::source_publish_intent_for_stream_type,
        core::server::source_model::{
            PublishedSourceDescriptorParts, PublishedSourceId, PublishedSourceOwner,
            SourceEncodingDescriptor, SourceEncodingDescriptorParts, SourceEncodingId,
        },
    };

    #[test]
    fn queued_publish_slots_are_unique() {
        let mut state = UserNegotiationState::default();

        state.queue_publish_slot(StreamType::Camera);
        state.queue_publish_slot(StreamType::Camera);

        assert_eq!(state.take_queued_publish_slots(), vec![StreamType::Camera]);
    }

    #[test]
    fn resolving_answer_keeps_queued_publish_slots_for_follow_up_staging() {
        let request_id = RequestId::new(String::from("server-1"));
        let mut state = UserNegotiationState::default();
        state.queue_publish_slot(StreamType::Camera);
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
            state.schedule_renegotiation(),
            RenegotiationDisposition::SendNow
        ));
        assert_eq!(state.take_queued_publish_slots(), vec![StreamType::Camera]);
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
            state.schedule_renegotiation(),
            RenegotiationDisposition::QueueOnly
        ));
    }

    #[test]
    fn source_descriptor_mid_uses_published_source_mid_not_consumer_binding_mid() {
        let source = published_source("published-cam-0");
        let mut wire_state = UserWireState::default();

        wire_state.apply_track_binding(
            "subscriber-down-0".to_owned(),
            UserId::Integer(7),
            StreamType::Camera,
            true,
            &source,
        );

        let snapshot = wire_state.snapshot();
        let binding = snapshot
            .first()
            .expect("wire state should contain the inserted track binding");
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
            resolution_scale: None,
            max_framerate: None,
            policy_role: None,
            max_temporal_layer_id: None,
            negotiated_format: None,
        });
        let intent = source_publish_intent_for_stream_type(StreamType::Camera);
        PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
            source_id,
            owner: PublishedSourceOwner::new(UserId::Integer(7)),
            stream_id: intent.stream_id().clone(),
            media_kind: intent.media_kind(),
            policy: intent.policy(),
            mid: Some(Mid::new(mid)),
            encodings: vec![encoding],
        })
        .expect("test source descriptor should satisfy source graph invariants")
    }
}
