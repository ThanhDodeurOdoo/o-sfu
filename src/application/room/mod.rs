use std::collections::BTreeMap;

use o_sfu_protocol::{shared as protocol_shared, signaling as protocol_signaling};

use crate::core::runtime as core_runtime;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "app-owned publication handles are introduced before every room media path consumes them directly"
)]
pub(crate) struct CallPublication {
    pub(crate) user_id: protocol_shared::UserId,
    pub(crate) slot: super::call_policy::CallPublicationSlot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "app room policy state is currently exercised through focused tests while websocket flows are migrated incrementally"
)]
pub(crate) struct CallRoomState {
    publications: Vec<CallPublication>,
}

impl CallRoomState {
    #[allow(
        dead_code,
        reason = "focused tests exercise app-owned publication policy before production websocket flows call this state directly"
    )]
    #[must_use]
    pub(crate) fn publish(
        &mut self,
        user_id: protocol_shared::UserId,
        slot: super::call_policy::CallPublicationSlot,
    ) -> bool {
        if self
            .publications
            .iter()
            .any(|publication| publication.user_id == user_id && publication.slot == slot)
        {
            return false;
        }
        self.publications.push(CallPublication { user_id, slot });
        true
    }

    #[allow(
        dead_code,
        reason = "focused tests exercise app-owned publication policy before production websocket flows call this state directly"
    )]
    #[must_use]
    pub(crate) fn unpublish(
        &mut self,
        user_id: &protocol_shared::UserId,
        slot: super::call_policy::CallPublicationSlot,
    ) -> bool {
        let previous_len = self.publications.len();
        self.publications
            .retain(|publication| publication.user_id != *user_id || publication.slot != slot);
        self.publications.len() != previous_len
    }
}

#[allow(
    dead_code,
    reason = "public application-room owner type reserved for the next direct room-manager wiring slice"
)]
pub(crate) struct CallRoom;
#[allow(
    dead_code,
    reason = "public application-room manager type reserved for the next direct runtime wiring slice"
)]
pub(crate) struct CallRoomManager;
#[allow(
    dead_code,
    reason = "recording policy owner type reserved while compatibility recording calls still delegate through core room methods"
)]
pub(crate) struct CallRecordingController;
#[allow(
    dead_code,
    reason = "source policy owner type reserved while current packet-gate projection still runs through core room methods"
)]
pub(crate) struct CallSourcePolicy;

#[must_use]
pub(crate) fn core_user_id(user_id: &protocol_shared::UserId) -> core_runtime::UserId {
    match user_id {
        protocol_shared::UserId::Integer(value) => core_runtime::UserId::Integer(*value),
        protocol_shared::UserId::String(value) => core_runtime::UserId::String(value.clone()),
    }
}

#[must_use]
pub(crate) fn protocol_user_id(user_id: &core_runtime::UserId) -> protocol_shared::UserId {
    match user_id {
        core_runtime::UserId::Integer(value) => protocol_shared::UserId::Integer(*value),
        core_runtime::UserId::String(value) => protocol_shared::UserId::String(value.clone()),
    }
}

#[must_use]
pub(crate) const fn core_stream_type(
    stream_type: protocol_shared::StreamType,
) -> core_runtime::StreamType {
    match stream_type {
        protocol_shared::StreamType::Audio => core_runtime::StreamType::Audio,
        protocol_shared::StreamType::Camera => core_runtime::StreamType::Camera,
        protocol_shared::StreamType::Screen => core_runtime::StreamType::Screen,
    }
}

#[must_use]
pub(crate) const fn protocol_stream_type(
    stream_type: core_runtime::StreamType,
) -> protocol_shared::StreamType {
    match stream_type {
        core_runtime::StreamType::Audio => protocol_shared::StreamType::Audio,
        core_runtime::StreamType::Camera => protocol_shared::StreamType::Camera,
        core_runtime::StreamType::Screen => protocol_shared::StreamType::Screen,
    }
}

#[must_use]
pub(crate) const fn core_video_layout_intent(
    intent: protocol_shared::VideoLayoutIntent,
) -> core_runtime::VideoLayoutIntent {
    match intent {
        protocol_shared::VideoLayoutIntent::Featured => core_runtime::VideoLayoutIntent::Featured,
        protocol_shared::VideoLayoutIntent::Pinned => core_runtime::VideoLayoutIntent::Pinned,
        protocol_shared::VideoLayoutIntent::VisibleThumbnail => {
            core_runtime::VideoLayoutIntent::VisibleThumbnail
        }
        protocol_shared::VideoLayoutIntent::Hidden => core_runtime::VideoLayoutIntent::Hidden,
        protocol_shared::VideoLayoutIntent::Overflow => core_runtime::VideoLayoutIntent::Overflow,
    }
}

#[must_use]
pub(crate) fn core_user_info(info: &protocol_shared::UserInfo) -> core_runtime::UserInfo {
    core_runtime::UserInfo {
        is_talking: info.is_talking,
        is_featured: info.is_featured,
        is_camera_on: info.is_camera_on,
        is_screen_sharing_on: info.is_screen_sharing_on,
        is_self_muted: info.is_self_muted,
        is_deaf: info.is_deaf,
        is_raising_hand: info.is_raising_hand,
    }
}

#[must_use]
pub(crate) fn protocol_user_info(info: &core_runtime::UserInfo) -> protocol_shared::UserInfo {
    protocol_shared::UserInfo {
        is_talking: info.is_talking,
        is_featured: info.is_featured,
        is_camera_on: info.is_camera_on,
        is_screen_sharing_on: info.is_screen_sharing_on,
        is_self_muted: info.is_self_muted,
        is_deaf: info.is_deaf,
        is_raising_hand: info.is_raising_hand,
    }
}

#[must_use]
pub(crate) fn core_download_states(
    states: &protocol_shared::DownloadStates,
) -> core_runtime::DownloadStates {
    core_runtime::DownloadStates {
        audio: states.audio,
        camera: states.camera,
        screen: states.screen,
        camera_layout: states.camera_layout.map(core_video_layout_intent),
        screen_layout: states.screen_layout.map(core_video_layout_intent),
    }
}

#[must_use]
pub(crate) const fn protocol_available_features(
    features: &core_runtime::AvailableFeatures,
) -> protocol_shared::AvailableFeatures {
    protocol_shared::AvailableFeatures {
        rtc: features.rtc,
        transcription: features.transcription,
        audio_recording: features.audio_recording,
        video_recording: features.video_recording,
    }
}

#[must_use]
pub(crate) const fn core_user_permissions(
    permissions: &protocol_shared::UserPermissions,
) -> core_runtime::UserPermissions {
    core_runtime::UserPermissions {
        transcription: permissions.transcription,
        audio_recording: permissions.audio_recording,
        video_recording: permissions.video_recording,
    }
}

#[must_use]
pub(crate) const fn protocol_recording_state(
    state: &core_runtime::RecordingState,
) -> protocol_shared::RecordingState {
    protocol_shared::RecordingState {
        recording: state.recording,
        audio: state.audio,
        transcription: state.transcription,
        video: state.video,
    }
}

#[must_use]
pub(crate) const fn protocol_stop_code(
    stop_code: core_runtime::StopCode,
) -> protocol_shared::StopCode {
    match stop_code {
        core_runtime::StopCode::UserRequest => protocol_shared::StopCode::UserRequest,
        core_runtime::StopCode::ChannelClosed => protocol_shared::StopCode::ChannelClosed,
        core_runtime::StopCode::RecordingTimeout => protocol_shared::StopCode::RecordingTimeout,
        core_runtime::StopCode::RecordingFailed => protocol_shared::StopCode::RecordingFailed,
        core_runtime::StopCode::DiskSpaceExhausted => protocol_shared::StopCode::DiskSpaceExhausted,
    }
}

#[must_use]
pub(crate) fn protocol_recording_state_update(
    update: &core_runtime::RecordingStateUpdate,
) -> protocol_shared::RecordingStateUpdate {
    protocol_shared::RecordingStateUpdate {
        state: protocol_recording_state(&update.state),
        stop_code: update.stop_code.map(protocol_stop_code),
    }
}

#[must_use]
pub(crate) fn core_recording_options(
    options: &protocol_signaling::RecordingOptions,
) -> core_runtime::RecordingOptions {
    core_runtime::RecordingOptions {
        audio: options.audio,
        transcription: options.transcription,
        video: options.video,
    }
}

#[must_use]
pub(crate) fn protocol_peer_snapshot(
    peer: &core_runtime::PeerSnapshot,
) -> protocol_signaling::PeerSnapshot {
    protocol_signaling::PeerSnapshot {
        user_id: protocol_user_id(&peer.user_id),
        info: protocol_user_info(&peer.info),
    }
}

#[must_use]
pub(crate) fn protocol_user_info_snapshot(
    snapshot: BTreeMap<core_runtime::UserId, core_runtime::UserInfo>,
) -> BTreeMap<protocol_shared::UserId, protocol_shared::UserInfo> {
    snapshot
        .into_iter()
        .map(|(user_id, info)| (protocol_user_id(&user_id), protocol_user_info(&info)))
        .collect()
}

#[must_use]
pub(crate) const fn core_websocket_close_code(
    code: protocol_signaling::WebSocketCloseCode,
) -> core_runtime::WebSocketCloseCode {
    match code {
        protocol_signaling::WebSocketCloseCode::Clean => core_runtime::WebSocketCloseCode::Clean,
        protocol_signaling::WebSocketCloseCode::Leaving => {
            core_runtime::WebSocketCloseCode::Leaving
        }
        protocol_signaling::WebSocketCloseCode::ProtocolError => {
            core_runtime::WebSocketCloseCode::ProtocolError
        }
        protocol_signaling::WebSocketCloseCode::Error => core_runtime::WebSocketCloseCode::Error,
        protocol_signaling::WebSocketCloseCode::AuthFailed => {
            core_runtime::WebSocketCloseCode::AuthFailed
        }
        protocol_signaling::WebSocketCloseCode::AuthTimeout => {
            core_runtime::WebSocketCloseCode::AuthTimeout
        }
        protocol_signaling::WebSocketCloseCode::Kicked => core_runtime::WebSocketCloseCode::Kicked,
        protocol_signaling::WebSocketCloseCode::RoomFull => {
            core_runtime::WebSocketCloseCode::RoomFull
        }
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::shared::{StreamType, UserId, UserInfo};

    use super::*;
    use crate::application::call_policy::CallPublicationSlot;

    #[test]
    fn default_odoo_policy_exposes_one_slot_per_stream_type() {
        assert_eq!(
            CallPublicationSlot::default_odoo_slots(),
            [
                CallPublicationSlot::from_stream_type(StreamType::Audio),
                CallPublicationSlot::from_stream_type(StreamType::Camera),
                CallPublicationSlot::from_stream_type(StreamType::Screen),
            ]
        );
    }

    #[test]
    fn duplicate_publish_is_idempotent() {
        let mut state = CallRoomState::default();
        let user_id = UserId::Integer(7);
        let slot = CallPublicationSlot::from_stream_type(StreamType::Camera);

        assert!(state.publish(user_id.clone(), slot));
        assert!(!state.publish(user_id.clone(), slot));
        assert!(state.unpublish(&user_id, slot));
        assert!(!state.unpublish(&user_id, slot));
    }

    #[test]
    fn app_only_extra_audio_slot_does_not_need_core_stream_type() {
        let slot = CallPublicationSlot::background_music();

        assert_eq!(slot.media_kind(), o_sfu_router::MediaKind::Audio);
        assert_eq!(slot.compatibility_stream_type(), None);
    }

    #[test]
    fn presence_updates_stay_app_state_until_policy_maps_them() {
        let presence = super::super::call_policy::CallPresence::from_user_info(&UserInfo {
            is_talking: Some(true),
            is_raising_hand: Some(true),
            ..UserInfo::default()
        });

        assert!(!presence.affects_media_routing());
    }
}
