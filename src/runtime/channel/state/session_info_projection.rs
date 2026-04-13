use crate::signaling::shared::SessionInfo;

use super::presence::SessionPresence;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::runtime::channel) struct SessionMediaView {
    pub(in crate::runtime::channel) camera_active: Option<bool>,
    pub(in crate::runtime::channel) screen_active: Option<bool>,
}

#[must_use]
pub(in crate::runtime::channel) fn project_session_info(
    presence: &SessionPresence,
    media: SessionMediaView,
) -> SessionInfo {
    SessionInfo {
        is_talking: presence.talking(),
        is_camera_on: media.camera_active,
        is_screen_sharing_on: media.screen_active,
        is_self_muted: presence.self_muted(),
        is_deaf: presence.deaf(),
        is_raising_hand: presence.raising_hand(),
    }
}
