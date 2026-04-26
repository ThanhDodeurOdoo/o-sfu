use o_sfu_protocol::shared as protocol_shared;

use crate::application::call_policy::CallPublicationSlot;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "app-owned publication handles are introduced before every room media path consumes them directly"
)]
pub(crate) struct CallPublication {
    pub(crate) user_id: protocol_shared::UserId,
    pub(crate) slot: CallPublicationSlot,
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
        slot: CallPublicationSlot,
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
        slot: CallPublicationSlot,
    ) -> bool {
        let previous_len = self.publications.len();
        self.publications
            .retain(|publication| publication.user_id != *user_id || publication.slot != slot);
        self.publications.len() != previous_len
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::shared::{StreamType, UserId, UserInfo};

    use super::*;
    use crate::application::call_policy::{CallPresence, CallPublicationSlot};

    #[test]
    fn default_policy_exposes_one_slot_per_stream_type() {
        assert_eq!(
            CallPublicationSlot::default_slots(),
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
    fn presence_updates_stay_app_state_until_policy_maps_them() {
        let presence = CallPresence::from_user_info(&UserInfo {
            is_talking: Some(true),
            is_raising_hand: Some(true),
            ..UserInfo::default()
        });

        assert!(!presence.affects_media_routing());
    }
}
