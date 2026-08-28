use std::{borrow::Cow, collections::BTreeMap};

use serde::{Serialize, Serializer};

use crate::{
    bundle_api::{
        BundleBroadcastUpdate, BundleDisconnectUpdate, BundleRemoteMediaUpdate,
        BundleSessionInfoSnapshotById, BundleUpdate, bundle_session_info_key,
    },
    core::ProtocolEvent,
    shared::{JsonPayload, RecordingStateUpdate, UserId, UserInfo},
    signaling::TrackBinding,
};

#[derive(Serialize)]
#[serde(tag = "name", content = "payload", rename_all = "snake_case")]
enum BundleUpdateRef<'a> {
    RemoteMedia {
        bindings: &'a [TrackBinding],
    },
    Broadcast {
        #[serde(rename = "senderId")]
        sender_id: &'a UserId,
        message: &'a JsonPayload,
    },
    Disconnect {
        #[serde(rename = "sessionId")]
        user_id: &'a UserId,
    },
    #[serde(rename = "info_change")]
    SessionInfoChange(BTreeMap<Cow<'a, str>, &'a UserInfo>),
    ChannelInfoChange(&'a RecordingStateUpdate),
}

#[must_use]
pub fn project_protocol_event(event: ProtocolEvent) -> BundleUpdate {
    match event {
        ProtocolEvent::TrackSnapshot { bindings } => {
            BundleUpdate::RemoteMedia(BundleRemoteMediaUpdate { bindings })
        }
        ProtocolEvent::PeerSnapshot { peers } => BundleUpdate::SessionInfoChange(
            peers
                .into_iter()
                .map(|peer| (bundle_session_info_key(&peer.user_id), peer.info))
                .collect::<BundleSessionInfoSnapshotById>(),
        ),
        ProtocolEvent::PeerInfo { user_id, info } => BundleUpdate::SessionInfoChange(
            BundleSessionInfoSnapshotById::from([(bundle_session_info_key(&user_id), info)]),
        ),
        ProtocolEvent::PeerLeft { user_id } => {
            BundleUpdate::Disconnect(BundleDisconnectUpdate { user_id })
        }
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdate::Broadcast(BundleBroadcastUpdate { sender_id, message })
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdate::ChannelInfoChange(state),
    }
}

pub(crate) fn serialize_protocol_event<S>(
    event: &ProtocolEvent,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let update = match event {
        ProtocolEvent::TrackSnapshot { bindings } => BundleUpdateRef::RemoteMedia { bindings },
        ProtocolEvent::PeerSnapshot { peers } => BundleUpdateRef::SessionInfoChange(
            peers
                .iter()
                .map(|peer| (peer.user_id.path_segment(), &peer.info))
                .collect(),
        ),
        ProtocolEvent::PeerInfo { user_id, info } => {
            BundleUpdateRef::SessionInfoChange(BTreeMap::from([(user_id.path_segment(), info)]))
        }
        ProtocolEvent::PeerLeft { user_id } => BundleUpdateRef::Disconnect { user_id },
        ProtocolEvent::Broadcast { sender_id, message } => {
            BundleUpdateRef::Broadcast { sender_id, message }
        }
        ProtocolEvent::RecordingStateChanged { state } => BundleUpdateRef::ChannelInfoChange(state),
    };
    update.serialize(serializer)
}

#[cfg(test)]
#[path = "host_bridge/TESTS/mod.rs"]
mod tests;
