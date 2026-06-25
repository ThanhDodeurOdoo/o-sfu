use std::collections::BTreeMap;

use o_sfu_protocol::wire::{
    ServerMessage, SourceDescriptor, SourceEncodingDescriptor, StreamType, TrackBinding,
    UploadLayerPolicyRole as ProtocolUploadLayerPolicyRole, UserId, UserInfo,
};

use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::{
        prelude::{Bitrate, UploadLayerPolicyRole},
        server::source_model::{PublishedSourceDescriptor, SourceTemporalLayerId},
    },
    runtime::room::{RemoteTrackSetup, TrackBindingUpdate},
};

#[derive(Debug, Default)]
pub struct TrackSnapshot {
    by_mid: BTreeMap<String, SourceDescriptor>,
}

impl TrackSnapshot {
    pub fn add_remote(&mut self, track: &RemoteTrackSetup) {
        let Some(stream_type) = stream_type_for_stream_id(&track.stream) else {
            return;
        };
        self.by_mid.insert(
            track.mid.clone(),
            wire_source_descriptor(&track.source, track.user.clone(), stream_type, track.active),
        );
    }

    pub fn snapshot_messages(&self) -> [ServerMessage; 2] {
        [
            ServerMessage::Tracks(
                self.by_mid
                    .iter()
                    .map(|(mid, source)| TrackBinding {
                        mid: mid.to_owned(),
                        user_id: source.user_id.clone(),
                        stream_type: source.stream_type,
                        active: source.active,
                    })
                    .collect(),
            ),
            ServerMessage::Sources(self.by_mid.values().cloned().collect()),
        ]
    }

    pub fn remove_user(&mut self, user_id: &UserId) -> bool {
        let count = self.by_mid.len();
        self.by_mid
            .retain(|_mid, source| &source.user_id != user_id);
        self.by_mid.len() != count
    }

    pub fn apply_infos(&mut self, snapshot: &BTreeMap<UserId, UserInfo>) -> bool {
        let mut changed = false;
        for source in self.by_mid.values_mut() {
            let Some(info) = snapshot.get(&source.user_id) else {
                continue;
            };
            let Some(active) = (match source.stream_type {
                StreamType::Camera => info.is_camera_on,
                StreamType::Screen => info.is_screen_sharing_on,
                StreamType::Audio => None,
            }) else {
                continue;
            };
            changed |= set_active(&mut source.active, active);
        }
        changed
    }

    pub fn apply_update(&mut self, update: &TrackBindingUpdate) -> bool {
        let Some(stream_type) = stream_type_for_stream_id(&update.stream_id) else {
            return false;
        };
        if let Some(active) = update.active {
            self.set_track_active(&update.user_id, stream_type, active)
        } else {
            let count = self.by_mid.len();
            self.by_mid.retain(|_mid, source| {
                source.user_id != update.user_id || source.stream_type != stream_type
            });
            self.by_mid.len() != count
        }
    }

    fn set_track_active(
        &mut self,
        user_id: &UserId,
        stream_type: StreamType,
        active: bool,
    ) -> bool {
        let mut changed = false;
        for source in self.by_mid.values_mut() {
            if &source.user_id != user_id || source.stream_type != stream_type {
                continue;
            }
            changed |= set_active(&mut source.active, active);
        }
        changed
    }
}

fn set_active(active: &mut bool, next: bool) -> bool {
    if *active == next {
        return false;
    }
    *active = next;
    true
}

fn wire_source_descriptor(
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
            max_bitrate: encoding.max_bitrate().map(Bitrate::as_bps),
            resolution_scale: encoding.resolution_scale(),
            max_framerate: encoding.max_framerate(),
            policy_role: encoding
                .policy_role()
                .map(protocol_upload_layer_policy_role),
            max_temporal_layer_id: encoding
                .max_temporal_layer_id()
                .map(SourceTemporalLayerId::as_u8),
        })
        .collect()
}

fn protocol_upload_layer_policy_role(role: UploadLayerPolicyRole) -> ProtocolUploadLayerPolicyRole {
    match role {
        UploadLayerPolicyRole::Featured => ProtocolUploadLayerPolicyRole::Featured,
        UploadLayerPolicyRole::Thumbnail => ProtocolUploadLayerPolicyRole::Thumbnail,
        UploadLayerPolicyRole::DegradedThumbnail => {
            ProtocolUploadLayerPolicyRole::DegradedThumbnail
        }
    }
}
