use o_sfu_protocol::wire::{
    ServerMessage, SourceDescriptor, SourceEncodingDescriptor, StreamType, TrackBinding,
    UploadLayerPolicyRole as ProtocolRole,
};

use crate::{
    application::stream_catalog::stream_type_for_stream_id,
    core::{
        prelude::{Bitrate, UploadLayerPolicyRole},
        server::source_model::{PublishedSourceDescriptor, SourceTemporalLayerId},
    },
    runtime::room::RemoteSourceSnapshot,
};

pub fn snapshot_messages(snapshot: &RemoteSourceSnapshot) -> [ServerMessage; 2] {
    let mut tracks = Vec::with_capacity(snapshot.sources.len());
    let mut sources = Vec::with_capacity(snapshot.sources.len());
    for projection in &snapshot.sources {
        let Some(stream_type) = stream_type_for_stream_id(projection.source.stream_id()) else {
            continue;
        };
        let user_id = projection.source.owner().user_id().clone();
        let active = match stream_type {
            StreamType::Audio => None,
            StreamType::Camera => projection.owner_info.is_camera_on,
            StreamType::Screen => projection.owner_info.is_screen_sharing_on,
        }
        .unwrap_or(projection.producer_active);
        tracks.push(TrackBinding {
            mid: projection.consumer_mid.clone(),
            user_id: user_id.clone(),
            stream_type,
            active,
        });
        sources.push(SourceDescriptor {
            source_id: projection.source.source_id().to_string(),
            user_id,
            stream_type,
            active,
            mid: Some(projection.consumer_mid.clone()),
            encodings: source_encodings(&projection.source),
        });
    }
    [
        ServerMessage::Tracks(tracks),
        ServerMessage::Sources(sources),
    ]
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
            policy_role: encoding.policy_role().map(|role| match role {
                UploadLayerPolicyRole::Featured => ProtocolRole::Featured,
                UploadLayerPolicyRole::Thumbnail => ProtocolRole::Thumbnail,
                UploadLayerPolicyRole::DegradedThumbnail => ProtocolRole::DegradedThumbnail,
            }),
            max_temporal_layer_id: encoding
                .max_temporal_layer_id()
                .map(SourceTemporalLayerId::as_u8),
        })
        .collect()
}
