use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, StreamType, UserInfo};

use super::UserError;
use crate::{
    application::stream_catalog::stream_id_for_stream_type,
    core::prelude::{SfuCoreError, SourceSubscriptionIntent, UserStreamId},
};

/// Project compatibility download state into core subscription intent.
///
/// Missing stream entries mean "leave that stream unchanged" for the room.
/// Present media or layout values become generic per-stream intents keyed by
/// [`UserStreamId`].
pub(super) fn subscription_intents_from_download_states(
    states: &DownloadStates,
) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    let mut intents = BTreeMap::new();
    let streams = [
        (StreamType::Audio, states.audio, None),
        (StreamType::Camera, states.camera, states.camera_layout),
        (StreamType::Screen, states.screen, states.screen_layout),
    ];
    for (stream_type, media, layout) in streams {
        if media.is_some() || layout.is_some() {
            intents.insert(
                stream_id_for_stream_type(stream_type),
                SourceSubscriptionIntent::new(media, layout),
            );
        }
    }
    intents
}

/// Build the user-info delta implied by a publication activity change.
///
/// Audio has no Odoo-visible user-info flag. Camera and screen publication
/// activity must mirror into presence so existing Discuss clients keep their
/// toolbar state in sync with negotiated media.
pub(super) fn publication_info_update(stream_type: StreamType, active: bool) -> Option<UserInfo> {
    match stream_type {
        StreamType::Audio => None,
        StreamType::Camera => Some(UserInfo {
            is_camera_on: Some(active),
            ..UserInfo::default()
        }),
        StreamType::Screen => Some(UserInfo {
            is_screen_sharing_on: Some(active),
            ..UserInfo::default()
        }),
    }
}

/// Collapse media-core negotiation errors into websocket-session errors.
///
/// Transport failures are server-side failures. Capability projection failures
/// and room rejections make the browser answer unusable for this session, so
/// callers close the socket as a protocol failure.
pub(super) fn map_core_negotiation_error(error: SfuCoreError) -> UserError {
    if error.is_client_negotiation_error() {
        UserError::ProtocolViolation
    } else {
        UserError::InternalError
    }
}
