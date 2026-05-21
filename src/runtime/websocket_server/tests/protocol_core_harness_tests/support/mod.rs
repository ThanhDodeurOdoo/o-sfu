pub(super) use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(super) use o_sfu_protocol::{
    bundle::{
        BundleBroadcastUpdate, BundleConnectionState, BundleDisconnectUpdate, BundleStateChange,
        BundleUpdate, bundle_session_info_key,
    },
    host::HostPendingRequestKind,
    wire::{
        AvailableFeatures, DownloadStates as ProtocolDownloadStates, EnvelopeBatch, RecordingState,
        RequestId, ServerMessage, StreamType as ProtocolStreamType, TrackBinding,
        UserId as ProtocolSessionId, UserInfo as ProtocolSessionInfo,
    },
};
pub(super) use o_sfu_router::MediaKind;
pub(super) use serde_json::json;
pub(super) use tokio::time::{sleep, timeout};
pub(super) use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

pub(super) use super::super::fixtures::*;
pub(super) use crate::{config::RuntimeFeatureFlags, runtime::room::Room};

mod assertions;
mod frames;
mod peer;
mod recording;
mod routes;
mod rtc;
mod scenarios;

pub(super) use assertions::{
    assert_track_snapshot_contains, consume_peer_info_update, consume_peer_joined_update,
    peer_reached_state,
};
pub(super) use frames::{
    no_server_frame, read_single_protocol_server_message, read_track_snapshot,
};
pub(super) use routes::{
    RealRtcRouteActivity, assert_real_rtc_subscribe_activity, real_rtc_route_activity,
    sample_video_rtp_parameters,
};
pub(super) use rtc::reduced_capability_rtc;
pub(super) use scenarios::{
    bob_update_info_and_deliver, close_peer_and_observe_recovery,
    close_peer_and_wait_for_room_cleanup, consume_camera_publish_bootstrap,
    publish_camera_and_bootstrap_subscriber, recover_peer_with_latest_info,
    recover_publisher_and_replay_camera_publish, recover_subscriber_and_replay_track,
    setup_protocol_recovery_peers, setup_real_rtc_protocol_peers,
};

pub(super) use self::{
    peer::ProtocolHarnessPeer,
    recording::{assert_recording_request_rejected, connect_protocol_recording_peer},
};

pub(super) const BATCH_FLUSH_DELAY_MS: u32 = 100;
pub(super) const RECOVERY_DELAY_MS: u32 = 1_000;
