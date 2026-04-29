//! Worker-local media mutation boundary

mod control;
mod keyframe;
mod lifecycle;
#[cfg(test)]
mod tests;
mod types;

pub(in crate::runtime::rtc_adapter) use control::{
    drain_due_rid_keyframe_refreshes, observe_source_rid_readiness,
};
pub(super) use control::{
    refresh_source_packet_gate, respond_request_consumer_keyframe, respond_set_consumer_active,
    respond_set_consumer_packet_gate, respond_set_consumer_packet_gates,
    respond_set_producer_active, respond_set_remote_source_packet_gate,
    respond_set_remote_source_route_active,
};
pub use keyframe::request_keyframe_for_source;
pub(super) use keyframe::respond_request_remote_keyframe;
pub(super) use lifecycle::{
    RecvMediaPolicy, respond_add_recv_media, respond_add_send_media, respond_remove_media,
    respond_resolve_media_mid,
};
pub(super) use types::{AddSendMediaRequest, RemoteKeyframeRequest};
