//! Worker-local media mutation boundary

mod control;
mod keyframe;
mod lifecycle;
#[cfg(test)]
mod tests;
mod types;

#[cfg(test)]
use control::observe_source_rid_readiness;
pub(super) use control::{
    apply_route_control_request, refresh_source_packet_gate, remove_source_route,
    respond_set_consumer_packet_gates,
};
pub(in crate::runtime::rtc_engine) use control::{
    apply_source_rid_readiness, drain_due_rid_keyframe_refreshes,
};
pub(in crate::runtime::rtc_engine) use keyframe::request_keyframe_for_source;
pub(super) use lifecycle::{
    RecvMediaPolicy, respond_add_recv_media, respond_add_send_media, respond_remove_media,
    respond_resolve_media_mid,
};
pub(super) use types::AddSendMediaRequest;
