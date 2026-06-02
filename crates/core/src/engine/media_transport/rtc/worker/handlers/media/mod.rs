//! Worker-local media mutation boundary

mod control;
mod keyframe;
mod lifecycle;
#[cfg(test)]
mod tests;
mod types;

#[cfg(test)]
use control::observe_source_rid_readiness;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use control::worker_set_consumer_packet_gates_for_benchmark;
pub(super) use control::{
    apply_route_control_request, remove_source_route, respond_set_consumer_packet_gates,
};
pub(in crate::engine::media_transport::rtc) use control::{
    apply_source_rid_readiness, drain_due_rid_keyframe_refreshes,
};
pub(in crate::engine::media_transport::rtc) use keyframe::{
    KeyframeRequestMode, KeyframeRequestTarget, request_keyframe_for_target,
};
pub(super) use lifecycle::{
    RecvMediaPolicy, worker_add_recv_media, worker_add_send_media, worker_remove_media,
};
pub(super) use types::AddSendMediaRequest;
