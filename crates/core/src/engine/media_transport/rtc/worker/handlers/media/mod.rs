//! Worker-local media mutation boundary

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod control;
mod keyframe;
mod lifecycle;
mod types;

#[cfg(test)]
use control::observe_src_rid_ready;
#[cfg(feature = "internal-benchmarks")]
pub use control::worker_set_consumer_pkt_gates_for_bench;
pub(super) use control::{
    apply_route_control_request, remove_source_route, respond_set_consumer_pkt_gates,
};
pub use control::{apply_src_rid_ready, drain_due_rid_kf_refreshes};
pub use keyframe::{KeyframeRequestMode, KeyframeRequestTarget, request_kf_for_target};
pub(super) use lifecycle::{
    RecvMediaPolicy, worker_add_recv_media, worker_add_send_media, worker_remove_media,
};
pub(super) use types::AddSendMediaRequest;
