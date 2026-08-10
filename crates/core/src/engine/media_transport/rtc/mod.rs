//! Private WebRTC backend below [`MediaTransport`](super::MediaTransport).
//!
//! Each [`RtcWorker`] runs one packet loop for its assigned `str0m` sessions,
//! one UDP socket shared by those sessions and command plus relay mailboxes.
//! [`MediaTransport`](super::MediaTransport) routes sessions to the workers
//! named by their transport keys and coordinates relay routes between workers.
//!
//! Main boundaries:
//!
//! - [`worker`], [`commands`] and [`state`] cover worker startup, mailbox control
//!   and packet-loop state.
//! - [`bootstrap`] creates worker sockets and session-local `str0m` state.
//! - [`codec`] centralizes RTP profiles, negotiated capabilities, negotiated RID
//!   handling and codec-specific packet inspection plus rewriting.
//! - [`packet_loop`] owns UDP routing and `str0m` polling. [`demux`] provides
//!   recovery indexes and [`routing_miss`] bounds repeated fallback work while
//!   `Rtc::accepts()` remains the session authority.
//! - [`route_table`], [`source_route`], [`route_control`] and
//!   [`keyframe_tracker`] keep forwarding routes, packet gates, activity ranking
//!   and keyframe retry state.
//! - [`forwarded_packet`], [`forwarding_planner`],
//!   [`forwarding_destination`], [`local_forwarding`] and [`local_send_rewrite`]
//!   share payloads across sinks, relays and local RTC destinations. Local RTC
//!   egress projects receiver RTP identity.
//! - [`media_registry`], [`relay_registry`] and [`bitrate`] keep media handles,
//!   relay targets and bitrate observations.

use std::{sync::Arc, time::Duration};

use crate::{SessionBitrateLimits, VideoBitrateLimits};

#[cfg(test)]
#[allow(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(feature = "internal-benchmarks")]
#[path = "TESTS/benchmark_support/mod.rs"]
pub mod benchmark_support;
mod bitrate;
mod bootstrap;
mod codec;
mod commands;
mod demux;
mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
#[cfg(any(test, fuzzing))]
#[path = "TESTS/fuzz_support/mod.rs"]
pub(crate) mod fuzz_support;
mod keyframe_tracker;
mod local_forwarding;
mod local_send_rewrite;
mod media_registry;
mod packet_loop;
mod relay_registry;
mod route_control;
mod route_table;
mod routing_miss;
mod slots;
mod source_route;
mod state;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;
mod worker;

pub(super) use codec::RtpProfile;
#[cfg(any(test, fuzzing))]
pub use codec::client_rtp_capabilities_from_answer;
pub use commands::{
    RtcWorkerCommand, RtcWorkerResponse, WorkerMediaControlBatch, WorkerMediaControlBatchOutcome,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use forwarded_packet::ForwardedPacket;
pub(super) use route_control::PacketLayerGate;
pub use worker::RtcWorker;

#[derive(Clone, Debug)]
struct RtcWorkerConfig {
    bitrate_limits: SessionBitrateLimits,
    video_bitrate_limits: VideoBitrateLimits,
    profile: Arc<RtpProfile>,
    media_quality_interval: Option<Duration>,
    media_id_base: u64,
}
