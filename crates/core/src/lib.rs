//! room state, routing and media transport orchestration
//!
//! `o-sfu-core` is the boundary between the production server crate and the
//! lower pure or protocol-specific crates
//! it hqndle room admission, user sessions, media publication state and the
//! transport facade that hides RTC worker topology from higher layers
//!
//! the public surface is split in two:
//!
//! - `prelude` contains application-facing value types and the `SfuCore` facade
//! - `server` contains construction, diagnostics, metrics and room integration
//!   types used by the runtime crate
//!
//! typical server construction gives transport-owned configuration plus shared
//! process services directly to `server::transport::MediaTransport`
//!
//! ```no_run
//! use std::{
//!     net::{IpAddr, Ipv4Addr},
//!     sync::Arc,
//! };
//!
//! use o_sfu_core::{
//!     prelude::{
//!         Bitrate, CodecPreferences, MediaCodecFlags, RtcPortRange, RtcUdpIoBackend,
//!         SessionBitrateLimits, VideoBitrateLimits,
//!     },
//!     server::{
//!         metrics::RuntimeMetrics,
//!         packet_sinks::RoomPacketSinkRegistry,
//!         transport::{MediaTransport, MediaTransportConfig, MediaTransportDeps},
//!     },
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MediaTransportConfig {
//!     worker_count: 1,
//!     announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
//!     bitrate_limits: SessionBitrateLimits::new(
//!         Bitrate::from_mbps(3),
//!         Bitrate::from_mbps(3),
//!     ),
//!     video_bitrate_limits: VideoBitrateLimits::default(),
//!     rtc_port_range: RtcPortRange::new(40_000, 40_099),
//!     rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
//!     codec_flags: MediaCodecFlags::default(),
//!     codec_preferences: CodecPreferences::default(),
//!     media_quality_interval: None,
//! };
//! let deps = MediaTransportDeps {
//!     packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
//!     metrics: Arc::new(RuntimeMetrics::default()),
//! };
//!
//! let transport = MediaTransport::build(config, deps)?;
//!
//! # let _transport = transport;
//! # Ok(())
//! # }
//! ```
//!
//! `MediaTransport::build` returns after every worker runtime has bound its UDP
//! socket. Session-local RTC state remains lazy.
//!
//! the example follows the production construction shape without opening a
//! listener or starting an async server

use std::fmt::{self, Display, Formatter};

pub use o_sfu_router::{ConnectionId, MediaWorkerId};

mod engine;
mod options;
pub mod prelude;
pub mod server;
mod sfu;

pub(crate) use options::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy,
    RtcPortRange, RtcUdpIoBackend, RuntimeFeatureFlags, SessionBitrateLimits,
    VideoAdaptationTuning, VideoBitrateLimits, VideoCodecPreference,
};

/// Media bitrate stored as bits per second (not bytes per second).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    #[must_use]
    pub const fn from_bps(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_kbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_mbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_bps(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    #[must_use]
    pub const fn divided_by(self, divisor: u64) -> Self {
        match self.0.checked_div(divisor) {
            Some(value) => Self(value),
            None => Self::zero(),
        }
    }
}

/// unique identifier for a running room instance within the server process
///
/// this separates a specific runtime allocation from the overarching application
/// room identity. it helps telemetry, logging, and underlying components distinguish
/// between consecutive lifecycles of the same room if it is torn down and recreated
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoomInstanceId(u64);

impl RoomInstanceId {
    #[must_use]
    pub fn allocate(next_room_instance_id: &mut u64) -> Self {
        let room_instance_id = Self(*next_room_instance_id);
        *next_room_instance_id = next_room_instance_id.saturating_add(1);
        room_instance_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for RoomInstanceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
