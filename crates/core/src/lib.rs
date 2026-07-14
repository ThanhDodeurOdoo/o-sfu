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
//! typical server construction gives configuration into
//! `CoreOptions`, then builds a `server::transport::MediaTransport` from those
//! options plus process services
//!
//! ```no_run
//! use std::{
//!     net::{IpAddr, Ipv4Addr},
//!     sync::Arc,
//! };
//!
//! use o_sfu_core::{
//!     prelude::{
//!         Bitrate, CodecOptions, CodecPreferences, CoreOptions, MediaCodecFlags, MediaOptions,
//!         ObservabilityOptions, RoutingOptions, RtcPortRange, RtcUdpIoBackend,
//!         SessionBitrateLimits, VideoBitrateLimits,
//!     },
//!     server::{
//!         diagnostics::DiagnosticsStore,
//!         metrics::RuntimeMetrics,
//!         packet_sinks::RoomPacketSinkRegistry,
//!         transport::{MediaTransport, MediaTransportDeps},
//!     },
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let options = CoreOptions::new(
//!     MediaOptions {
//!         announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
//!         rtc_port_range: RtcPortRange::new(40_000, 40_099),
//!         rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
//!         bitrate_limits: SessionBitrateLimits::new(
//!             Bitrate::from_mbps(3),
//!             Bitrate::from_mbps(3),
//!         ),
//!         video_bitrate_limits: VideoBitrateLimits::default(),
//!     },
//!     RoutingOptions::new(1),
//!     CodecOptions {
//!         flags: MediaCodecFlags::default(),
//!         preferences: CodecPreferences::default(),
//!     },
//!     ObservabilityOptions {
//!         transport_diagnostics_enabled: true,
//!         transport_metrics_enabled: true,
//!         media_quality_interval: None,
//!     },
//! );
//! let deps = MediaTransportDeps {
//!     diagnostics: Arc::new(DiagnosticsStore::default()),
//!     packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
//!     metrics: Arc::new(RuntimeMetrics::default()),
//! };
//!
//! let transport = MediaTransport::from_core_options(&options, deps)?;
//!
//! # let _transport = transport;
//! # Ok(())
//! # }
//! ```
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
    AudioCodecPreference, CodecPreferences, CoreOptions, LocalSpilloverPolicy, MediaCodecFlags,
    RoomMediaLimits, RoomSpilloverMode, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
    RuntimeFeatureFlags, SessionBitrateLimits, VideoBitrateLimits, VideoCodecPreference,
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
