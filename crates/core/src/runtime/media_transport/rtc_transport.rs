//! production RTC transport construction for the media transport boundary
//!
//! owns the concrete RTC transport handle, its builder and the startup
//! validation that protects shard construction from invalid worker or
//! port-range topology

use std::sync::Arc;

use thiserror::Error;

use super::{
    config::{MediaTransportDeps, RtcTransportConfig, RtcTransportShardSetConfig},
    shard_set::RtcTransportShardSet,
};
use crate::CoreOptions;

/// Production media transport backed by the process-local RTC shard set.
///
/// `RtcTransport` owns the actual RTC shard collection. It is a core-owned
/// implementation detail for production media, not the type the server runtime
/// should name in orchestration code. Use [`super::MediaTransport::from_core_options`]
/// at the runtime boundary unless a targeted transport test needs to construct
/// a real RTC backend directly.
///
/// Cloning this handle is cheap. Clones share the same shard set and therefore
/// the same packet loops, diagnostics state, source-policy signal and relay
/// registrations.
#[derive(Debug, Clone)]
pub struct RtcTransport {
    pub(super) shards: Arc<RtcTransportShardSet>,
}

impl RtcTransport {
    /// Starts named RTC transport construction.
    ///
    /// The builder validates cold-path topology choices such as worker count
    /// and UDP port splitting before the first shard is created.
    #[must_use]
    pub const fn builder() -> RtcTransportBuilder {
        RtcTransportBuilder::new()
    }

    /// Builds a production RTC transport from a prepared builder.
    ///
    /// This associated function exists for call sites that prefer passing the
    /// builder as one value. Normal fluent construction can call
    /// [`RtcTransportBuilder::build`] directly.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when the builder is missing required
    /// inputs or describes an invalid worker topology.
    pub fn build(builder: RtcTransportBuilder) -> Result<Self, RtcTransportBuildError> {
        builder.build()
    }

    fn from_shard_set_config(
        config: &RtcTransportShardSetConfig,
    ) -> Result<Self, RtcTransportBuildError> {
        validate_worker_split(
            config.transport_config().rtc_port_range(),
            config.worker_count(),
        )?;
        Ok(Self::from_unchecked_shard_set_config(config))
    }

    fn from_unchecked_shard_set_config(config: &RtcTransportShardSetConfig) -> Self {
        Self {
            shards: Arc::new(RtcTransportShardSet::new(config)),
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn shards(&self) -> &Arc<RtcTransportShardSet> {
        &self.shards
    }
}

/// Named construction input for the production RTC transport.
///
/// Building the RTC transport needs operator policy, process services and
/// worker topology. The builder keeps those inputs named so the runtime does
/// not have to assemble positional shard-set plumbing or know that one shared
/// source-policy signal will be installed into every shard.
///
/// # Validation
///
/// `worker_count` defaults to one. `build` rejects zero workers and rejects
/// worker counts that cannot receive at least one UDP port from the configured
/// range.
#[derive(Debug, Clone)]
pub struct RtcTransportBuilder {
    /// RTC-specific operator policy collected from runtime core options or a
    /// test fixture.
    transport: Option<RtcTransportConfig>,
    /// Process services needed by the transport while it emits diagnostics,
    /// metrics and packet-sink fanout.
    deps: Option<MediaTransportDeps>,
    /// Number of RTC shard workers to construct.
    worker_count: usize,
}

impl RtcTransportBuilder {
    /// Creates a builder with one media worker and no required inputs.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            transport: None,
            deps: None,
            worker_count: 1,
        }
    }

    /// Projects runtime core options into RTC transport policy.
    ///
    /// This is the preferred production construction path because it keeps the
    /// server runtime in terms of media transport policy rather than RTC engine
    /// internals.
    #[must_use]
    pub fn core_options(mut self, options: &CoreOptions) -> Self {
        self.transport = Some(RtcTransportConfig {
            public_ip: options.media.public_ip,
            bitrate_limits: options.media.bitrate_limits,
            video_bitrate_limits: options.media.video_bitrate_limits,
            rtc_port_range: options.media.rtc_port_range,
            codec_flags: options.codecs.flags,
            codec_preferences: options.codecs.preferences,
        });
        self.worker_count = options.routing.media_worker_count;
        self
    }

    /// Provides an already assembled RTC transport config.
    ///
    /// This is mainly useful for targeted tests that need a narrow port range
    /// or codec policy without constructing a full server config.
    #[must_use]
    pub fn transport_config(mut self, config: RtcTransportConfig) -> Self {
        self.transport = Some(config);
        self
    }

    /// Provides the shared process services used by every RTC shard.
    #[must_use]
    pub fn deps(mut self, deps: MediaTransportDeps) -> Self {
        self.deps = Some(deps);
        self
    }

    /// Selects how many RTC worker shards the transport should create.
    ///
    /// The value is validated by [`Self::build`]. Supplying zero or more
    /// workers than available UDP ports is a construction error.
    #[must_use]
    pub const fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    /// Creates the RTC transport and validates worker topology.
    ///
    /// The method is cold-path only. It allocates shard state, creates one
    /// shared source-policy signal for the shard set and does no packet-loop
    /// work by itself.
    ///
    /// # Errors
    ///
    /// Returns [`RtcTransportBuildError`] when transport config or dependency
    /// inputs are missing or when worker placement cannot fit the port range.
    pub fn build(self) -> Result<RtcTransport, RtcTransportBuildError> {
        let transport = self
            .transport
            .ok_or(RtcTransportBuildError::MissingTransportConfig)?;
        let deps = self.deps.ok_or(RtcTransportBuildError::MissingDeps)?;
        RtcTransport::from_shard_set_config(&RtcTransportShardSetConfig::new(
            transport,
            deps,
            self.worker_count,
        ))
    }
}

impl Default for RtcTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid construction inputs for the production RTC transport.
///
/// These errors are configuration failures. They should surface during startup
/// or test fixture creation before any media session is admitted.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum RtcTransportBuildError {
    /// The caller did not provide RTC transport policy.
    #[error("RTC transport configuration is missing")]
    MissingTransportConfig,
    /// The caller did not provide the shared diagnostics, metrics and
    /// packet-sink services needed by the transport.
    #[error("RTC transport dependencies are missing")]
    MissingDeps,
    /// A transport cannot be built without at least one worker shard.
    #[error("RTC transport worker count must be at least one")]
    InvalidWorkerCount,
    /// The configured UDP range cannot be split so every requested worker owns
    /// at least one port.
    #[error(
        "RTC transport cannot split {port_count} UDP ports across {worker_count} media workers"
    )]
    InvalidPortSplit {
        worker_count: usize,
        port_count: u16,
    },
}

fn validate_worker_split(
    rtc_port_range: crate::RtcPortRange,
    worker_count: usize,
) -> Result<(), RtcTransportBuildError> {
    if worker_count == 0 {
        return Err(RtcTransportBuildError::InvalidWorkerCount);
    }
    if worker_count > usize::from(rtc_port_range.port_count()) {
        return Err(RtcTransportBuildError::InvalidPortSplit {
            worker_count,
            port_count: rtc_port_range.port_count(),
        });
    }
    Ok(())
}
