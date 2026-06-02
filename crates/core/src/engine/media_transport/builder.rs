//! media transport construction
//!
//! owns the builder and startup validation that protect worker construction
//! from invalid worker or port-range topology

use thiserror::Error;

use super::{
    MediaTransport,
    config::{MediaTransportConfig, MediaTransportDeps},
};
use crate::CoreOptions;

impl MediaTransport {
    /// Starts named media transport construction.
    ///
    /// The builder validates cold-path topology choices such as worker count
    /// and UDP port splitting before the first worker is created.
    #[must_use]
    pub const fn builder() -> MediaTransportBuilder {
        MediaTransportBuilder::new()
    }

    /// Builds the runtime media transport from neutral core options and process
    /// dependencies.
    ///
    /// This is the production server construction path. It returns the same
    /// transport handle the runtime uses everywhere else, so there is no second
    /// RTC-specific wrapper to reason about.
    ///
    /// # Errors
    ///
    /// Returns [`MediaTransportBuildError`] when the derived transport cannot be
    /// built from the supplied options and dependencies.
    pub fn from_core_options(
        options: &CoreOptions,
        deps: MediaTransportDeps,
    ) -> Result<Self, MediaTransportBuildError> {
        Self::builder().core_options(options).deps(deps).build()
    }
}

/// Named construction input for the media transport.
///
/// Building the RTC transport needs operator policy, process services and
/// worker topology. The builder keeps those inputs named so the runtime does
/// not have to assemble positional worker plumbing or know that one shared
/// source-policy signal will be installed into every worker.
///
/// # Validation
///
/// `worker_count` defaults to one. `build` rejects zero workers and rejects
/// worker counts that cannot receive at least one UDP port from the configured
/// range.
#[derive(Debug, Clone)]
pub struct MediaTransportBuilder {
    transport: Option<MediaTransportConfig>,
    deps: Option<MediaTransportDeps>,
    worker_count: usize,
}

impl MediaTransportBuilder {
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
        self.transport = Some(MediaTransportConfig::from_core_options(options));
        self.worker_count = options.routing.media_worker_count;
        self
    }

    /// Provides an already assembled RTC transport config.
    ///
    /// This is mainly useful for targeted tests that need a narrow port range
    /// or codec policy without constructing a full server config.
    #[must_use]
    pub fn transport_config(mut self, config: MediaTransportConfig) -> Self {
        self.transport = Some(config);
        self
    }

    /// Provides the shared process services used by every RTC worker.
    #[must_use]
    pub fn deps(mut self, deps: MediaTransportDeps) -> Self {
        self.deps = Some(deps);
        self
    }

    /// Selects how many RTC workers the transport should create.
    ///
    /// The value is validated by [`Self::build`]. Supplying zero or more
    /// workers than available UDP ports is a construction error.
    #[must_use]
    pub const fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    /// Creates the media transport and validates worker topology.
    ///
    /// The method is cold-path only. It allocates worker state, creates one
    /// shared source-policy signal for the transport workers and does no packet-loop
    /// work by itself.
    ///
    /// # Errors
    ///
    /// Returns [`MediaTransportBuildError`] when transport config or dependency
    /// inputs are missing or when worker placement cannot fit the port range.
    pub fn build(self) -> Result<MediaTransport, MediaTransportBuildError> {
        let transport = self
            .transport
            .ok_or(MediaTransportBuildError::MissingTransportConfig)?;
        let deps = self.deps.ok_or(MediaTransportBuildError::MissingDeps)?;
        let worker_ranges = split_worker_ranges(transport.rtc_port_range, self.worker_count)?;
        Ok(MediaTransport::new(&transport, &deps, worker_ranges))
    }
}

impl Default for MediaTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid construction inputs for the media transport.
///
/// These errors are configuration failures. They should surface during startup
/// or test fixture creation before any media session is admitted.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum MediaTransportBuildError {
    /// The caller did not provide media transport policy.
    #[error("media transport configuration is missing")]
    MissingTransportConfig,
    /// The caller did not provide the shared diagnostics, metrics and
    /// packet-sink services needed by the transport.
    #[error("media transport dependencies are missing")]
    MissingDeps,
    /// A transport cannot be built without at least one RTC worker.
    #[error("media transport worker count must be at least one")]
    InvalidWorkerCount,
    /// The configured UDP range cannot be split so every requested worker owns
    /// at least one port.
    #[error(
        "media transport cannot split {port_count} UDP ports across {worker_count} media workers"
    )]
    InvalidPortSplit {
        worker_count: usize,
        port_count: u16,
    },
}

fn split_worker_ranges(
    rtc_port_range: crate::RtcPortRange,
    worker_count: usize,
) -> Result<Vec<crate::RtcPortRange>, MediaTransportBuildError> {
    if worker_count == 0 {
        return Err(MediaTransportBuildError::InvalidWorkerCount);
    }
    rtc_port_range.split_for_workers(worker_count).ok_or(
        MediaTransportBuildError::InvalidPortSplit {
            worker_count,
            port_count: rtc_port_range.port_count(),
        },
    )
}
