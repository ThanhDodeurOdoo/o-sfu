use std::{fmt, net::IpAddr};

use crate::Bitrate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaOptions {
    pub announced_ip: IpAddr,
    pub rtc_port_range: RtcPortRange,
    pub rtc_udp_io_backend: RtcUdpIoBackend,
    pub bitrate_limits: SessionBitrateLimits,
    pub video_bitrate_limits: VideoBitrateLimits,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RtcUdpIoBackend {
    #[default]
    Tokio,
    IoUring,
}

impl RtcUdpIoBackend {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Tokio => "tokio",
            Self::IoUring => "io_uring",
        }
    }
}

impl fmt::Display for RtcUdpIoBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.wire_name())
    }
}

/// Room media activation limits.
///
/// These limits control receiver delivery. They do not erase publication state
/// or user subscription intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomMediaLimits {
    max_active_audio_speakers: usize,
    max_video_downloads_per_receiver: usize,
}

/// Invalid room media limit input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomMediaLimitsError {
    #[error("maximum active audio speakers must be greater than zero")]
    MaxActiveAudioSpeakersZero,
    #[error("maximum video downloads per receiver must be greater than zero")]
    MaxVideoDownloadsPerReceiverZero,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtcPortRange {
    min: u16,
    max: u16,
}

impl RtcPortRange {
    #[must_use]
    pub const fn new(min: u16, max: u16) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub const fn min(self) -> u16 {
        self.min
    }

    #[must_use]
    pub const fn max(self) -> u16 {
        self.max
    }

    #[must_use]
    pub const fn port_count(self) -> u16 {
        self.max - self.min + 1
    }

    pub fn ports(self) -> impl Iterator<Item = u16> {
        self.min..=self.max
    }

    #[must_use]
    pub fn split_for_workers(self, worker_count: usize) -> Option<Vec<Self>> {
        if worker_count == 0 || worker_count > usize::from(self.port_count()) {
            return None;
        }
        let total_ports = usize::from(self.port_count());
        let base_ports_per_worker = total_ports / worker_count;
        let extra_ports = total_ports % worker_count;
        let mut next_min = u32::from(self.min);
        let mut ranges = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let worker_port_count = base_ports_per_worker + usize::from(worker_idx < extra_ports);
            let worker_port_count = u32::try_from(worker_port_count).ok()?;
            let max_inclusive = next_min + worker_port_count - 1;
            ranges.push(Self::new(
                u16::try_from(next_min).ok()?,
                u16::try_from(max_inclusive).ok()?,
            ));
            next_min = max_inclusive + 1;
        }
        Some(ranges)
    }
}

impl RoomMediaLimits {
    pub const DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS: usize = 4;
    pub const DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER: usize = 10;

    /// Build room media limits after validating their invariants.
    ///
    /// # Errors
    ///
    /// Returns [`RoomMediaLimitsError`] when a limit is zero.
    pub const fn try_new(
        max_active_audio_speakers: usize,
        max_video_downloads_per_receiver: usize,
    ) -> Result<Self, RoomMediaLimitsError> {
        if max_active_audio_speakers == 0 {
            return Err(RoomMediaLimitsError::MaxActiveAudioSpeakersZero);
        }
        if max_video_downloads_per_receiver == 0 {
            return Err(RoomMediaLimitsError::MaxVideoDownloadsPerReceiverZero);
        }
        Ok(Self {
            max_active_audio_speakers,
            max_video_downloads_per_receiver,
        })
    }

    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            max_active_audio_speakers: Self::DEFAULT_MAX_ACTIVE_AUDIO_SPEAKERS,
            max_video_downloads_per_receiver: Self::DEFAULT_MAX_VIDEO_DOWNLOADS_PER_RECEIVER,
        }
    }

    #[must_use]
    pub const fn max_active_audio_speakers(self) -> usize {
        self.max_active_audio_speakers
    }

    #[must_use]
    pub const fn max_video_downloads_per_receiver(self) -> usize {
        self.max_video_downloads_per_receiver
    }
}

impl Default for RoomMediaLimits {
    fn default() -> Self {
        Self::conservative()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBitrateLimits {
    max_bitrate_in: Bitrate,
    max_bitrate_out: Bitrate,
}

impl SessionBitrateLimits {
    #[must_use]
    pub const fn new(max_bitrate_in: Bitrate, max_bitrate_out: Bitrate) -> Self {
        Self {
            max_bitrate_in,
            max_bitrate_out,
        }
    }

    #[must_use]
    pub const fn max_bitrate_in(self) -> Bitrate {
        self.max_bitrate_in
    }

    #[must_use]
    pub const fn max_bitrate_out(self) -> Bitrate {
        self.max_bitrate_out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoBitrateLimits {
    max_video_bitrate: Bitrate,
}

impl VideoBitrateLimits {
    pub const DEFAULT_MAX_VIDEO_BITRATE: Bitrate = Bitrate::from_mbps(4);

    #[must_use]
    pub const fn new(max_video_bitrate: Bitrate) -> Self {
        Self { max_video_bitrate }
    }

    #[must_use]
    pub const fn max_video_bitrate(self) -> Bitrate {
        self.max_video_bitrate
    }
}

impl Default for VideoBitrateLimits {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_VIDEO_BITRATE)
    }
}
