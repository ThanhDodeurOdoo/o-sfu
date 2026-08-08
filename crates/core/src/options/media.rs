use std::fmt;

use crate::Bitrate;

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

    /// Splits a valid caller-provided range into contiguous worker ranges.
    ///
    /// Earlier workers receive one extra port when the range does not divide
    /// evenly. Callers must provide `min <= max` and exclude `0..=u16::MAX`
    /// because its 65,536-port count is not representable by `u16`. Returns
    /// `None` for zero workers or more workers than ports.
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
pub struct VideoAdaptationTuning {
    pub(crate) multiparty_scalable_video_threshold: usize,
    pub(crate) thumbnail_budget_divisor: u64,
    pub(crate) downswitch_pressure_observations: u8,
    pub(crate) upswitch_stable_observations: u8,
    pub(crate) receiver_budget_headroom_percent: u8,
    pub(crate) audio_reserve_per_speaker: Bitrate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VideoAdaptationTuningError {
    #[error("multiparty scalable video threshold must be greater than zero")]
    MultipartyScalableVideoThresholdZero,
    #[error("thumbnail budget divisor must be greater than zero")]
    ThumbnailBudgetDivisorZero,
    #[error("downswitch pressure observations must be greater than zero")]
    DownswitchPressureObservationsZero,
    #[error("upswitch stable observations must be greater than zero")]
    UpswitchStableObservationsZero,
    #[error("receiver budget headroom percent must not exceed 100")]
    ReceiverBudgetHeadroomPercentTooHigh,
}

impl VideoAdaptationTuning {
    pub const DEFAULT_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD: usize = 3;
    pub const DEFAULT_THUMBNAIL_BUDGET_DIVISOR: u64 = 2;
    pub const DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS: u8 = 2;
    pub const DEFAULT_UPSWITCH_STABLE_OBSERVATIONS: u8 = 3;
    /// Zero applies no percentage headroom. Positive values reduce the portion of
    /// each receiver estimate available to video before the separate per-route
    /// audio reserve is subtracted.
    pub const DEFAULT_RECEIVER_BUDGET_HEADROOM_PERCENT: u8 = 0;
    /// No per-speaker audio reserve by default, so the video budget is unchanged.
    /// Set to the nominal audio bitrate to reserve it for each admitted audio
    /// route a receiver consumes. The reserve reduces its video budget and
    /// contributes to its receiver BWE target. Deafened receivers reserve
    /// nothing. Zero disables both adjustments.
    pub const DEFAULT_AUDIO_RESERVE_PER_SPEAKER: Bitrate = Bitrate::zero();

    /// # Errors
    ///
    /// Returns [`VideoAdaptationTuningError`] when the scalable-video threshold,
    /// the thumbnail budget divisor or either observation knob is zero, or when
    /// the headroom percent exceeds 100.
    pub const fn try_new(
        multiparty_scalable_video_threshold: usize,
        thumbnail_budget_divisor: u64,
        downswitch_pressure_observations: u8,
        upswitch_stable_observations: u8,
        receiver_budget_headroom_percent: u8,
        audio_reserve_per_speaker: Bitrate,
    ) -> Result<Self, VideoAdaptationTuningError> {
        if multiparty_scalable_video_threshold == 0 {
            return Err(VideoAdaptationTuningError::MultipartyScalableVideoThresholdZero);
        }
        if thumbnail_budget_divisor == 0 {
            return Err(VideoAdaptationTuningError::ThumbnailBudgetDivisorZero);
        }
        if downswitch_pressure_observations == 0 {
            return Err(VideoAdaptationTuningError::DownswitchPressureObservationsZero);
        }
        if upswitch_stable_observations == 0 {
            return Err(VideoAdaptationTuningError::UpswitchStableObservationsZero);
        }
        if receiver_budget_headroom_percent > 100 {
            return Err(VideoAdaptationTuningError::ReceiverBudgetHeadroomPercentTooHigh);
        }
        Ok(Self {
            multiparty_scalable_video_threshold,
            thumbnail_budget_divisor,
            downswitch_pressure_observations,
            upswitch_stable_observations,
            receiver_budget_headroom_percent,
            audio_reserve_per_speaker,
        })
    }
}

impl Default for VideoAdaptationTuning {
    fn default() -> Self {
        Self {
            multiparty_scalable_video_threshold: Self::DEFAULT_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD,
            thumbnail_budget_divisor: Self::DEFAULT_THUMBNAIL_BUDGET_DIVISOR,
            downswitch_pressure_observations: Self::DEFAULT_DOWNSWITCH_PRESSURE_OBSERVATIONS,
            upswitch_stable_observations: Self::DEFAULT_UPSWITCH_STABLE_OBSERVATIONS,
            receiver_budget_headroom_percent: Self::DEFAULT_RECEIVER_BUDGET_HEADROOM_PERCENT,
            audio_reserve_per_speaker: Self::DEFAULT_AUDIO_RESERVE_PER_SPEAKER,
        }
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

#[cfg(test)]
#[path = "TESTS/media.rs"]
mod tests;
