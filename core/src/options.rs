use std::net::IpAddr;

use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct MediaCodecSet: u16 {
        const OPUS = 1 << 0;
        const PCMU = 1 << 1;
        const PCMA = 1 << 2;
        const VP8 = 1 << 3;
        const H264 = 1 << 4;
        const H265 = 1 << 5;
        const VP9 = 1 << 6;
        const AV1 = 1 << 7;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoreOptions {
    pub media: MediaOptions,
    pub routing: RoutingOptions,
    pub codecs: CodecOptions,
    pub observability: ObservabilityOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaOptions {
    pub public_ip: IpAddr,
    pub rtc_port_range: RtcPortRange,
    pub bitrate_limits: SessionBitrateLimits,
    pub video_bitrate_limits: VideoBitrateLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingOptions {
    pub media_worker_count: usize,
    /// Room-local routing policy used by the room runtime.
    ///
    /// This is a cold-path control-plane setting. It decides how many local
    /// router placements a new room may reserve when the room is created. It
    /// does not participate in packet forwarding and it does not change the
    /// transport worker count after startup.
    pub room_sharding_policy: RoomShardingPolicy,
}

/// Same-room placement policy for local router spillover.
///
/// The policy is part of the public core configuration surface because server
/// startup has to choose the room topology model before any room exists. It
/// describes how many process-local router placements a room may use and which
/// spillover mode should interpret that limit.
///
/// `RoomShardingPolicy` belongs to room orchestration, not to the RTP packet
/// loop. The room factory reads it once when reserving router and media-worker
/// placements for a new room. Room state then uses the same policy to decide
/// whether a user connection can be placed on a spillover router.
///
/// # Invariants
///
/// `max_local_routers()` never returns zero. Constructors accept raw values so
/// outer config layers can normalize or validate operator input in one place,
/// while core callers still get a safe fallback if a policy is built directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomShardingPolicy {
    max_local_routers: usize,
    spillover: RoomSpilloverMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalSpilloverPolicy {
    /// Joined receiver count that is high enough to start spillover pressure.
    min_receiver_count: usize,
    /// Active and pending consumer routes allowed per active local router.
    max_active_consumers_per_router: usize,
    /// Receiver fan-out allowed for one published source before it is pressured.
    max_fanout_per_source: usize,
    /// Aggregate transport egress bitrate threshold, in bits per second.
    egress_bitrate_threshold_bps: u64,
    /// Packet-loop scheduling lag threshold, in milliseconds.
    packet_loop_lag_threshold_ms: u64,
    /// Queued transport command depth that indicates control-path pressure.
    command_backlog_threshold: usize,
    /// Relay mailbox depth that indicates cross-worker forwarding pressure.
    relay_mailbox_depth_threshold: usize,
    /// Worker pressure score threshold on a 0 to 100 saturation scale.
    worker_pressure_threshold: u8,
    /// Consecutive pressured observations required before attaching capacity.
    activation_window: usize,
    /// Consecutive idle cleanup observations required before draining capacity.
    cooldown_window: usize,
}

/// How a room interprets its reserved local router placements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomSpilloverMode {
    /// Keep all users, producers and consumers on the room's primary router.
    ///
    /// This is the default deployment mode. It preserves the historical
    /// topology shape even when the process has multiple RTC media workers.
    StrictSingleRouter,
    /// Allow the room runtime to use the pre-reserved local router set.
    ///
    /// Placement is deterministic and bounded by `max_local_routers`. It is not
    /// an adaptive load-triggered policy; adaptive thresholds will need their own
    /// measured inputs before they become public API.
    BoundedLocalSpillover,
    /// Keep small rooms on the primary router and attach local capacity only
    /// after measured pressure crosses the configured policy thresholds.
    LoadTriggeredLocalSpillover(LocalSpilloverPolicy),
}

impl RoutingOptions {
    #[must_use]
    pub const fn new(media_worker_count: usize) -> Self {
        Self {
            media_worker_count,
            room_sharding_policy: RoomShardingPolicy::strict_single_router(),
        }
    }
}

impl RoomShardingPolicy {
    /// Build the default policy that keeps every room on one local router.
    ///
    /// Use this unless the runtime has explicitly opted into same-room
    /// spillover. It keeps the room topology identical to the historical
    /// single-router model and is safe with any positive media-worker count.
    #[must_use]
    pub const fn strict_single_router() -> Self {
        Self {
            max_local_routers: 1,
            spillover: RoomSpilloverMode::StrictSingleRouter,
        }
    }

    /// Build a policy that may place one room across several local routers.
    ///
    /// `max_local_routers` is an upper bound for one room. The runtime config
    /// layer must keep it less than or equal to the RTC media worker count so
    /// every reserved router has a worker placement. If a caller passes zero,
    /// [`Self::max_local_routers`] normalizes it to one.
    ///
    /// This constructor does not allocate routers. It only records the policy
    /// consumed by room creation and topology state.
    #[must_use]
    pub const fn bounded_local_spillover(max_local_routers: usize) -> Self {
        Self {
            max_local_routers,
            spillover: RoomSpilloverMode::BoundedLocalSpillover,
        }
    }

    /// Build the production same-room spillover policy.
    ///
    /// `max_local_routers` is still only an upper bound. Rooms start on their
    /// primary placement and attach additional local placements when the
    /// provided load policy reports sustained pressure.
    #[must_use]
    pub const fn load_triggered_local_spillover(
        max_local_routers: usize,
        policy: LocalSpilloverPolicy,
    ) -> Self {
        Self {
            max_local_routers,
            spillover: RoomSpilloverMode::LoadTriggeredLocalSpillover(policy),
        }
    }

    /// Return the non-zero local router cap for one room.
    ///
    /// The cap is the number of room-local router placements the runtime may
    /// reserve, not a count of currently attached routers. Spillover routers
    /// can stay detached until a user is placed on them.
    #[must_use]
    pub const fn max_local_routers(self) -> usize {
        if self.max_local_routers == 0 {
            1
        } else {
            self.max_local_routers
        }
    }

    /// Return the spillover mode that interprets this policy.
    ///
    /// Callers should branch on this value instead of treating
    /// `max_local_routers() == 1` as the only strict-mode signal. That keeps
    /// the policy open to future modes that may also use one router at a time.
    #[must_use]
    pub const fn spillover(self) -> RoomSpilloverMode {
        self.spillover
    }

    /// Return how many reserved local placements may receive home sessions.
    ///
    /// Strict mode always uses the primary placement. Bounded spillover uses the
    /// configured cap, limited by how many placements the room factory reserved.
    #[must_use]
    pub fn allowed_local_router_count(self, reserved_local_routers: usize) -> usize {
        match self.spillover {
            RoomSpilloverMode::BoundedLocalSpillover => {
                self.max_local_routers().min(reserved_local_routers).max(1)
            }
            RoomSpilloverMode::StrictSingleRouter
            | RoomSpilloverMode::LoadTriggeredLocalSpillover(_) => 1,
        }
    }
}

impl Default for RoomShardingPolicy {
    fn default() -> Self {
        Self::strict_single_router()
    }
}

impl LocalSpilloverPolicy {
    pub const DEFAULT_MIN_RECEIVER_COUNT: usize = 16;
    pub const DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER: usize = 64;
    pub const DEFAULT_MAX_FANOUT_PER_SOURCE: usize = 48;
    pub const DEFAULT_EGRESS_BITRATE_THRESHOLD_BPS: u64 = 750_000_000;
    pub const DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS: u64 = 20;
    pub const DEFAULT_COMMAND_BACKLOG_THRESHOLD: usize = 128;
    pub const DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD: usize = 128;
    pub const DEFAULT_WORKER_PRESSURE_THRESHOLD: u8 = 80;
    pub const DEFAULT_ACTIVATION_WINDOW: usize = 2;
    pub const DEFAULT_COOLDOWN_WINDOW: usize = 4;

    /// Build the default conservative threshold set.
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            min_receiver_count: Self::DEFAULT_MIN_RECEIVER_COUNT,
            max_active_consumers_per_router: Self::DEFAULT_MAX_ACTIVE_CONSUMERS_PER_ROUTER,
            max_fanout_per_source: Self::DEFAULT_MAX_FANOUT_PER_SOURCE,
            egress_bitrate_threshold_bps: Self::DEFAULT_EGRESS_BITRATE_THRESHOLD_BPS,
            packet_loop_lag_threshold_ms: Self::DEFAULT_PACKET_LOOP_LAG_THRESHOLD_MS,
            command_backlog_threshold: Self::DEFAULT_COMMAND_BACKLOG_THRESHOLD,
            relay_mailbox_depth_threshold: Self::DEFAULT_RELAY_MAILBOX_DEPTH_THRESHOLD,
            worker_pressure_threshold: Self::DEFAULT_WORKER_PRESSURE_THRESHOLD,
            activation_window: Self::DEFAULT_ACTIVATION_WINDOW,
            cooldown_window: Self::DEFAULT_COOLDOWN_WINDOW,
        }
    }

    #[must_use]
    pub const fn with_min_receiver_count(mut self, value: usize) -> Self {
        self.min_receiver_count = value;
        self
    }

    #[must_use]
    pub const fn with_max_active_consumers_per_router(mut self, value: usize) -> Self {
        self.max_active_consumers_per_router = value;
        self
    }

    #[must_use]
    pub const fn with_max_fanout_per_source(mut self, value: usize) -> Self {
        self.max_fanout_per_source = value;
        self
    }

    #[must_use]
    pub const fn with_egress_bitrate_threshold_bps(mut self, value: u64) -> Self {
        self.egress_bitrate_threshold_bps = value;
        self
    }

    #[must_use]
    pub const fn with_packet_loop_lag_threshold_ms(mut self, value: u64) -> Self {
        self.packet_loop_lag_threshold_ms = value;
        self
    }

    #[must_use]
    pub const fn with_command_backlog_threshold(mut self, value: usize) -> Self {
        self.command_backlog_threshold = value;
        self
    }

    #[must_use]
    pub const fn with_relay_mailbox_depth_threshold(mut self, value: usize) -> Self {
        self.relay_mailbox_depth_threshold = value;
        self
    }

    #[must_use]
    pub const fn with_worker_pressure_threshold(mut self, value: u8) -> Self {
        self.worker_pressure_threshold = value;
        self
    }

    #[must_use]
    pub const fn with_activation_window(mut self, value: usize) -> Self {
        self.activation_window = value;
        self
    }

    #[must_use]
    pub const fn with_cooldown_window(mut self, value: usize) -> Self {
        self.cooldown_window = value;
        self
    }

    #[must_use]
    pub const fn min_receiver_count(self) -> usize {
        self.min_receiver_count
    }

    #[must_use]
    pub const fn max_active_consumers_per_router(self) -> usize {
        if self.max_active_consumers_per_router == 0 {
            1
        } else {
            self.max_active_consumers_per_router
        }
    }

    #[must_use]
    pub const fn max_fanout_per_source(self) -> usize {
        if self.max_fanout_per_source == 0 {
            1
        } else {
            self.max_fanout_per_source
        }
    }

    #[must_use]
    pub const fn egress_bitrate_threshold_bps(self) -> u64 {
        self.egress_bitrate_threshold_bps
    }

    #[must_use]
    pub const fn packet_loop_lag_threshold_ms(self) -> u64 {
        self.packet_loop_lag_threshold_ms
    }

    #[must_use]
    pub const fn command_backlog_threshold(self) -> usize {
        self.command_backlog_threshold
    }

    #[must_use]
    pub const fn relay_mailbox_depth_threshold(self) -> usize {
        self.relay_mailbox_depth_threshold
    }

    #[must_use]
    pub const fn worker_pressure_threshold(self) -> u8 {
        self.worker_pressure_threshold
    }

    #[must_use]
    pub const fn activation_window(self) -> usize {
        if self.activation_window == 0 {
            1
        } else {
            self.activation_window
        }
    }

    #[must_use]
    pub const fn cooldown_window(self) -> usize {
        if self.cooldown_window == 0 {
            1
        } else {
            self.cooldown_window
        }
    }
}

impl Default for LocalSpilloverPolicy {
    fn default() -> Self {
        Self::conservative()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecOptions {
    pub flags: MediaCodecFlags,
    pub preferences: CodecPreferences,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservabilityOptions {
    pub transport_diagnostics_enabled: bool,
    pub transport_metrics_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeFeatureFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBitrateLimits {
    max_bitrate_in_bps: u64,
    max_bitrate_out_bps: u64,
}

impl SessionBitrateLimits {
    #[must_use]
    pub const fn new(max_bitrate_in_bps: u64, max_bitrate_out_bps: u64) -> Self {
        Self {
            max_bitrate_in_bps,
            max_bitrate_out_bps,
        }
    }

    #[must_use]
    pub const fn max_bitrate_in_bps(&self) -> u64 {
        self.max_bitrate_in_bps
    }

    #[must_use]
    pub const fn max_bitrate_out_bps(&self) -> u64 {
        self.max_bitrate_out_bps
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoBitrateLimits {
    max_video_bitrate_bps: u64,
}

impl VideoBitrateLimits {
    pub const DEFAULT_MAX_VIDEO_BITRATE_BPS: u64 = 4_000_000;

    #[must_use]
    pub const fn new(max_video_bitrate_bps: u64) -> Self {
        Self {
            max_video_bitrate_bps,
        }
    }

    #[must_use]
    pub const fn max_video_bitrate_bps(self) -> u64 {
        self.max_video_bitrate_bps
    }
}

impl Default for VideoBitrateLimits {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_VIDEO_BITRATE_BPS)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: MediaCodecSet,
}

macro_rules! media_codec_accessors {
    ($($enabled:ident => $with:ident => $flag:ident),+ $(,)?) => {
        $(
            #[must_use]
            pub fn $enabled(self) -> bool {
                self.enabled.contains(MediaCodecSet::$flag)
            }

            #[must_use]
            pub fn $with(self, enabled: bool) -> Self {
                self.with_flag(MediaCodecSet::$flag, enabled)
            }
        )+
    };
}

impl MediaCodecFlags {
    #[must_use]
    fn with_flag(mut self, flag: MediaCodecSet, enabled: bool) -> Self {
        if enabled {
            self.enabled.insert(flag);
        } else {
            self.enabled.remove(flag);
        }
        self
    }

    media_codec_accessors!(
        opus_enabled => with_opus => OPUS,
        pcmu_enabled => with_pcmu => PCMU,
        pcma_enabled => with_pcma => PCMA,
        vp8_enabled => with_vp8 => VP8,
        h264_enabled => with_h264 => H264,
        h265_enabled => with_h265 => H265,
        vp9_enabled => with_vp9 => VP9,
        av1_enabled => with_av1 => AV1,
    );
}

impl Default for MediaCodecFlags {
    fn default() -> Self {
        Self {
            enabled: MediaCodecSet::OPUS | MediaCodecSet::VP8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodecPreference {
    Opus,
    Pcmu,
    Pcma,
}

impl AudioCodecPreference {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::Pcmu => "PCMU",
            Self::Pcma => "PCMA",
        }
    }

    #[must_use]
    pub fn enabled_by(self, flags: MediaCodecFlags) -> bool {
        match self {
            Self::Opus => flags.opus_enabled(),
            Self::Pcmu => flags.pcmu_enabled(),
            Self::Pcma => flags.pcma_enabled(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodecPreference {
    Vp8,
    H264,
    H265,
    Vp9,
    Av1,
}

impl VideoCodecPreference {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Vp8 => "VP8",
            Self::H264 => "H264",
            Self::H265 => "H265",
            Self::Vp9 => "VP9",
            Self::Av1 => "AV1",
        }
    }

    #[must_use]
    pub fn enabled_by(self, flags: MediaCodecFlags) -> bool {
        match self {
            Self::Vp8 => flags.vp8_enabled(),
            Self::H264 => flags.h264_enabled(),
            Self::H265 => flags.h265_enabled(),
            Self::Vp9 => flags.vp9_enabled(),
            Self::Av1 => flags.av1_enabled(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecPreferences {
    audio: [AudioCodecPreference; 3],
    video: [VideoCodecPreference; 5],
}

impl CodecPreferences {
    pub const DEFAULT_AUDIO: [AudioCodecPreference; 3] = [
        AudioCodecPreference::Opus,
        AudioCodecPreference::Pcmu,
        AudioCodecPreference::Pcma,
    ];
    pub const DEFAULT_VIDEO: [VideoCodecPreference; 5] = [
        VideoCodecPreference::Vp8,
        VideoCodecPreference::H264,
        VideoCodecPreference::H265,
        VideoCodecPreference::Vp9,
        VideoCodecPreference::Av1,
    ];

    #[must_use]
    pub const fn new(audio: [AudioCodecPreference; 3], video: [VideoCodecPreference; 5]) -> Self {
        Self { audio, video }
    }

    #[must_use]
    pub fn with_audio_order(self, preferred: &[AudioCodecPreference]) -> Self {
        Self {
            audio: complete_audio_order(preferred),
            ..self
        }
    }

    #[must_use]
    pub fn with_video_order(self, preferred: &[VideoCodecPreference]) -> Self {
        Self {
            video: complete_video_order(preferred),
            ..self
        }
    }

    #[must_use]
    pub const fn audio_order(self) -> [AudioCodecPreference; 3] {
        self.audio
    }

    #[must_use]
    pub const fn video_order(self) -> [VideoCodecPreference; 5] {
        self.video
    }
}

impl Default for CodecPreferences {
    fn default() -> Self {
        Self::new(Self::DEFAULT_AUDIO, Self::DEFAULT_VIDEO)
    }
}

fn complete_audio_order(preferred: &[AudioCodecPreference]) -> [AudioCodecPreference; 3] {
    let mut output = CodecPreferences::DEFAULT_AUDIO;
    let mut len = 0;
    for codec in preferred
        .iter()
        .copied()
        .chain(CodecPreferences::DEFAULT_AUDIO)
    {
        if contains_audio_codec(output, len, codec) {
            continue;
        }
        if let Some(slot) = output.get_mut(len) {
            *slot = codec;
            len += 1;
        }
    }
    output
}

fn complete_video_order(preferred: &[VideoCodecPreference]) -> [VideoCodecPreference; 5] {
    let mut output = CodecPreferences::DEFAULT_VIDEO;
    let mut len = 0;
    for codec in preferred
        .iter()
        .copied()
        .chain(CodecPreferences::DEFAULT_VIDEO)
    {
        if contains_video_codec(output, len, codec) {
            continue;
        }
        if let Some(slot) = output.get_mut(len) {
            *slot = codec;
            len += 1;
        }
    }
    output
}

fn contains_audio_codec(
    codecs: [AudioCodecPreference; 3],
    len: usize,
    needle: AudioCodecPreference,
) -> bool {
    codecs.into_iter().take(len).any(|codec| codec == needle)
}

fn contains_video_codec(
    codecs: [VideoCodecPreference; 5],
    len: usize,
    needle: VideoCodecPreference,
) -> bool {
    codecs.into_iter().take(len).any(|codec| codec == needle)
}

impl CoreOptions {
    #[must_use]
    pub const fn new(
        media: MediaOptions,
        routing: RoutingOptions,
        codecs: CodecOptions,
        observability: ObservabilityOptions,
    ) -> Self {
        Self {
            media,
            routing,
            codecs,
            observability,
        }
    }
}
