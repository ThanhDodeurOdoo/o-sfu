//! codec policy shared by server config and RTC transport construction
//!
//! [`CodecOptions`] is part of the cold-path [`CoreOptions`](super::CoreOptions)
//! projection because server configuration parses operator input into these
//! value types, then media transport bootstrap reads them when it creates str0m RTC
//! state for a session, packet forwarding reads the negotiated RTP state later,
//! so this module only controls the capability surface advertised during
//! negotiation

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

/// codec policy consumed by media transport startup
///
/// this groups independent enablement flags with preference order so callers can
/// pass one immutable value through [`CoreOptions`](super::CoreOptions) without
/// giving the transport layer access to server environment parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecOptions {
    /// codecs that may be advertised by newly created RTC sessions
    pub flags: MediaCodecFlags,
    /// preferred offer order for codecs that are enabled and supported by the
    /// RTC backend
    pub preferences: CodecPreferences,
}

/// enablign set for codecs that may enter the RTC capability surface
///
/// values are copyable because this policy is read during session
/// bootstrap and passed through test or benchmark fixtures, disabling a codec
/// removes it from newly created RTC configurations, existing sessions keep the
/// state they already negotiated
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: MediaCodecSet,
}

// gives each codec a paired `*_enabled` reader and `with_*` builder so config
// parsing, tests and RTC bootstrap all use the same public flag surface
macro_rules! media_codec_accessors {
    ($($enabled:ident => $with:ident => $flag:ident),+ $(,)?) => {
        $(
            #[doc = concat!("returns whether `", stringify!($flag), "` may be advertised by new RTC sessions")]
            #[must_use]
            pub fn $enabled(self) -> bool {
                self.enabled.contains(MediaCodecSet::$flag)
            }

            #[doc = concat!("returns a copy with `", stringify!($flag), "` enabled or disabled for new RTC sessions")]
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

/// audio codec entry used to rank the negotiated audio capability surface
///
/// preference values are not a promise that a codec is available, callers must
/// combine the order with [`MediaCodecFlags`] through [`Self::enabled_by`] before
/// building an RTC offer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodecPreference {
    /// default audio codec for browser RTC sessions
    Opus,
    /// g.711 mu-law compatibility codec
    Pcmu,
    /// g.711 a-law compatibility codec
    Pcma,
}

impl AudioCodecPreference {
    /// canonical codec token used at config and SDP-adjacent boundaries
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::Pcmu => "PCMU",
            Self::Pcma => "PCMA",
        }
    }

    /// returns whether this preference can participate in a new RTC offer
    #[must_use]
    pub fn enabled_by(self, flags: MediaCodecFlags) -> bool {
        match self {
            Self::Opus => flags.opus_enabled(),
            Self::Pcmu => flags.pcmu_enabled(),
            Self::Pcma => flags.pcma_enabled(),
        }
    }
}

/// video codec entry used to rank the negotiated video capability surface
///
/// the order is a policy preference only, codec support still depends on
/// [`MediaCodecFlags`] and on the RTC bootstrap path that installs concrete
/// payload configurations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodecPreference {
    /// default video codec for browser RTC sessions
    Vp8,
    /// browser-compatible h264 path with the shared payload contract
    H264,
    /// optional h265 capability controlled by runtime flags
    H265,
    /// optional vp9 capability controlled by runtime flags
    Vp9,
    /// optional av1 capability controlled by runtime flags
    Av1,
}

impl VideoCodecPreference {
    /// canonical codec token used at config and SDP-adjacent boundaries
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

    /// returns whether this preference can participate in a new RTC offer
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

/// complete audio and video codec ordering for offer construction
///
/// callers may provide a partial preferred order, [`CodecPreferences`] fills the
/// remaining codecs with the canonical defaults so downstream code never has to
/// handle a short or duplicate preference list
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecPreferences {
    audio: [AudioCodecPreference; 3],
    video: [VideoCodecPreference; 5],
}

impl CodecPreferences {
    /// canonical audio order used when operators do not override preferences
    pub const DEFAULT_AUDIO: [AudioCodecPreference; 3] = [
        AudioCodecPreference::Opus,
        AudioCodecPreference::Pcmu,
        AudioCodecPreference::Pcma,
    ];
    /// canonical video order used when operators do not override preferences
    pub const DEFAULT_VIDEO: [VideoCodecPreference; 5] = [
        VideoCodecPreference::Vp8,
        VideoCodecPreference::H264,
        VideoCodecPreference::H265,
        VideoCodecPreference::Vp9,
        VideoCodecPreference::Av1,
    ];

    /// builds a full preference set from already-complete audio and video orders
    #[must_use]
    pub const fn new(audio: [AudioCodecPreference; 3], video: [VideoCodecPreference; 5]) -> Self {
        Self { audio, video }
    }

    /// returns a copy where `preferred` codecs lead the audio order
    ///
    /// codecs omitted from `preferred` keep their default relative order, caller
    /// input is expected to be validated by the server config parser before it
    /// reaches this core value type
    #[must_use]
    pub fn with_audio_order(self, preferred: &[AudioCodecPreference]) -> Self {
        Self {
            audio: complete_codec_order(preferred, Self::DEFAULT_AUDIO),
            ..self
        }
    }

    /// returns a copy where `preferred` codecs lead the video order
    ///
    /// codecs omitted from `preferred` keep their default relative order, caller
    /// input is expected to be validated by the server config parser before it
    /// reaches this core value type
    #[must_use]
    pub fn with_video_order(self, preferred: &[VideoCodecPreference]) -> Self {
        Self {
            video: complete_codec_order(preferred, Self::DEFAULT_VIDEO),
            ..self
        }
    }

    /// complete audio order after defaults filled any omitted codecs
    #[must_use]
    pub const fn audio_order(self) -> [AudioCodecPreference; 3] {
        self.audio
    }

    /// complete video order after defaults filled any omitted codecs
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

fn complete_codec_order<T, const N: usize>(preferred: &[T], default: [T; N]) -> [T; N]
where
    T: Copy + Eq,
{
    let mut output = default;
    let mut len = 0;
    for codec in preferred.iter().copied().chain(default) {
        if contains_codec(&output, len, codec) {
            continue;
        }
        if let Some(slot) = output.get_mut(len) {
            *slot = codec;
            len += 1;
        }
    }
    output
}

fn contains_codec<T, const N: usize>(codecs: &[T; N], len: usize, needle: T) -> bool
where
    T: Copy + Eq,
{
    codecs.iter().take(len).any(|codec| *codec == needle)
}
