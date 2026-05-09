use o_sfu_rfc::rtp::{self, h264, vp8};
use o_sfu_router::{MediaKind as RouterMediaKind, MediaStream};
use str0m::media::MediaKind as Str0mMediaKind;

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime::rtc_engine) struct PacketLoopSourceFacts {
    kind: PacketLoopSourceKind,
    codec: PacketLoopSourceCodec,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PacketLoopSourceKind {
    #[default]
    Unknown,
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PacketLoopSourceCodec {
    #[default]
    Unknown,
    H264,
    Vp8,
    Other,
}

impl PacketLoopSourceFacts {
    pub(in crate::runtime::rtc_engine) const fn is_video(self) -> bool {
        matches!(self.kind, PacketLoopSourceKind::Video)
    }

    pub(in crate::runtime::rtc_engine) fn payload_starts_decoder_refresh(
        self,
        payload: &[u8],
    ) -> bool {
        match self.codec {
            PacketLoopSourceCodec::H264 => h264::payload_starts_idr(payload),
            PacketLoopSourceCodec::Vp8 | PacketLoopSourceCodec::Unknown => {
                vp8::payload_starts_keyframe(payload)
            }
            PacketLoopSourceCodec::Other => false,
        }
    }

    pub(in crate::runtime::rtc_engine) fn set_kind_from_str0m(&mut self, kind: Str0mMediaKind) {
        self.kind = PacketLoopSourceKind::from_str0m(kind);
    }

    pub(in crate::runtime::rtc_engine) fn set_from_parameters(&mut self, parameters: &MediaStream) {
        self.kind = PacketLoopSourceKind::from_parameters(parameters);
        self.codec = PacketLoopSourceCodec::from_parameters(parameters);
    }
}

impl PacketLoopSourceKind {
    const fn from_str0m(kind: Str0mMediaKind) -> Self {
        match kind {
            Str0mMediaKind::Audio => Self::Audio,
            Str0mMediaKind::Video => Self::Video,
        }
    }

    fn from_parameters(parameters: &MediaStream) -> Self {
        parameters
            .formats()
            .next()
            .map_or(Self::Unknown, |format| match format.media_kind() {
                RouterMediaKind::Audio => Self::Audio,
                RouterMediaKind::Video => Self::Video,
            })
    }
}

impl PacketLoopSourceCodec {
    fn from_parameters(parameters: &MediaStream) -> Self {
        let mut has_vp8 = false;
        for format in parameters.formats() {
            match *format.codec() {
                rtp::CodecName::H264 => return Self::H264,
                rtp::CodecName::Vp8 => has_vp8 = true,
                _ => {}
            }
        }
        if has_vp8 { Self::Vp8 } else { Self::Other }
    }
}
