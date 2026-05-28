//! h264 payload contract shared by router capabilities and str0m bootstrap
//!
//! these entries are the browser-facing RTP payload table that str0m advertises
//! in SDP
//! router capabilities use the same PT/fmtp pairs as receiver-safe local
//! forwarding targets

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine) struct H264PayloadSpec {
    payload_type: u8,
    packetization_mode: H264PacketizationMode,
    profile_level_id: u32,
}

impl H264PayloadSpec {
    const fn new(
        payload_type: u8,
        packetization_mode: H264PacketizationMode,
        profile_level_id: u32,
    ) -> Self {
        Self {
            payload_type,
            packetization_mode,
            profile_level_id,
        }
    }

    pub const fn payload_type(self) -> u8 {
        self.payload_type
    }

    pub const fn packetization_mode(self) -> H264PacketizationMode {
        self.packetization_mode
    }

    pub const fn profile_level_id(self) -> u32 {
        self.profile_level_id
    }

    pub fn profile_level_id_parameter(self) -> String {
        let profile_level_id = self.profile_level_id;
        format!("{profile_level_id:06x}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine) enum H264PacketizationMode {
    SingleNalUnit,
    NonInterleaved,
}

impl H264PacketizationMode {
    pub const fn fmtp_value(self) -> u8 {
        match self {
            Self::SingleNalUnit => 0,
            Self::NonInterleaved => 1,
        }
    }

    pub const fn str0m_flag(self) -> bool {
        matches!(self, Self::NonInterleaved)
    }
}

pub(in crate::engine) const H264_PAYLOAD_SPECS: &[H264PayloadSpec] = &[
    H264PayloadSpec::new(127, H264PacketizationMode::NonInterleaved, 0x0042_001f),
    H264PayloadSpec::new(125, H264PacketizationMode::SingleNalUnit, 0x0042_001f),
    H264PayloadSpec::new(108, H264PacketizationMode::NonInterleaved, 0x0042_e01f),
    H264PayloadSpec::new(124, H264PacketizationMode::SingleNalUnit, 0x0042_e01f),
    H264PayloadSpec::new(123, H264PacketizationMode::NonInterleaved, 0x004d_001f),
    H264PayloadSpec::new(35, H264PacketizationMode::SingleNalUnit, 0x004d_001f),
    H264PayloadSpec::new(114, H264PacketizationMode::NonInterleaved, 0x0064_001f),
];
