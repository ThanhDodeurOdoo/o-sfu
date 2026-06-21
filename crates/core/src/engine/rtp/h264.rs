//! h264 payload profile advertised to browser peers

use o_sfu_rfc::rtp::{
    PayloadType,
    h264::{LevelIdc, PacketizationMode, Profile, ProfileLevelId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct H264PayloadSpec {
    payload_type: PayloadType,
    packetization_mode: PacketizationMode,
    profile_level_id: ProfileLevelId,
}

impl H264PayloadSpec {
    const fn new(
        payload_type: PayloadType,
        packetization_mode: PacketizationMode,
        profile: Profile,
        level: LevelIdc,
    ) -> Self {
        Self {
            payload_type,
            packetization_mode,
            profile_level_id: ProfileLevelId::new(profile, level),
        }
    }

    pub const fn payload_type(self) -> PayloadType {
        self.payload_type
    }

    pub const fn packetization_mode(self) -> PacketizationMode {
        self.packetization_mode
    }

    pub const fn profile_level_id(self) -> ProfileLevelId {
        self.profile_level_id
    }
}

pub const H264_PAYLOAD_SPECS: &[H264PayloadSpec] = &[
    H264PayloadSpec::new(
        PayloadType::new(127),
        PacketizationMode::NonInterleaved,
        Profile::Baseline,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(125),
        PacketizationMode::SingleNalUnit,
        Profile::Baseline,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(108),
        PacketizationMode::NonInterleaved,
        Profile::ConstrainedBaseline,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(124),
        PacketizationMode::SingleNalUnit,
        Profile::ConstrainedBaseline,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(123),
        PacketizationMode::NonInterleaved,
        Profile::Main,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(35),
        PacketizationMode::SingleNalUnit,
        Profile::Main,
        LevelIdc::Level3_1,
    ),
    H264PayloadSpec::new(
        PayloadType::new(114),
        PacketizationMode::NonInterleaved,
        Profile::High,
        LevelIdc::Level3_1,
    ),
];
