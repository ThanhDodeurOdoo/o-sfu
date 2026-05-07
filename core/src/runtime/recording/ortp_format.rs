#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
pub(crate) const ORTP_MAGIC: [u8; 4] = *b"ORTP";
#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
pub(crate) const ORTP_VERSION: u16 = 1;
#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
pub(crate) const ORTP_FILE_HEADER_LEN: usize = 32;
#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
pub(crate) const ORTP_FRAME_HEADER_LEN: usize = 12;

#[allow(
    dead_code,
    reason = "recording finalization owns staged ORTP codec ids"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum OrtpCodec {
    Opus = 1,
    Vp8 = 2,
    Vp9 = 3,
    H264 = 4,
}

impl From<OrtpCodec> for u16 {
    fn from(value: OrtpCodec) -> Self {
        match value {
            OrtpCodec::Opus => 1,
            OrtpCodec::Vp8 => 2,
            OrtpCodec::Vp9 => 3,
            OrtpCodec::H264 => 4,
        }
    }
}

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrtpFileHeader {
    pub(crate) codec: OrtpCodec,
    pub(crate) clock_rate: u32,
    pub(crate) channel_count: u8,
    pub(crate) payload_type: u8,
}

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
impl OrtpFileHeader {
    pub(crate) fn to_bytes(self) -> [u8; ORTP_FILE_HEADER_LEN] {
        let mut bytes = [0_u8; ORTP_FILE_HEADER_LEN];
        bytes[0..4].copy_from_slice(&ORTP_MAGIC);
        bytes[4..6].copy_from_slice(&ORTP_VERSION.to_le_bytes());
        bytes[6..8].copy_from_slice(&u16::from(self.codec).to_le_bytes());
        bytes[8..12].copy_from_slice(&self.clock_rate.to_le_bytes());
        bytes[12] = self.channel_count;
        bytes[13] = self.payload_type;
        bytes
    }
}

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrtpFrameHeader {
    pub(crate) reception_timestamp_us: u64,
    pub(crate) rtp_packet_len: u32,
}

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
impl OrtpFrameHeader {
    pub(crate) fn to_bytes(self) -> [u8; ORTP_FRAME_HEADER_LEN] {
        let mut bytes = [0_u8; ORTP_FRAME_HEADER_LEN];
        bytes[0..8].copy_from_slice(&self.reception_timestamp_us.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.rtp_packet_len.to_le_bytes());
        bytes
    }
}
