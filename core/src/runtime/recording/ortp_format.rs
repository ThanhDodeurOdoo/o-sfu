use std::{error::Error, fmt};

pub(crate) const ORTP_MAGIC: [u8; 4] = *b"ORTP";
pub(crate) const ORTP_VERSION: u16 = 1;
pub(crate) const ORTP_FILE_HEADER_LEN: usize = 32;
pub(crate) const ORTP_FRAME_HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum OrtpCodec {
    Opus = 1,
    Vp8 = 2,
    Vp9 = 3,
    H264 = 4,
}

impl TryFrom<u16> for OrtpCodec {
    type Error = OrtpFormatError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Opus),
            2 => Ok(Self::Vp8),
            3 => Ok(Self::Vp9),
            4 => Ok(Self::H264),
            _ => Err(OrtpFormatError::UnsupportedCodec(value)),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrtpFileHeader {
    pub(crate) codec: OrtpCodec,
    pub(crate) clock_rate: u32,
    pub(crate) channel_count: u8,
    pub(crate) payload_type: u8,
}

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

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, OrtpFormatError> {
        let Some(header) = bytes.get(..ORTP_FILE_HEADER_LEN) else {
            return Err(OrtpFormatError::TruncatedHeader {
                expected: ORTP_FILE_HEADER_LEN,
                actual: bytes.len(),
            });
        };
        if read_file_header_array::<4>(header, 0)? != ORTP_MAGIC {
            return Err(OrtpFormatError::InvalidMagic);
        }
        let version = u16::from_le_bytes(read_file_header_array::<2>(header, 4)?);
        if version != ORTP_VERSION {
            return Err(OrtpFormatError::UnsupportedVersion(version));
        }
        let codec =
            OrtpCodec::try_from(u16::from_le_bytes(read_file_header_array::<2>(header, 6)?))?;
        let clock_rate = u32::from_le_bytes(read_file_header_array::<4>(header, 8)?);
        Ok(Self {
            codec,
            clock_rate,
            channel_count: *header.get(12).ok_or(OrtpFormatError::TruncatedHeader {
                expected: ORTP_FILE_HEADER_LEN,
                actual: header.len(),
            })?,
            payload_type: *header.get(13).ok_or(OrtpFormatError::TruncatedHeader {
                expected: ORTP_FILE_HEADER_LEN,
                actual: header.len(),
            })?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrtpFrameHeader {
    pub(crate) reception_timestamp_us: u64,
    pub(crate) rtp_packet_len: u32,
}

impl OrtpFrameHeader {
    pub(crate) fn to_bytes(self) -> [u8; ORTP_FRAME_HEADER_LEN] {
        let mut bytes = [0_u8; ORTP_FRAME_HEADER_LEN];
        bytes[0..8].copy_from_slice(&self.reception_timestamp_us.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.rtp_packet_len.to_le_bytes());
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, OrtpFormatError> {
        let Some(header) = bytes.get(..ORTP_FRAME_HEADER_LEN) else {
            return Err(OrtpFormatError::TruncatedFrameHeader {
                expected: ORTP_FRAME_HEADER_LEN,
                actual: bytes.len(),
            });
        };
        Ok(Self {
            reception_timestamp_us: u64::from_le_bytes(read_frame_header_array::<8>(header, 0)?),
            rtp_packet_len: u32::from_le_bytes(read_frame_header_array::<4>(header, 8)?),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrtpFormatError {
    InvalidMagic,
    UnsupportedVersion(u16),
    UnsupportedCodec(u16),
    TruncatedHeader { expected: usize, actual: usize },
    TruncatedFrameHeader { expected: usize, actual: usize },
}

impl fmt::Display for OrtpFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid ORTP file magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported ORTP version {version}")
            }
            Self::UnsupportedCodec(codec) => {
                write!(formatter, "unsupported ORTP codec {codec}")
            }
            Self::TruncatedHeader { expected, actual } => write!(
                formatter,
                "truncated ORTP header: expected {expected} bytes, got {actual}"
            ),
            Self::TruncatedFrameHeader { expected, actual } => write!(
                formatter,
                "truncated ORTP frame header: expected {expected} bytes, got {actual}"
            ),
        }
    }
}

impl Error for OrtpFormatError {}

fn read_file_header_array<const N: usize>(
    header: &[u8],
    start: usize,
) -> Result<[u8; N], OrtpFormatError> {
    read_array::<N>(header, start).map_err(|actual| OrtpFormatError::TruncatedHeader {
        expected: ORTP_FILE_HEADER_LEN,
        actual,
    })
}

fn read_frame_header_array<const N: usize>(
    header: &[u8],
    start: usize,
) -> Result<[u8; N], OrtpFormatError> {
    read_array::<N>(header, start).map_err(|actual| OrtpFormatError::TruncatedFrameHeader {
        expected: ORTP_FRAME_HEADER_LEN,
        actual,
    })
}

fn read_array<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], usize> {
    let end = start.saturating_add(N);
    let Some(slice) = bytes.get(start..end) else {
        return Err(bytes.len());
    };
    slice.try_into().map_err(|_error| bytes.len())
}
