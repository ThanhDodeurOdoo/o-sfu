//! local browser RTP profile used for str0m bootstrap and router capabilities

pub mod h264;
pub mod payload_type;

pub const MID_EXTENSION_ID: u8 = 1;
pub const ABS_SEND_TIME_EXTENSION_ID: u8 = 4;
pub const TRANSPORT_WIDE_CC_EXTENSION_ID: u8 = 5;
pub const SSRC_AUDIO_LEVEL_EXTENSION_ID: u8 = 10;
