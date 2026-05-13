use std::io::{Result as IoResult, Write};

use super::{OrtpFileHeader, ortp_format::OrtpFrameHeader};

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
pub(crate) struct StreamWriter<W> {
    inner: W,
}

#[allow(dead_code, reason = "recording finalization owns staged ORTP writing")]
impl<W: Write> StreamWriter<W> {
    pub(crate) fn new(mut inner: W, header: OrtpFileHeader) -> IoResult<Self> {
        inner.write_all(&header.to_bytes())?;
        Ok(Self { inner })
    }

    pub(crate) fn write_frame(
        &mut self,
        reception_timestamp_us: u64,
        rtp_packet: &[u8],
    ) -> IoResult<()> {
        let frame_header = OrtpFrameHeader {
            reception_timestamp_us,
            rtp_packet_len: u32::try_from(rtp_packet.len()).unwrap_or(u32::MAX),
        };
        self.inner.write_all(&frame_header.to_bytes())?;
        self.inner.write_all(rtp_packet)?;
        Ok(())
    }

    pub(crate) fn into_inner(self) -> W {
        self.inner
    }
}
