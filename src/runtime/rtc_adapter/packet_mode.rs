//! Adapter-local packet-mode switch for the RTC transport adapter.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PacketMode {
    Frame,
    Rtp,
}

pub(super) const ACTIVE_PACKET_MODE: PacketMode = PacketMode::Rtp;

impl PacketMode {
    pub(super) const fn uses_str0m_rtp_mode(self) -> bool {
        matches!(self, Self::Rtp)
    }
}
