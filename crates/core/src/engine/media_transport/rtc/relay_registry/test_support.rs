#[cfg(test)]
use {
    super::{
        ActiveRelayTarget, ForwardedPacket, RELAY_MAILBOX_CAPACITY, RelayEnqueueOutcome,
        RelayPacketMailbox,
    },
    crate::engine::{
        media_transport::TransportMediaId, media_transport::rtc::state::PacketLoopState,
    },
    tokio::sync::mpsc,
};

#[cfg(test)]
impl RelayPacketMailbox {
    pub fn channel_for_test() -> (Self, mpsc::Receiver<ForwardedPacket>) {
        Self::channel_for_test_with_capacity(RELAY_MAILBOX_CAPACITY)
    }

    pub fn channel_for_test_with_capacity(
        capacity: usize,
    ) -> (Self, mpsc::Receiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self::new(tx), rx)
    }
}

#[cfg(test)]
impl ActiveRelayTarget {
    pub fn forward_packet_outcome(
        &self,
        state: &PacketLoopState,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RelayEnqueueOutcome> {
        self.target
            .forward_packet(state, packet, source_transport_media_id)
            .map(|report| report.outcome)
    }
}
