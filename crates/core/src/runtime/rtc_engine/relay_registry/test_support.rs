#[cfg(test)]
use {
    super::{
        ActiveRelayTarget, ForwardedPacket, InterNodeRelaySender, RELAY_MAILBOX_CAPACITY,
        RelayEnqueueOutcome, RelayPacketMailbox,
    },
    crate::runtime::{media_transport::TransportMediaId, rtc_engine::state::PacketLoopState},
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
impl InterNodeRelaySender {
    pub fn channel_for_test() -> (Self, mpsc::Receiver<ForwardedPacket>) {
        Self::channel_for_test_with_capacity(RELAY_MAILBOX_CAPACITY)
    }

    pub fn channel_for_test_with_capacity(
        capacity: usize,
    ) -> (Self, mpsc::Receiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }
}

#[cfg(test)]
impl ActiveRelayTarget {
    pub fn forward_packet(
        &self,
        state: &PacketLoopState,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RelayEnqueueOutcome> {
        self.target()
            .forward_packet(state, packet, source_transport_media_id)
            .map(super::RelayEnqueueReport::outcome)
    }
}
