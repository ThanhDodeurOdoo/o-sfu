#[cfg(test)]
use tokio::sync::mpsc;

#[cfg(test)]
use super::{
    ActiveRelayTarget, ForwardedPacket, InterNodeRelaySender, RELAY_MAILBOX_CAPACITY,
    RelayEnqueueOutcome, RelayPacketMailbox,
};
#[cfg(test)]
use crate::runtime::media_transport::TransportMediaId;

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
#[cfg(test)]
impl ActiveRelayTarget {
    pub fn forward_packet(
        &self,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> RelayEnqueueOutcome {
        self.target()
            .forward_packet(packet, source_transport_media_id)
            .outcome()
    }
}
