use std::sync::PoisonError;

use tokio::sync::mpsc;

use crate::runtime::transport_adapter::TransportMediaId;

use super::{
    ForwardedPacket, InterNodeRelaySender, RELAY_MAILBOX_CAPACITY, RelayEnqueueOutcome,
    RelayPacketMailbox, RelayRegistry, RelaySourceRegistration, RelayTargetTransport,
};

impl RelayPacketMailbox {
    pub(in crate::runtime::rtc_adapter) fn channel_for_test()
    -> (Self, mpsc::Receiver<ForwardedPacket>) {
        Self::channel_for_test_with_capacity(RELAY_MAILBOX_CAPACITY)
    }

    pub(in crate::runtime::rtc_adapter) fn channel_for_test_with_capacity(
        capacity: usize,
    ) -> (Self, mpsc::Receiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self::new(tx), rx)
    }
}

impl InterNodeRelaySender {
    pub(in crate::runtime::rtc_adapter) fn channel_for_test()
    -> (Self, mpsc::Receiver<ForwardedPacket>) {
        Self::channel_for_test_with_capacity(RELAY_MAILBOX_CAPACITY)
    }

    pub(in crate::runtime::rtc_adapter) fn channel_for_test_with_capacity(
        capacity: usize,
    ) -> (Self, mpsc::Receiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }
}

impl RelayTargetTransport {
    pub(in crate::runtime::rtc_adapter) fn forward_packet(
        &self,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> RelayEnqueueOutcome {
        match self {
            Self::IntraNodeMailbox(mailbox) => {
                mailbox.forward_packet(packet, source_transport_media_id)
            }
            Self::InterNodeSender(sender) => {
                sender.forward_packet(packet, source_transport_media_id)
            }
        }
    }
}

impl RelaySourceRegistration {
    fn target_count(&self) -> usize {
        self.targets.len()
    }

    fn active_target_count(&self) -> usize {
        self.targets
            .values()
            .filter(|registration| registration.active_reference_count > 0)
            .count()
    }
}

impl RelayRegistry {
    pub(in crate::runtime::rtc_adapter) fn target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::target_count)
    }

    pub(in crate::runtime::rtc_adapter) fn active_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::active_target_count)
    }
}
