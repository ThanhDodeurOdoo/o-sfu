use std::{
    collections::HashMap,
    fmt,
    sync::{
        PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;

use crate::runtime::transport_adapter::TransportMediaId;

use super::forwarded_packet::ForwardedPacket;

#[derive(Debug, Clone)]
pub(super) struct RelayPacketMailbox {
    tx: mpsc::UnboundedSender<ForwardedPacket>,
}

impl RelayPacketMailbox {
    pub(super) fn new(tx: mpsc::UnboundedSender<ForwardedPacket>) -> Self {
        Self { tx }
    }

    pub(super) fn forward_packet(
        &self,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) {
        let _ = self
            .tx
            .send(packet.share_for_relay(source_transport_media_id));
    }

    #[cfg(test)]
    pub(super) fn channel_for_test() -> (Self, mpsc::UnboundedReceiver<ForwardedPacket>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self::new(tx), rx)
    }
}

pub(super) struct RelayRegistry {
    any_active: AtomicBool,
    active_channels: RwLock<HashMap<u64, RelayPacketMailbox>>,
}

impl Default for RelayRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_channels: RwLock::new(HashMap::new()),
        }
    }
}

impl RelayRegistry {
    pub(super) fn mailbox_for_channel(
        &self,
        channel_runtime_id: u64,
    ) -> Option<RelayPacketMailbox> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_runtime_id)
            .cloned()
    }

    pub(super) fn activate_channel(&self, channel_runtime_id: u64, mailbox: RelayPacketMailbox) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_runtime_id, mailbox);
        self.any_active.store(true, Ordering::Release);
    }

    #[allow(
        dead_code,
        reason = "the first production relay registration slice activates cross-worker fan-out, but mailbox teardown still remains on the follow-up cleanup path"
    )]
    pub(super) fn deactivate_channel(&self, channel_runtime_id: u64) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_runtime_id);
        self.any_active
            .store(!active_channels.is_empty(), Ordering::Release);
    }

    fn active_channel_count(&self) -> usize {
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl fmt::Debug for RelayRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_channel_count", &self.active_channel_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::rtc_adapter::{sample_forwarded_packet, state::RtcBootstrapState};
    use crate::runtime::transport_adapter::TransportSessionKey;
    use crate::signaling::shared::SessionId;

    #[test]
    fn relay_registry_tracks_active_channels() {
        let registry = RelayRegistry::default();
        let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 12;

        registry.activate_channel(channel_runtime_id, mailbox);
        assert!(registry.mailbox_for_channel(channel_runtime_id).is_some());

        registry.deactivate_channel(channel_runtime_id);
        assert!(registry.mailbox_for_channel(channel_runtime_id).is_none());
    }

    #[test]
    fn relay_registry_forwards_packets_through_registered_mailboxes() {
        let registry = RelayRegistry::default();
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 13;
        let session_key = TransportSessionKey::new(13, 0, 14, SessionId::Integer(15));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        registry.activate_channel(channel_runtime_id, mailbox);

        let relay_mailbox = registry.mailbox_for_channel(channel_runtime_id);
        assert!(relay_mailbox.is_some());
        if let Some(relay_mailbox) = relay_mailbox {
            relay_mailbox.forward_packet(&packet, TransportMediaId::new(9));
        }

        let forwarded = relay_rx.try_recv().ok();
        assert!(forwarded.is_some());
        if let Some(forwarded) = forwarded {
            assert_eq!(forwarded.payload().as_slice(), b"payload");
            assert_eq!(
                forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
                Some(TransportMediaId::new(9))
            );
        }
    }
}
