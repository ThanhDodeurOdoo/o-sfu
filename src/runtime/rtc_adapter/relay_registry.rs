use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, PoisonError, RwLock,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct RelayTargetId(u64);

impl RelayTargetId {
    pub(super) const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Clone)]
struct RelayTargetRegistration {
    mailbox: RelayPacketMailbox,
    reference_count: usize,
    active_reference_count: usize,
}

#[derive(Clone, Default)]
struct RelaySourceRegistration {
    #[cfg(test)]
    channel_runtime_id: u64,
    targets: BTreeMap<RelayTargetId, RelayTargetRegistration>,
    mailboxes: Arc<[RelayPacketMailbox]>,
}

impl RelaySourceRegistration {
    #[cfg(test)]
    fn for_channel(channel_runtime_id: u64) -> Self {
        Self {
            channel_runtime_id,
            ..Self::default()
        }
    }

    fn add_target(&mut self, target_id: RelayTargetId, mailbox: RelayPacketMailbox) {
        if let Some(registration) = self.targets.get_mut(&target_id) {
            registration.reference_count = registration.reference_count.saturating_add(1);
        } else {
            self.targets.insert(
                target_id,
                RelayTargetRegistration {
                    mailbox,
                    reference_count: 1,
                    active_reference_count: 0,
                },
            );
        }
    }

    fn remove_target(&mut self, target_id: RelayTargetId) -> bool {
        let Some(registration) = self.targets.get_mut(&target_id) else {
            return self.targets.is_empty();
        };
        if registration.reference_count > 1 {
            registration.reference_count -= 1;
            if registration.active_reference_count > registration.reference_count {
                registration.active_reference_count = registration.reference_count;
                self.rebuild_mailboxes();
            }
            return false;
        }
        self.targets.remove(&target_id);
        self.rebuild_mailboxes();
        self.targets.is_empty()
    }

    fn set_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        let Some(registration) = self.targets.get_mut(&target_id) else {
            return;
        };
        let was_forwarding = registration.active_reference_count > 0;
        if active {
            registration.active_reference_count = registration
                .active_reference_count
                .saturating_add(1)
                .min(registration.reference_count);
        } else if registration.active_reference_count > 0 {
            registration.active_reference_count -= 1;
        }
        let is_forwarding = registration.active_reference_count > 0;
        if was_forwarding != is_forwarding {
            self.rebuild_mailboxes();
        }
    }

    fn mailboxes(&self) -> Arc<[RelayPacketMailbox]> {
        Arc::clone(&self.mailboxes)
    }

    #[cfg(test)]
    fn channel_runtime_id(&self) -> u64 {
        self.channel_runtime_id
    }

    #[cfg(test)]
    fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[cfg(test)]
    fn active_target_count(&self) -> usize {
        self.targets
            .values()
            .filter(|registration| registration.active_reference_count > 0)
            .count()
    }

    fn has_active_targets(&self) -> bool {
        self.targets
            .values()
            .any(|registration| registration.active_reference_count > 0)
    }

    fn rebuild_mailboxes(&mut self) {
        self.mailboxes = self
            .targets
            .values()
            .filter(|registration| registration.active_reference_count > 0)
            .map(|registration| registration.mailbox.clone())
            .collect::<Vec<_>>()
            .into();
    }
}

pub(super) struct RelayRegistry {
    any_active: AtomicBool,
    active_sources: RwLock<BTreeMap<TransportMediaId, RelaySourceRegistration>>,
}

impl Default for RelayRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            active_sources: RwLock::new(BTreeMap::new()),
        }
    }
}

impl RelayRegistry {
    pub(super) fn mailboxes_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<Arc<[RelayPacketMailbox]>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .and_then(|registration| {
                registration
                    .has_active_targets()
                    .then(|| registration.mailboxes())
            })
    }

    pub(super) fn activate_source_target(
        &self,
        channel_runtime_id: u64,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        mailbox: RelayPacketMailbox,
    ) {
        #[cfg(not(test))]
        let _ = channel_runtime_id;
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_sources
            .entry(source_transport_media_id)
            .or_insert_with(|| {
                #[cfg(test)]
                {
                    RelaySourceRegistration::for_channel(channel_runtime_id)
                }
                #[cfg(not(test))]
                {
                    RelaySourceRegistration::default()
                }
            })
            .add_target(target_id, mailbox);
        let has_active_sources = active_sources
            .values()
            .any(RelaySourceRegistration::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }

    pub(super) fn deactivate_source_target(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let remove_source = active_sources
            .get_mut(&source_transport_media_id)
            .is_some_and(|source| source.remove_target(target_id));
        if remove_source {
            active_sources.remove(&source_transport_media_id);
        }
        let has_active_sources = active_sources
            .values()
            .any(RelaySourceRegistration::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }

    pub(super) fn set_source_target_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(source_registration) = active_sources.get_mut(&source_transport_media_id) else {
            return;
        };
        source_registration.set_target_active(target_id, active);
        let has_active_sources = active_sources
            .values()
            .any(RelaySourceRegistration::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }

    fn active_source_count(&self) -> usize {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    pub(super) fn target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::active_target_count)
    }

    #[cfg(test)]
    pub(super) fn has_any_source_for_channel(&self, channel_runtime_id: u64) -> bool {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .any(|source| source.channel_runtime_id() == channel_runtime_id)
    }
}

impl fmt::Debug for RelayRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelayRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_source_count", &self.active_source_count())
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
        let source_transport_media_id = TransportMediaId::new(8);
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            relay_target,
            mailbox,
        );
        registry.set_source_target_active(source_transport_media_id, relay_target, true);
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_some()
        );
        assert!(registry.has_any_source_for_channel(channel_runtime_id));

        registry.deactivate_source_target(source_transport_media_id, relay_target);
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_none()
        );
        assert!(!registry.has_any_source_for_channel(channel_runtime_id));
    }

    #[test]
    fn relay_registry_forwards_packets_through_registered_mailboxes() {
        let registry = RelayRegistry::default();
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 13;
        let source_transport_media_id = TransportMediaId::new(9);
        let session_key = TransportSessionKey::new(13, 0, 14, SessionId::Integer(15));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            relay_target,
            mailbox,
        );
        registry.set_source_target_active(source_transport_media_id, relay_target, true);

        let relay_mailboxes = registry.mailboxes_for_source(source_transport_media_id);
        assert!(relay_mailboxes.is_some());
        if let Some(relay_mailboxes) = relay_mailboxes {
            assert_eq!(relay_mailboxes.len(), 1);
            if let Some(relay_mailbox) = relay_mailboxes.first() {
                relay_mailbox.forward_packet(&packet, source_transport_media_id);
            }
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

    #[test]
    fn relay_registry_keeps_multiple_target_mailboxes_per_channel() {
        let registry = RelayRegistry::default();
        let (first_mailbox, mut first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, mut second_rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 18;
        let source_transport_media_id = TransportMediaId::new(11);
        let session_key = TransportSessionKey::new(18, 0, 19, SessionId::Integer(20));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            RelayTargetId::new(1),
            first_mailbox,
        );
        registry.set_source_target_active(source_transport_media_id, RelayTargetId::new(1), true);
        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            RelayTargetId::new(2),
            second_mailbox,
        );
        registry.set_source_target_active(source_transport_media_id, RelayTargetId::new(2), true);

        let relay_mailboxes = registry.mailboxes_for_source(source_transport_media_id);
        assert!(relay_mailboxes.is_some());
        if let Some(relay_mailboxes) = relay_mailboxes {
            assert_eq!(relay_mailboxes.len(), 2);
            for relay_mailbox in relay_mailboxes.iter() {
                relay_mailbox.forward_packet(&packet, source_transport_media_id);
            }
        }

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
    }

    #[test]
    fn relay_registry_reference_counts_target_mailboxes_before_cleanup() {
        let registry = RelayRegistry::default();
        let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 21;
        let source_transport_media_id = TransportMediaId::new(12);
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            relay_target,
            mailbox.clone(),
        );
        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            relay_target,
            mailbox,
        );
        registry.set_source_target_active(source_transport_media_id, relay_target, true);
        registry.set_source_target_active(source_transport_media_id, relay_target, true);
        assert_eq!(
            registry.target_count_for_source(source_transport_media_id),
            1
        );
        assert_eq!(
            registry.active_target_count_for_source(source_transport_media_id),
            1
        );

        registry.deactivate_source_target(source_transport_media_id, relay_target);
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_some()
        );

        registry.deactivate_source_target(source_transport_media_id, relay_target);
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_none()
        );
    }

    #[test]
    fn relay_registry_keeps_sources_independent_within_one_channel() {
        let registry = RelayRegistry::default();
        let channel_runtime_id = 24;
        let first_source_transport_media_id = TransportMediaId::new(31);
        let second_source_transport_media_id = TransportMediaId::new(32);
        let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();

        registry.activate_source_target(
            channel_runtime_id,
            first_source_transport_media_id,
            RelayTargetId::new(1),
            first_mailbox,
        );
        registry.set_source_target_active(
            first_source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
        registry.activate_source_target(
            channel_runtime_id,
            second_source_transport_media_id,
            RelayTargetId::new(2),
            second_mailbox,
        );
        registry.set_source_target_active(
            second_source_transport_media_id,
            RelayTargetId::new(2),
            true,
        );

        assert_eq!(
            registry.target_count_for_source(first_source_transport_media_id),
            1
        );
        assert_eq!(
            registry.target_count_for_source(second_source_transport_media_id),
            1
        );
        registry.deactivate_source_target(first_source_transport_media_id, RelayTargetId::new(1));
        assert!(
            registry
                .mailboxes_for_source(first_source_transport_media_id)
                .is_none()
        );
        assert!(
            registry
                .mailboxes_for_source(second_source_transport_media_id)
                .is_some()
        );
    }

    #[test]
    fn relay_registry_only_forwards_to_targets_with_active_routes() {
        let registry = RelayRegistry::default();
        let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();
        let channel_runtime_id = 27;
        let source_transport_media_id = TransportMediaId::new(41);
        let first_target = RelayTargetId::new(1);
        let second_target = RelayTargetId::new(2);

        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            first_target,
            first_mailbox,
        );
        registry.activate_source_target(
            channel_runtime_id,
            source_transport_media_id,
            second_target,
            second_mailbox,
        );
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_none()
        );

        registry.set_source_target_active(source_transport_media_id, second_target, true);
        let relay_mailboxes = registry.mailboxes_for_source(source_transport_media_id);
        assert!(relay_mailboxes.is_some());
        let Some(relay_mailboxes) = relay_mailboxes else {
            return;
        };
        assert_eq!(relay_mailboxes.len(), 1);
        assert_eq!(
            registry.active_target_count_for_source(source_transport_media_id),
            1
        );

        registry.set_source_target_active(source_transport_media_id, second_target, false);
        assert!(
            registry
                .mailboxes_for_source(source_transport_media_id)
                .is_none()
        );
        assert_eq!(
            registry.active_target_count_for_source(source_transport_media_id),
            0
        );
    }
}
