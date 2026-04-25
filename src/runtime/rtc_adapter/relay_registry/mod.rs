use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, PoisonError, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::mpsc;

use super::forwarded_packet::ForwardedPacket;
use crate::runtime::transport_adapter::TransportMediaId;

#[cfg(test)]
mod test_support;

pub(super) const RELAY_MAILBOX_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayEnqueueOutcome {
    Enqueued,
    Overloaded,
    Closed,
}

#[derive(Debug, Clone)]
pub(super) struct RelayPacketMailbox {
    tx: mpsc::Sender<ForwardedPacket>,
}

impl RelayPacketMailbox {
    pub(super) fn new(tx: mpsc::Sender<ForwardedPacket>) -> Self {
        Self { tx }
    }

    pub(super) fn forward_packet(
        &self,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> RelayEnqueueOutcome {
        forward_packet_to_target(&self.tx, packet, source_transport_media_id)
    }
}

#[derive(Debug, Clone)]
pub(super) struct InterNodeRelaySender {
    tx: mpsc::Sender<ForwardedPacket>,
}

impl InterNodeRelaySender {
    pub(super) fn forward_packet(
        &self,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> RelayEnqueueOutcome {
        forward_packet_to_target(&self.tx, packet, source_transport_media_id)
    }
}

#[derive(Debug, Clone)]
pub(super) enum RelayTargetTransport {
    IntraNodeMailbox(RelayPacketMailbox),
    InterNodeSender(InterNodeRelaySender),
}

#[derive(Debug, Clone)]
pub struct ActiveRelayTarget<TargetId, Target> {
    target_id: TargetId,
    target: Target,
}

impl<TargetId, Target> ActiveRelayTarget<TargetId, Target> {
    fn new(target_id: TargetId, target: Target) -> Self {
        Self { target_id, target }
    }

    pub const fn target_id(&self) -> TargetId
    where
        TargetId: Copy,
    {
        self.target_id
    }

    pub const fn target(&self) -> &Target {
        &self.target
    }
}

impl From<RelayPacketMailbox> for RelayTargetTransport {
    fn from(value: RelayPacketMailbox) -> Self {
        Self::IntraNodeMailbox(value)
    }
}

impl From<InterNodeRelaySender> for RelayTargetTransport {
    fn from(value: InterNodeRelaySender) -> Self {
        Self::InterNodeSender(value)
    }
}

fn forward_packet_to_target(
    tx: &mpsc::Sender<ForwardedPacket>,
    packet: &ForwardedPacket,
    source_transport_media_id: TransportMediaId,
) -> RelayEnqueueOutcome {
    match tx.try_send(packet.share_for_relay(source_transport_media_id)) {
        Ok(()) => RelayEnqueueOutcome::Enqueued,
        Err(mpsc::error::TrySendError::Full(_packet)) => RelayEnqueueOutcome::Overloaded,
        Err(mpsc::error::TrySendError::Closed(_packet)) => RelayEnqueueOutcome::Closed,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct RelayTargetId(u64);

impl RelayTargetId {
    pub(super) const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone)]
struct RelayTargetRegistration<Target> {
    target: Target,
    reference_count: usize,
    active_reference_count: usize,
}

/// Shared per-source relay target state used by the live registry and the Loom
/// model that validates publication and reference-count transitions.
#[derive(Debug, Clone)]
pub struct RelayTargetRegistry<TargetId, Target> {
    targets: BTreeMap<TargetId, RelayTargetRegistration<Target>>,
    active_targets: Arc<[ActiveRelayTarget<TargetId, Target>]>,
}

impl<TargetId, Target> Default for RelayTargetRegistry<TargetId, Target> {
    fn default() -> Self {
        Self {
            targets: BTreeMap::new(),
            active_targets: Arc::default(),
        }
    }
}

impl<TargetId, Target> RelayTargetRegistry<TargetId, Target>
where
    TargetId: Copy + Ord,
    Target: Clone,
{
    pub fn add_target(&mut self, target_id: TargetId, target: Target) {
        if let Some(registration) = self.targets.get_mut(&target_id) {
            registration.reference_count = registration.reference_count.saturating_add(1);
        } else {
            self.targets.insert(
                target_id,
                RelayTargetRegistration {
                    target,
                    reference_count: 1,
                    active_reference_count: 0,
                },
            );
        }
    }

    pub fn remove_target(&mut self, target_id: TargetId) -> bool {
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

    pub fn set_target_active(&mut self, target_id: TargetId, active: bool) {
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

    #[must_use]
    pub fn active_targets(&self) -> Arc<[ActiveRelayTarget<TargetId, Target>]> {
        Arc::clone(&self.active_targets)
    }

    #[must_use]
    pub fn has_active_targets(&self) -> bool {
        self.targets
            .values()
            .any(|registration| registration.active_reference_count > 0)
    }

    pub fn is_target_active(&self, target_id: TargetId) -> bool {
        self.targets
            .get(&target_id)
            .is_some_and(|registration| registration.active_reference_count > 0)
    }

    #[must_use]
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[must_use]
    pub fn active_target_count(&self) -> usize {
        self.targets
            .values()
            .filter(|registration| registration.active_reference_count > 0)
            .count()
    }

    fn rebuild_mailboxes(&mut self) {
        self.active_targets = self
            .targets
            .iter()
            .filter(|(_target_id, registration)| registration.active_reference_count > 0)
            .map(|(target_id, registration)| {
                ActiveRelayTarget::new(*target_id, registration.target.clone())
            })
            .collect::<Vec<_>>()
            .into();
    }
}

type RelaySourceRegistration = RelayTargetRegistry<RelayTargetId, RelayTargetTransport>;

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
    pub(super) fn targets_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<Arc<[ActiveRelayTarget<RelayTargetId, RelayTargetTransport>]>> {
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
                    .then(|| registration.active_targets())
            })
    }

    pub(super) fn activate_source_target(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayTargetTransport,
    ) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_sources
            .entry(source_transport_media_id)
            .or_default()
            .add_target(target_id, target);
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

    pub(super) fn is_source_target_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .is_some_and(|source_registration| source_registration.is_target_active(target_id))
    }

    fn active_source_count(&self) -> usize {
        self.active_sources
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
            .field("active_source_count", &self.active_source_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use o_sfu_protocol::shared::SessionId;

    use super::*;
    use crate::runtime::rtc_adapter::{
        sample_forwarded_packet, state::RtcBootstrapState, test_support::test_transport_session_key,
    };

    #[test]
    fn relay_registry_tracks_active_sources() {
        let registry = RelayRegistry::default();
        let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(8);
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(source_transport_media_id, relay_target, mailbox.into());
        registry.set_source_target_active(source_transport_media_id, relay_target, true);
        assert!(
            registry
                .targets_for_source(source_transport_media_id)
                .is_some()
        );

        registry.deactivate_source_target(source_transport_media_id, relay_target);
        assert!(
            registry
                .targets_for_source(source_transport_media_id)
                .is_none()
        );
    }

    #[test]
    fn relay_registry_forwards_packets_through_registered_mailboxes() {
        let registry = RelayRegistry::default();
        let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(9);
        let session_key = test_transport_session_key(13, 0, 14, SessionId::Integer(15));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(source_transport_media_id, relay_target, mailbox.into());
        registry.set_source_target_active(source_transport_media_id, relay_target, true);

        let relay_targets = registry.targets_for_source(source_transport_media_id);
        assert!(relay_targets.is_some());
        if let Some(relay_targets) = relay_targets {
            assert_eq!(relay_targets.len(), 1);
            if let Some(relay_target) = relay_targets.first() {
                relay_target.forward_packet(&packet, source_transport_media_id);
            }
        }

        let forwarded = relay_rx.try_recv().ok();
        assert!(forwarded.is_some());
        if let Some(mut forwarded) = forwarded {
            assert_eq!(forwarded.payload().as_slice(), b"payload");
            assert_eq!(
                forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
                Some(TransportMediaId::new(9))
            );
        }
    }

    #[test]
    fn relay_registry_keeps_multiple_target_mailboxes_per_source() {
        let registry = RelayRegistry::default();
        let (first_mailbox, mut first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, mut second_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(11);
        let session_key = test_transport_session_key(18, 0, 19, SessionId::Integer(20));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(1),
            first_mailbox.into(),
        );
        registry.set_source_target_active(source_transport_media_id, RelayTargetId::new(1), true);
        registry.activate_source_target(
            source_transport_media_id,
            RelayTargetId::new(2),
            second_mailbox.into(),
        );
        registry.set_source_target_active(source_transport_media_id, RelayTargetId::new(2), true);

        let relay_targets = registry.targets_for_source(source_transport_media_id);
        assert!(relay_targets.is_some());
        if let Some(relay_targets) = relay_targets {
            assert_eq!(relay_targets.len(), 2);
            for relay_target in relay_targets.iter() {
                relay_target.forward_packet(&packet, source_transport_media_id);
            }
        }

        assert!(first_rx.try_recv().is_ok());
        assert!(second_rx.try_recv().is_ok());
    }

    #[test]
    fn relay_registry_reference_counts_target_mailboxes_before_cleanup() {
        let registry = RelayRegistry::default();
        let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(12);
        let relay_target = RelayTargetId::new(1);

        registry.activate_source_target(
            source_transport_media_id,
            relay_target,
            mailbox.clone().into(),
        );
        registry.activate_source_target(source_transport_media_id, relay_target, mailbox.into());
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
                .targets_for_source(source_transport_media_id)
                .is_some()
        );

        registry.deactivate_source_target(source_transport_media_id, relay_target);
        assert!(
            registry
                .targets_for_source(source_transport_media_id)
                .is_none()
        );
    }

    #[test]
    fn relay_registry_keeps_sources_independent() {
        let registry = RelayRegistry::default();
        let first_source_transport_media_id = TransportMediaId::new(31);
        let second_source_transport_media_id = TransportMediaId::new(32);
        let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();

        registry.activate_source_target(
            first_source_transport_media_id,
            RelayTargetId::new(1),
            first_mailbox.into(),
        );
        registry.set_source_target_active(
            first_source_transport_media_id,
            RelayTargetId::new(1),
            true,
        );
        registry.activate_source_target(
            second_source_transport_media_id,
            RelayTargetId::new(2),
            second_mailbox.into(),
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
                .targets_for_source(first_source_transport_media_id)
                .is_none()
        );
        assert!(
            registry
                .targets_for_source(second_source_transport_media_id)
                .is_some()
        );
    }

    #[test]
    fn relay_registry_only_forwards_to_targets_with_active_routes() {
        let registry = RelayRegistry::default();
        let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
        let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(41);
        let first_target = RelayTargetId::new(1);
        let second_target = RelayTargetId::new(2);

        registry.activate_source_target(
            source_transport_media_id,
            first_target,
            first_mailbox.into(),
        );
        registry.activate_source_target(
            source_transport_media_id,
            second_target,
            second_mailbox.into(),
        );
        assert!(
            registry
                .targets_for_source(source_transport_media_id)
                .is_none()
        );

        registry.set_source_target_active(source_transport_media_id, second_target, true);
        let relay_targets = registry.targets_for_source(source_transport_media_id);
        assert!(relay_targets.is_some());
        let Some(relay_targets) = relay_targets else {
            return;
        };
        assert_eq!(relay_targets.len(), 1);
        assert_eq!(
            registry.active_target_count_for_source(source_transport_media_id),
            1
        );

        registry.set_source_target_active(source_transport_media_id, second_target, false);
        assert!(
            registry
                .targets_for_source(source_transport_media_id)
                .is_none()
        );
        assert_eq!(
            registry.active_target_count_for_source(source_transport_media_id),
            0
        );
    }

    #[test]
    fn relay_registry_forwards_packets_through_registered_inter_node_targets() {
        let registry = RelayRegistry::default();
        let (sender, mut relay_rx) = InterNodeRelaySender::channel_for_test();
        let source_transport_media_id = TransportMediaId::new(41);
        let session_key = test_transport_session_key(33, 0, 34, SessionId::Integer(35));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
        let relay_target = RelayTargetId::new(7);

        registry.activate_source_target(source_transport_media_id, relay_target, sender.into());
        registry.set_source_target_active(source_transport_media_id, relay_target, true);

        let relay_targets = registry.targets_for_source(source_transport_media_id);
        assert!(relay_targets.is_some());
        let Some(relay_targets) = relay_targets else {
            return;
        };
        assert_eq!(relay_targets.len(), 1);
        let Some(relay_target) = relay_targets.first() else {
            return;
        };
        relay_target.forward_packet(&packet, source_transport_media_id);

        let forwarded = relay_rx.try_recv().ok();
        assert!(forwarded.is_some());
        let Some(mut forwarded) = forwarded else {
            return;
        };
        assert_eq!(forwarded.payload().as_slice(), b"payload");
        assert_eq!(
            forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
            Some(source_transport_media_id)
        );
    }

    #[test]
    fn relay_registry_reports_overload_when_a_bounded_mailbox_is_full() {
        let (mailbox, _rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);
        let source_transport_media_id = TransportMediaId::new(42);
        let session_key = test_transport_session_key(36, 0, 37, SessionId::Integer(38));
        let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

        assert_eq!(
            mailbox.forward_packet(&packet, source_transport_media_id),
            RelayEnqueueOutcome::Enqueued
        );
        assert_eq!(
            mailbox.forward_packet(&packet, source_transport_media_id),
            RelayEnqueueOutcome::Overloaded
        );
    }
}
