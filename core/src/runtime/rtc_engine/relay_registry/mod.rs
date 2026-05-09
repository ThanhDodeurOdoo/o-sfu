use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::mpsc;

use super::{forwarded_packet::ForwardedPacket, state::RtcBootstrapState};
use crate::runtime::media_transport::TransportMediaId;

#[cfg(any(test, feature = "testing-transport"))]
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

    pub(super) fn backlog_depth(&self) -> usize {
        sender_backlog_depth(&self.tx)
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
pub(super) struct ActiveRelayTarget<TargetId, Target> {
    target_id: TargetId,
    target: Target,
}

impl<TargetId, Target> ActiveRelayTarget<TargetId, Target> {
    fn new(target_id: TargetId, target: Target) -> Self {
        Self { target_id, target }
    }

    pub(super) const fn target_id(&self) -> TargetId
    where
        TargetId: Copy,
    {
        self.target_id
    }

    pub(super) const fn target(&self) -> &Target {
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

pub(super) fn sender_backlog_depth<T>(tx: &mpsc::Sender<T>) -> usize {
    tx.max_capacity().saturating_sub(tx.capacity())
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
    active: bool,
}

#[derive(Debug, Clone, Copy)]
struct RelayTargetRemoval {
    target_removed: bool,
    source_empty: bool,
    active_set_changed: bool,
}

/// Source-worker cache for relay target handles and active bits.
#[derive(Debug, Clone)]
pub(super) struct RelayTargetRegistry<TargetId, Target> {
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
    pub(super) fn add_target(&mut self, target_id: TargetId, target: Target) {
        self.targets
            .entry(target_id)
            .or_insert(RelayTargetRegistration {
                target,
                active: false,
            });
    }

    fn remove_target(&mut self, target_id: TargetId) -> RelayTargetRemoval {
        let was_active = self.is_target_active(target_id);
        let target_removed = self.targets.remove(&target_id).is_some();
        if was_active {
            self.rebuild_mailboxes();
        }
        RelayTargetRemoval {
            target_removed,
            source_empty: self.targets.is_empty(),
            active_set_changed: was_active,
        }
    }

    pub(super) fn set_target_active(&mut self, target_id: TargetId, active: bool) -> bool {
        let Some(registration) = self.targets.get_mut(&target_id) else {
            return false;
        };
        if registration.active != active {
            registration.active = active;
            self.rebuild_mailboxes();
            return true;
        }
        false
    }

    #[must_use]
    pub(super) fn active_targets_slice(&self) -> &[ActiveRelayTarget<TargetId, Target>] {
        &self.active_targets
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn has_active_targets(&self) -> bool {
        !self.active_targets.is_empty()
    }

    pub(super) fn is_target_active(&self, target_id: TargetId) -> bool {
        self.targets
            .get(&target_id)
            .is_some_and(|registration| registration.active)
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn active_target_count(&self) -> usize {
        self.targets
            .values()
            .filter(|registration| registration.active)
            .count()
    }

    fn rebuild_mailboxes(&mut self) {
        self.active_targets = self
            .targets
            .iter()
            .filter(|(_target_id, registration)| registration.active)
            .map(|(target_id, registration)| {
                ActiveRelayTarget::new(*target_id, registration.target.clone())
            })
            .collect::<Vec<_>>()
            .into();
    }
}

pub(super) type RelaySourceRegistration = RelayTargetRegistry<RelayTargetId, RelayTargetTransport>;

/// Source-indexed relay destinations owned by one packet loop.
///
/// A source worker keeps this state beside its normal local route table. For
/// every source packet, the forwarding planner can add local sends and relay
/// sends from the same source media id:
///
/// ```text
/// W0 RtcBootstrapState
///
///   source_media_id A
///        |
///        +--> local W0 consumers
///        |
///        +--> W1 RelayPacketMailbox
///        |
///        +--> W2 RelayPacketMailbox
/// ```
///
/// Relay sends use bounded `try_send`; overload drops are counted at the
/// packet-loop forwarding boundary instead of blocking the source worker.
impl RtcBootstrapState {
    #[cfg(test)]
    pub(super) fn relay_targets_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget<RelayTargetId, RelayTargetTransport>]> {
        self.packet_loop
            .relay_targets
            .get(&source_transport_media_id)
            .and_then(|registration| {
                registration
                    .has_active_targets()
                    .then(|| registration.active_targets_slice())
            })
    }

    pub(super) fn add_relay_target(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayTargetTransport,
    ) {
        self.packet_loop
            .relay_targets
            .entry(source_transport_media_id)
            .or_default()
            .add_target(target_id, target);
    }

    pub(super) fn remove_relay_target(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(source) = self
            .packet_loop
            .relay_targets
            .get_mut(&source_transport_media_id)
        else {
            return;
        };
        let removal = source.remove_target(target_id);
        if removal.source_empty {
            self.packet_loop
                .relay_targets
                .remove(&source_transport_media_id);
        }
        if removal.target_removed {
            self.packet_loop
                .route_control
                .forget_relay_packet_gate(source_transport_media_id, target_id);
        }
        if removal.active_set_changed {
            self.packet_loop.bump_relay_topology_generation();
        }
    }

    pub(super) fn set_relay_target_active(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        let Some(source_registration) = self
            .packet_loop
            .relay_targets
            .get_mut(&source_transport_media_id)
        else {
            return;
        };
        if source_registration.set_target_active(target_id, active) {
            self.packet_loop.bump_relay_topology_generation();
        }
    }

    pub(super) fn is_relay_target_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.packet_loop
            .relay_targets
            .get(&source_transport_media_id)
            .is_some_and(|source_registration| source_registration.is_target_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.packet_loop
            .relay_targets
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.packet_loop
            .relay_targets
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::active_target_count)
    }
}
