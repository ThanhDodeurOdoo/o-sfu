use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::mpsc;

use super::{forwarded_packet::ForwardedPacket, state::PacketLoopState};
use crate::engine::media_transport::TransportMediaId;

#[cfg(any(test, feature = "testing-transport"))]
mod test_support;

pub(super) const RELAY_MAILBOX_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayEnqueueOutcome {
    Enqueued,
    Overloaded,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RelayEnqueueReport {
    pub(super) outcome: RelayEnqueueOutcome,
    pub(super) mailbox_depth: usize,
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
        state: &PacketLoopState,
        packet: &ForwardedPacket,
        source_transport_media_id: TransportMediaId,
    ) -> Option<RelayEnqueueReport> {
        let outcome = forward_packet_to_target(state, &self.tx, packet, source_transport_media_id)?;
        Some(RelayEnqueueReport {
            outcome,
            mailbox_depth: self.backlog_depth(),
        })
    }

    pub(super) fn backlog_depth(&self) -> usize {
        sender_backlog_depth(&self.tx)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ActiveRelayTarget {
    pub(super) target_id: RelayTargetId,
    pub(super) target: RelayPacketMailbox,
}

fn forward_packet_to_target(
    state: &PacketLoopState,
    tx: &mpsc::Sender<ForwardedPacket>,
    packet: &ForwardedPacket,
    source_transport_media_id: TransportMediaId,
) -> Option<RelayEnqueueOutcome> {
    let packet = packet.share_for_relay(state, source_transport_media_id)?;
    Some(match tx.try_send(packet) {
        Ok(()) => RelayEnqueueOutcome::Enqueued,
        Err(mpsc::error::TrySendError::Full(_packet)) => RelayEnqueueOutcome::Overloaded,
        Err(mpsc::error::TrySendError::Closed(_packet)) => RelayEnqueueOutcome::Closed,
    })
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
struct RelayTargetRegistration {
    target: RelayPacketMailbox,
    active: bool,
}

/// Source-worker cache for relay target handles and active bits.
#[derive(Debug, Clone, Default)]
pub(super) struct RelaySourceRegistration {
    targets: BTreeMap<RelayTargetId, RelayTargetRegistration>,
    active_targets: Arc<[ActiveRelayTarget]>,
}

impl RelaySourceRegistration {
    pub(super) fn add_target(&mut self, target_id: RelayTargetId, target: RelayPacketMailbox) {
        self.targets
            .entry(target_id)
            .or_insert(RelayTargetRegistration {
                target,
                active: false,
            });
    }

    pub(super) fn remove_target(&mut self, target_id: RelayTargetId) -> bool {
        self.targets.remove(&target_id);
        self.rebuild_mailboxes();
        self.targets.is_empty()
    }

    pub(super) fn set_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        let Some(registration) = self.targets.get_mut(&target_id) else {
            return;
        };
        if registration.active != active {
            registration.active = active;
            self.rebuild_mailboxes();
        }
    }

    #[must_use]
    pub(super) fn active_targets(&self) -> &[ActiveRelayTarget] {
        &self.active_targets
    }

    #[must_use]
    pub(super) fn has_active_targets(&self) -> bool {
        !self.active_targets.is_empty()
    }

    pub(super) fn contains_target(&self, target_id: RelayTargetId) -> bool {
        self.targets.contains_key(&target_id)
    }

    pub(super) fn is_target_active(&self, target_id: RelayTargetId) -> bool {
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
            .map(|(target_id, registration)| ActiveRelayTarget {
                target_id: *target_id,
                target: registration.target.clone(),
            })
            .collect();
    }
}

/// Source-indexed relay destinations owned by one packet loop.
///
/// A source worker keeps this state beside its normal local route table. For
/// every source packet, the forwarding planner can add local sends and relay
/// sends from the same source media id:
///
/// ```text
/// W0 PacketLoopState
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
impl PacketLoopState {
    pub(super) fn relay_targets_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget]> {
        self.relay_targets
            .get(&source_transport_media_id)
            .and_then(|registration| {
                registration
                    .has_active_targets()
                    .then(|| registration.active_targets())
            })
    }

    pub(super) fn add_relay_target(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) {
        self.relay_targets
            .entry(source_transport_media_id)
            .or_default()
            .add_target(target_id, target);
    }

    pub(super) fn remove_relay_target(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(source) = self.relay_targets.get_mut(&source_transport_media_id) else {
            return;
        };
        let had_target = source.contains_target(target_id);
        let remove_source = source.remove_target(target_id);
        let target_removed = had_target && !source.contains_target(target_id);
        if remove_source {
            self.relay_targets.remove(&source_transport_media_id);
        }
        if target_removed {
            self.route_control
                .forget_relay_packet_gate(source_transport_media_id, target_id);
        }
    }

    pub(super) fn set_relay_target_active(
        &mut self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        let Some(source_registration) = self.relay_targets.get_mut(&source_transport_media_id)
        else {
            return;
        };
        source_registration.set_target_active(target_id, active);
    }

    pub(super) fn is_relay_target_active(
        &self,
        source_transport_media_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.relay_targets
            .get(&source_transport_media_id)
            .is_some_and(|source_registration| source_registration.is_target_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_targets
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count_for_source(
        &self,
        source_transport_media_id: TransportMediaId,
    ) -> usize {
        self.relay_targets
            .get(&source_transport_media_id)
            .map_or(0, RelaySourceRegistration::active_target_count)
    }
}
