use std::{collections::BTreeSet, mem, net::SocketAddr, sync::Arc, time::Instant};

use arc_swap::ArcSwap;

use super::{
    bitrate::{BitrateRegistry, MediaBitrateCounter},
    state::{RtcSnapshotState, TransportSessionHealth},
};
use crate::runtime::{
    RoomInstanceId,
    media_transport::{
        SourcePolicySignal, TransportMediaId, TransportQualitySample, TransportSessionKey,
    },
};

#[derive(Clone)]
pub(super) struct RtcObservationPublishers {
    bitrate_registry: Arc<ArcSwap<BitrateRegistry>>,
    snapshot_state: Arc<ArcSwap<RtcSnapshotState>>,
}

impl RtcObservationPublishers {
    pub(super) fn new() -> Self {
        Self {
            bitrate_registry: Arc::new(ArcSwap::from_pointee(BitrateRegistry::default())),
            snapshot_state: Arc::new(ArcSwap::from_pointee(RtcSnapshotState::default())),
        }
    }

    pub(super) fn bitrate_registry(&self) -> Arc<ArcSwap<BitrateRegistry>> {
        Arc::clone(&self.bitrate_registry)
    }

    pub(super) fn snapshot_state(&self) -> Arc<ArcSwap<RtcSnapshotState>> {
        Arc::clone(&self.snapshot_state)
    }
}

pub(super) struct PacketLoopObservations {
    bitrate_registry: BitrateRegistry,
    snapshot_state: RtcSnapshotState,
    publishers: RtcObservationPublishers,
    dirty_source_policy_rooms: BTreeSet<RoomInstanceId>,
    bitrate_dirty: bool,
    snapshot_dirty: bool,
}

impl PacketLoopObservations {
    pub(super) fn new(publishers: RtcObservationPublishers) -> Self {
        Self {
            bitrate_registry: BitrateRegistry::default(),
            snapshot_state: RtcSnapshotState::default(),
            publishers,
            dirty_source_policy_rooms: BTreeSet::new(),
            bitrate_dirty: false,
            snapshot_dirty: false,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_state(&self) -> &RtcSnapshotState {
        &self.snapshot_state
    }

    pub(super) fn add_session(&mut self, session_key: &TransportSessionKey) {
        if self.snapshot_state.add_session(session_key) {
            self.snapshot_dirty = true;
        }
    }

    pub(super) fn remove_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let (previous, changed) = self.snapshot_state.remove_session(session_key);
        if changed {
            self.snapshot_dirty = true;
        }
        if self.bitrate_registry.remove_session(session_key) {
            self.bitrate_dirty = true;
        }
        previous
    }

    pub(super) fn set_transport_health(
        &mut self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) -> Option<TransportSessionHealth> {
        let previous = self
            .snapshot_state
            .set_transport_health(session_key, health);
        if previous != Some(health) {
            self.snapshot_dirty = true;
        }
        previous
    }

    pub(super) fn set_receiver_bandwidth(
        &mut self,
        session_key: &TransportSessionKey,
        estimate: crate::Bitrate,
    ) {
        if self
            .snapshot_state
            .set_receiver_bandwidth(session_key, estimate)
            == Some(estimate)
        {
            return;
        }
        self.snapshot_dirty = true;
        self.dirty_source_policy_rooms
            .insert(session_key.room_instance_id());
    }

    pub(super) fn update_transport_quality(
        &mut self,
        session_key: &TransportSessionKey,
        update: impl FnOnce(&mut TransportQualitySample),
    ) {
        self.snapshot_state
            .update_transport_quality(session_key, update);
        self.snapshot_dirty = true;
    }

    pub(super) fn remember_remote_addr(
        &mut self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> bool {
        let changed = self
            .snapshot_state
            .remote_addr_demux
            .remember_remote_addr(source_addr, session_key);
        if changed {
            self.snapshot_dirty = true;
        }
        changed
    }

    pub(super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        if self
            .snapshot_state
            .remote_addr_demux
            .forget_remote_addr(source_addr)
        {
            self.snapshot_dirty = true;
        }
    }

    pub(super) fn register_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Arc<MediaBitrateCounter> {
        let counter =
            self.bitrate_registry
                .register_incoming_media(session_key, transport_media_id, now);
        self.bitrate_dirty = true;
        counter
    }

    pub(super) fn remove_incoming_media(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) {
        if self
            .bitrate_registry
            .remove_incoming_media(session_key, transport_media_id)
        {
            self.bitrate_dirty = true;
        }
    }

    pub(super) fn register_session_egress(
        &mut self,
        session_key: &TransportSessionKey,
        now: Instant,
    ) -> Arc<MediaBitrateCounter> {
        let (counter, inserted) = self
            .bitrate_registry
            .register_session_egress(session_key, now);
        if inserted {
            self.bitrate_dirty = true;
        }
        counter
    }

    pub(super) fn publish_dirty_and_wake_policy(
        &mut self,
        source_policy_signal: &SourcePolicySignal,
    ) {
        if self.bitrate_dirty {
            self.publishers
                .bitrate_registry
                .store(Arc::new(self.bitrate_registry.clone()));
            self.bitrate_dirty = false;
        }
        if self.snapshot_dirty {
            self.publishers
                .snapshot_state
                .store(Arc::new(self.snapshot_state.clone()));
            self.snapshot_dirty = false;
        }
        if self.dirty_source_policy_rooms.is_empty() {
            return;
        }
        let dirty_source_policy_rooms = mem::take(&mut self.dirty_source_policy_rooms);
        source_policy_signal.mark_dirty_rooms(dirty_source_policy_rooms);
    }
}
