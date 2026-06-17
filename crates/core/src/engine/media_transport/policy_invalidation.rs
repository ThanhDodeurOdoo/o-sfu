//! transport-side source-policy invalidation/wake coordination
//!
//! `Room` owns source-packet selection policy
//! packet loops only observe facts that can make that policy stale,
//! like receiver bandwidth and active speaker state
//!
//! this module is the bounded wake bridge between those two layers
//! it carries no room state and no policy result
//! it only records which room instances need another policy pass, then wakes
//! the room-side sync task once for a clean to dirty transition
//!
//! many packet-loop observations can arrive while the room sync task is busy
//! the task only needs the affected room instance ids because policy sync
//! re-reads current transport snapshots before recomputing policy
//! this keeps hot-path observation from becoming an unbounded job queue, so
//! these info are coalesced
//!
//! ordering is important: callers record room ids before setting the dirty bit so a waiter that
//! observes the dirty edge can drain the matching room set without losing a
//! pre-existing update
use std::{
    collections::BTreeSet,
    mem,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::Notify;

use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

/// shared dirty bit for coalesced source-policy wakeups
///
/// the bit represents "at least one transport observation changed since the
/// last drain"
/// it stays separate from the room-id registry so Loom and unit
/// tests can verify the wake edge without depending on `Notify`
#[derive(Debug, Default)]
pub struct SourcePolicyDirtyState {
    dirty: AtomicBool,
}

impl SourcePolicyDirtyState {
    /// inspect the dirty state without consuming it
    #[must_use]
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// consume the current dirty edge
    ///
    /// callers that receive `true` must drain the room-id registry before
    /// sleeping again
    /// this preserves the pairing between the dirty edge and the affected room
    /// instances
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    /// mark the state dirty and report whether a wakeup is needed
    ///
    /// returns `true` only for the clean to dirty transition
    /// callers use that edge to avoid waking the policy task for every
    /// packet-loop observation
    pub fn mark_dirty(&self) -> bool {
        !self.dirty.swap(true, Ordering::AcqRel)
    }
}

/// coalesced room instances that need source-policy recomputation
///
/// the registry is a cold-path companion to the dirty bit
/// packet-loop code batches and deduplicates before publishing when it can,
/// while this set keeps duplicate room ids harmless for direct callers
#[derive(Debug, Default)]
struct DirtyRoomRegistry {
    room_instance_ids: Mutex<BTreeSet<RoomInstanceId>>,
}

impl DirtyRoomRegistry {
    fn insert_many(&self, room_instance_ids: impl IntoIterator<Item = RoomInstanceId>) -> bool {
        let mut dirty_rooms = lock_unpoisoned(&self.room_instance_ids);
        let mut saw_room = false;
        for room_instance_id in room_instance_ids {
            dirty_rooms.insert(room_instance_id);
            saw_room = true;
        }
        drop(dirty_rooms);
        saw_room
    }

    fn drain(&self) -> BTreeSet<RoomInstanceId> {
        let mut dirty_rooms = lock_unpoisoned(&self.room_instance_ids);
        mem::take(&mut *dirty_rooms)
    }
}

/// receiver side of the source-policy wake bridge
///
/// a subscription is owned by the room-side policy sync task
/// it drains coalesced room ids after a dirty edge, then the task asks the
/// media transport for fresh observations before recomputing policy
#[derive(Debug, Clone)]
pub struct SourcePolicyUpdateSubscription {
    dirty: Arc<SourcePolicyDirtyState>,
    dirty_rooms: Arc<DirtyRoomRegistry>,
    notify: Arc<Notify>,
}

impl SourcePolicyUpdateSubscription {
    /// wait until at least one room has transport-observed source-policy input
    ///
    /// updates marked before this method starts are observed before sleeping
    /// because the dirty bit is checked first
    /// updates that race with the wait path either set the bit before the next
    /// check or wake the `Notify` listener
    pub async fn wait_for_update(&self) -> BTreeSet<RoomInstanceId> {
        loop {
            if self.dirty.take_dirty() {
                return self.dirty_rooms.drain();
            }
            self.notify.notified().await;
        }
    }

    /// drain pending room ids without waiting
    ///
    /// manager-level loops use this after processing expiry deadlines so they
    /// can merge timer-driven and packet-driven policy work into one room pass
    #[must_use]
    pub fn take_pending_updates(&self) -> BTreeSet<RoomInstanceId> {
        if self.dirty.take_dirty() {
            return self.dirty_rooms.drain();
        }
        BTreeSet::new()
    }
}

/// sender side of the source-policy wake bridge
///
/// packet loops and deterministic test transports share this signal
/// callers report room instances whose transport observations changed
/// the signal coalesces duplicates and wakes one room-side listener when the
/// bridge moves from clean to dirty
#[derive(Debug, Default)]
pub struct SourcePolicySignal {
    dirty: Arc<SourcePolicyDirtyState>,
    dirty_rooms: Arc<DirtyRoomRegistry>,
    notify: Arc<Notify>,
}

impl SourcePolicySignal {
    /// create a subscription for the room-side policy sync task
    ///
    /// subscriptions share the same dirty bit and room registry
    /// the runtime expects one active waiter for normal operation
    #[must_use]
    pub fn subscribe(&self) -> SourcePolicyUpdateSubscription {
        SourcePolicyUpdateSubscription {
            dirty: Arc::clone(&self.dirty),
            dirty_rooms: Arc::clone(&self.dirty_rooms),
            notify: Arc::clone(&self.notify),
        }
    }

    /// mark one room instance as needing a source-policy refresh
    ///
    /// duplicate marks are cheap
    /// only the first mark after a drain wakes the subscription
    pub fn mark_dirty(&self, room_instance_id: RoomInstanceId) {
        self.mark_dirty_rooms([room_instance_id]);
    }

    /// mark a batch of room instances as needing a source-policy refresh
    ///
    /// empty batches do not set the dirty bit
    /// duplicate room ids collapse into one entry before the next drain
    pub fn mark_dirty_rooms(&self, room_instance_ids: impl IntoIterator<Item = RoomInstanceId>) {
        if !self.dirty_rooms.insert_many(room_instance_ids) {
            return;
        }
        if self.dirty.mark_dirty() {
            self.notify.notify_one();
        }
    }
}

#[cfg(test)]
#[path = "TESTS/policy_invalidation.rs"]
mod tests;
