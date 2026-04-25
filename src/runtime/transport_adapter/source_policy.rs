//! Transport-side source-policy wake coordination.
//!
//! The room layer own the actual source-packet selection policy: it decides,
//! from room membership and publication state, which producer layers should stay
//! routable. The transport layer in the other hand own observations such as active-speaker
//! activity and expiry deadlines that can change wihtout any room mutation. That
//! split means the runtime needs one narrow bridge from transport-side observation
//! changes back into the room-side policy sync task.
//!
//! This module is that bridge. It deliberatley does not store the policy itself,
//! any room state, or any queued work items. Instead it exposes one coalescing
//! dirty bit, a coalesced room-id set, and a wake primitive:
//!
//! - `SourcePolicyDirtyState` tracks whether at least one transport-side change
//!   happened since the last policy sync
//! - `SourcePolicySignal::mark_dirty()` records the affected room instance id,
//!   transitions the state from clean to dirty, and only wakes the listener on
//!   that edge.
//! - `SourcePolicyUpdateSubscription::wait_for_update()` consumes the dirty state
//!   plus the coalesced room-id set before sleeping again, so a previously
//!   observed update cannot be lost if it races with the wait path
//!
//! The important property is coalescingL: Multiple RTP packets can arrive
//! while the room sync task is still busy, but those packets do not need
//! an equal number of wakeups or replayed jobs. The room task only needs to
//! know which room instance ids changed and then re-read the current
//! transport-owned observation state from the adapter. This keeps the signaling
//! between packet-loop activity and policy recomputation bounded and avoids
//! turning hot-path packet observation into an unbounded event queue.
//!
//! Tokio's `Notify` handles the async wakeup in production, while the dirty-bit
//! logic remains separately (so it can be tested in Loom)
use std::{
    collections::BTreeSet,
    mem,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};

use tokio::sync::Notify;

use crate::runtime::RoomInstanceId;

#[derive(Debug, Default)]
pub(crate) struct SourcePolicyDirtyState {
    dirty: AtomicBool,
}

impl SourcePolicyDirtyState {
    pub(crate) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn mark_dirty(&self) -> bool {
        !self.dirty.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

#[derive(Debug, Default)]
struct DirtyRoomRegistry {
    room_instance_ids: Mutex<BTreeSet<RoomInstanceId>>,
}

impl DirtyRoomRegistry {
    fn insert(&self, room_instance_id: RoomInstanceId) {
        let mut dirty_rooms = self
            .room_instance_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        dirty_rooms.insert(room_instance_id);
    }

    fn insert_many(&self, room_instance_ids: impl IntoIterator<Item = RoomInstanceId>) -> bool {
        let mut dirty_rooms = self
            .room_instance_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut inserted_any = false;
        for room_instance_id in room_instance_ids {
            dirty_rooms.insert(room_instance_id);
            inserted_any = true;
        }
        drop(dirty_rooms);
        inserted_any
    }

    fn drain(&self) -> BTreeSet<RoomInstanceId> {
        let mut dirty_rooms = self
            .room_instance_ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        mem::take(&mut *dirty_rooms)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourcePolicyUpdateSubscription {
    dirty: Arc<SourcePolicyDirtyState>,
    dirty_rooms: Arc<DirtyRoomRegistry>,
    notify: Arc<Notify>,
}

impl SourcePolicyUpdateSubscription {
    pub(crate) async fn wait_for_update(&self) -> BTreeSet<RoomInstanceId> {
        loop {
            if self.dirty.take_dirty() {
                return self.dirty_rooms.drain();
            }
            self.notify.notified().await;
        }
    }

    pub(crate) fn take_pending_updates(&self) -> BTreeSet<RoomInstanceId> {
        if self.dirty.take_dirty() {
            return self.dirty_rooms.drain();
        }
        BTreeSet::new()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SourcePolicySignal {
    dirty: Arc<SourcePolicyDirtyState>,
    dirty_rooms: Arc<DirtyRoomRegistry>,
    notify: Arc<Notify>,
}

impl SourcePolicySignal {
    #[must_use]
    pub(crate) fn subscribe(&self) -> SourcePolicyUpdateSubscription {
        SourcePolicyUpdateSubscription {
            dirty: Arc::clone(&self.dirty),
            dirty_rooms: Arc::clone(&self.dirty_rooms),
            notify: Arc::clone(&self.notify),
        }
    }

    pub(crate) fn mark_dirty(&self, room_instance_id: RoomInstanceId) {
        self.dirty_rooms.insert(room_instance_id);
        if self.dirty.mark_dirty() {
            self.notify.notify_one();
        }
    }

    pub(crate) fn mark_dirty_rooms(
        &self,
        room_instance_ids: impl IntoIterator<Item = RoomInstanceId>,
    ) {
        if !self.dirty_rooms.insert_many(room_instance_ids) {
            return;
        }
        if self.dirty.mark_dirty() {
            self.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc, time::Duration};

    use tokio::{task::yield_now, time::timeout};

    use super::SourcePolicySignal;
    use crate::runtime::RoomInstanceId;

    #[tokio::test]
    async fn wait_for_update_observes_dirty_state_marked_before_wait() {
        let signal = SourcePolicySignal::default();
        let subscription = signal.subscribe();
        signal.mark_dirty(RoomInstanceId::from_raw(7));

        let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
        assert_eq!(
            updates.ok(),
            Some(BTreeSet::from([RoomInstanceId::from_raw(7)]))
        );
    }

    #[tokio::test]
    async fn wait_for_update_wakes_task_after_cross_task_mark_dirty() {
        let signal = Arc::new(SourcePolicySignal::default());
        let subscription = signal.subscribe();

        let waiter = tokio::spawn(async move {
            timeout(Duration::from_secs(1), subscription.wait_for_update())
                .await
                .ok()
        });

        yield_now().await;
        signal.mark_dirty(RoomInstanceId::from_raw(9));

        let waiter_result = waiter.await;
        assert!(waiter_result.is_ok());
        let Ok(woke) = waiter_result else {
            return;
        };
        assert_eq!(woke, Some(BTreeSet::from([RoomInstanceId::from_raw(9)])));
    }

    #[tokio::test]
    async fn wait_for_update_coalesces_multiple_dirty_marks_per_channel() {
        let signal = SourcePolicySignal::default();
        let subscription = signal.subscribe();
        signal.mark_dirty(RoomInstanceId::from_raw(4));
        signal.mark_dirty(RoomInstanceId::from_raw(4));
        signal.mark_dirty(RoomInstanceId::from_raw(6));

        let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
        assert_eq!(
            updates.ok(),
            Some(BTreeSet::from([
                RoomInstanceId::from_raw(4),
                RoomInstanceId::from_raw(6),
            ]))
        );
    }

    #[tokio::test]
    async fn wait_for_update_observes_batch_dirty_marks() {
        let signal = SourcePolicySignal::default();
        let subscription = signal.subscribe();
        signal.mark_dirty_rooms([
            RoomInstanceId::from_raw(8),
            RoomInstanceId::from_raw(8),
            RoomInstanceId::from_raw(10),
        ]);

        let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
        assert_eq!(
            updates.ok(),
            Some(BTreeSet::from([
                RoomInstanceId::from_raw(8),
                RoomInstanceId::from_raw(10),
            ]))
        );
    }
}
