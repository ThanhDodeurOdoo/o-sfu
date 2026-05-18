//! current-room directory indexes and lifecycle lease state
//!
//! the directory owns process-local lookup aliases for live rooms plus the
//! lease state that prevents empty-room removal from racing accepted work

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::Room;
use crate::runtime::{RoomInstanceId, sync::lock_unpoisoned};

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

fn rfc3339_now() -> String {
    match OffsetDateTime::now_utc().format(&Rfc3339) {
        Ok(timestamp) => timestamp,
        Err(_error) => String::from("1970-01-01T00:00:00Z"),
    }
}

/// directory row for one current room instance
///
/// cloned entries carry the same lifecycle gate as the live directory row, so
/// a manager snapshot can accept work without keeping the directory lock held
#[derive(Debug, Clone)]
pub(crate) struct RoomDirectoryEntry {
    room: Arc<Room>,
    lifecycle: RoomLifecycle,
    create_date: String,
    remote_address: String,
}

impl RoomDirectoryEntry {
    fn new(room: Arc<Room>, remote_address: Option<&str>) -> Self {
        Self {
            room,
            lifecycle: RoomLifecycle::default(),
            create_date: rfc3339_now(),
            remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
        }
    }

    #[must_use]
    pub(crate) fn room(&self) -> Arc<Room> {
        Arc::clone(&self.room)
    }

    #[must_use]
    pub(crate) fn lifecycle(&self) -> RoomLifecycle {
        self.lifecycle.clone()
    }

    #[must_use]
    pub(crate) fn create_date(&self) -> &str {
        &self.create_date
    }

    #[must_use]
    pub(crate) fn remote_address(&self) -> &str {
        &self.remote_address
    }
}

/// mutable state behind one directory entry's lifecycle lease gate
///
/// this lock is intentionally synchronous and short lived
/// callers may hold a
/// [`RoomLifecycleLease`] while awaiting, but this mutex is only held while a
/// lease is accepted or released
#[derive(Debug, Default)]
struct RoomLifecycleState {
    /// accepted room work that has not finished or been dropped
    active_mutations: usize,
    /// empty-room removal request waiting for accepted work to drain
    remove_when_idle: bool,
    /// terminal marker set once one finisher wins directory removal
    closing: bool,
}

/// cloneable admission gate for the current room stored in one directory row
///
/// this type coordinates manager-level liveness only
/// room membership ordering
/// remains owned by [`Room`] and its state transition methods
#[derive(Debug, Clone, Default)]
pub(crate) struct RoomLifecycle {
    state: Arc<Mutex<RoomLifecycleState>>,
}

impl RoomLifecycle {
    /// accept a new current-room operation
    ///
    /// `None` means empty-room removal is pending or already won
    /// callers must
    /// still validate that the room pointer is current after acquiring the lease
    #[must_use]
    pub(crate) fn begin(&self) -> Option<RoomLifecycleLease> {
        let mut state = lock_unpoisoned(&self.state);
        if state.closing || state.remove_when_idle {
            return None;
        }
        state.active_mutations = state.active_mutations.checked_add(1)?;
        let lease = RoomLifecycleLease {
            state: Arc::clone(&self.state),
            finished: false,
        };
        drop(state);
        Some(lease)
    }
}

/// cancellation-safe permit for work accepted against a directory entry
///
/// dropping the lease releases admission without requesting removal
/// manager
/// teardown paths call [`Self::finish`] after checking whether the room is empty
/// with no pending cleanup retries
#[derive(Debug)]
pub(crate) struct RoomLifecycleLease {
    /// shared lease state for the directory entry that accepted this work
    state: Arc<Mutex<RoomLifecycleState>>,
    /// guards against double release when `finish`, `cancel` or `Drop` overlap
    finished: bool,
}

impl RoomLifecycleLease {
    /// release a lease that was accepted for a stale directory row
    ///
    /// this is separate from `Drop` so stale-current validation can be explicit
    /// at the manager boundary
    pub(crate) fn cancel(mut self) {
        let _ = self.release(false, false);
    }

    /// release a lease after the accepted operation has finished
    ///
    /// returns `true` for the single caller that should remove the directory
    /// entry
    /// `room_can_be_removed` must be computed after the caller future
    /// finished, because cleanup retries can change while work is running
    #[must_use]
    pub(crate) fn finish(mut self, remove_if_empty: bool, room_can_be_removed: bool) -> bool {
        self.release(remove_if_empty, room_can_be_removed)
    }

    #[must_use]
    fn release(&mut self, remove_if_empty: bool, room_can_be_removed: bool) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        let mut state = lock_unpoisoned(&self.state);
        if state.active_mutations > 0 {
            state.active_mutations -= 1;
        }
        if remove_if_empty && room_can_be_removed {
            state.remove_when_idle = true;
        }
        let should_remove = if state.active_mutations == 0 && state.remove_when_idle {
            if room_can_be_removed {
                state.closing = true;
                true
            } else {
                state.remove_when_idle = false;
                false
            }
        } else {
            false
        };
        drop(state);
        should_remove
    }
}

impl Drop for RoomLifecycleLease {
    fn drop(&mut self) {
        let _ = self.release(false, false);
    }
}

#[derive(Debug, Default)]
pub(crate) struct RoomDirectory {
    rooms_by_uuid: BTreeMap<String, RoomDirectoryEntry>,
    uuids_by_instance_id: BTreeMap<RoomInstanceId, String>,
    uuids_by_issuer: BTreeMap<String, String>,
}

impl RoomDirectory {
    #[must_use]
    pub(crate) fn get_by_issuer(&self, issuer: &str) -> Option<Arc<Room>> {
        let uuid = self.uuids_by_issuer.get(issuer)?;
        self.get_by_uuid(uuid)
    }

    #[must_use]
    pub(crate) fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        self.rooms_by_uuid.get(uuid).map(RoomDirectoryEntry::room)
    }

    #[must_use]
    pub(crate) fn entry(&self, uuid: &str) -> Option<RoomDirectoryEntry> {
        self.rooms_by_uuid.get(uuid).cloned()
    }

    #[must_use]
    pub(crate) fn entry_by_instance_id(
        &self,
        room_instance_id: RoomInstanceId,
    ) -> Option<RoomDirectoryEntry> {
        let uuid = self.uuids_by_instance_id.get(&room_instance_id)?;
        self.entry(uuid)
    }

    #[must_use]
    pub(crate) fn entries(&self) -> Vec<RoomDirectoryEntry> {
        self.rooms_by_uuid.values().cloned().collect()
    }

    pub(crate) fn insert(&mut self, room: Arc<Room>, remote_address: Option<&str>) {
        let room_id = room.uuid().to_owned();
        self.uuids_by_issuer
            .insert(room.issuer().to_owned(), room_id.clone());
        self.uuids_by_instance_id
            .insert(room.instance_id(), room_id.clone());
        self.rooms_by_uuid
            .insert(room_id, RoomDirectoryEntry::new(room, remote_address));
    }

    #[must_use]
    pub(crate) fn contains_current(&self, uuid: &str, room: &Arc<Room>) -> bool {
        self.rooms_by_uuid
            .get(uuid)
            .is_some_and(|entry| Arc::ptr_eq(&entry.room, room))
    }

    pub(crate) fn remove_if_current(&mut self, uuid: &str, room: &Arc<Room>) -> bool {
        let Some(entry) = self.rooms_by_uuid.get(uuid) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.room, room) {
            return false;
        }
        self.rooms_by_uuid.remove(uuid);
        self.uuids_by_issuer.remove(room.issuer());
        self.uuids_by_instance_id.remove(&room.instance_id());
        true
    }
}
