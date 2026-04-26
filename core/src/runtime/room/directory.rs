use std::{collections::BTreeMap, sync::Arc};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;

use super::Room;
use crate::runtime::RoomInstanceId;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

fn rfc3339_now() -> String {
    match OffsetDateTime::now_utc().format(&Rfc3339) {
        Ok(timestamp) => timestamp,
        Err(_error) => String::from("1970-01-01T00:00:00Z"),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RoomDirectoryEntry {
    room: Arc<Room>,
    lifecycle_lock: Arc<Mutex<()>>,
    create_date: String,
    remote_address: String,
}

impl RoomDirectoryEntry {
    fn new(room: Arc<Room>, remote_address: Option<&str>) -> Self {
        Self {
            room,
            lifecycle_lock: Arc::new(Mutex::new(())),
            create_date: rfc3339_now(),
            remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
        }
    }

    #[must_use]
    pub(crate) fn room(&self) -> Arc<Room> {
        Arc::clone(&self.room)
    }

    #[must_use]
    pub(crate) fn lifecycle_lock(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.lifecycle_lock)
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
