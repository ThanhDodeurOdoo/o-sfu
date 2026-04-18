use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::time::rfc3339_now;

use super::Channel;

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

#[derive(Debug, Clone)]
pub(crate) struct ChannelDirectoryEntry {
    channel: Arc<Channel>,
    lifecycle_lock: Arc<Mutex<()>>,
    create_date: String,
    remote_address: String,
}

impl ChannelDirectoryEntry {
    fn new(channel: Arc<Channel>, remote_address: Option<&str>) -> Self {
        Self {
            channel,
            lifecycle_lock: Arc::new(Mutex::new(())),
            create_date: rfc3339_now(),
            remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
        }
    }

    #[must_use]
    pub(crate) fn channel(&self) -> Arc<Channel> {
        Arc::clone(&self.channel)
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
pub(crate) struct ChannelDirectory {
    channels_by_uuid: BTreeMap<String, ChannelDirectoryEntry>,
    uuids_by_issuer: BTreeMap<String, String>,
}

impl ChannelDirectory {
    #[must_use]
    pub(crate) fn get_by_issuer(&self, issuer: &str) -> Option<Arc<Channel>> {
        let uuid = self.uuids_by_issuer.get(issuer)?;
        self.get_by_uuid(uuid)
    }

    #[must_use]
    pub(crate) fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Channel>> {
        self.channels_by_uuid
            .get(uuid)
            .map(ChannelDirectoryEntry::channel)
    }

    #[must_use]
    pub(crate) fn entry(&self, uuid: &str) -> Option<ChannelDirectoryEntry> {
        self.channels_by_uuid.get(uuid).cloned()
    }

    #[must_use]
    pub(crate) fn entries(&self) -> Vec<ChannelDirectoryEntry> {
        self.channels_by_uuid.values().cloned().collect()
    }

    pub(crate) fn insert(&mut self, channel: Arc<Channel>, remote_address: Option<&str>) {
        let channel_uuid = channel.uuid().to_owned();
        self.uuids_by_issuer
            .insert(channel.issuer().to_owned(), channel_uuid.clone());
        self.channels_by_uuid.insert(
            channel_uuid,
            ChannelDirectoryEntry::new(channel, remote_address),
        );
    }

    #[must_use]
    pub(crate) fn contains_current(&self, uuid: &str, channel: &Arc<Channel>) -> bool {
        self.channels_by_uuid
            .get(uuid)
            .is_some_and(|entry| Arc::ptr_eq(&entry.channel, channel))
    }

    pub(crate) fn remove_if_current(&mut self, uuid: &str, channel: &Arc<Channel>) -> bool {
        let Some(entry) = self.channels_by_uuid.get(uuid) else {
            return false;
        };
        if !Arc::ptr_eq(&entry.channel, channel) {
            return false;
        }
        self.channels_by_uuid.remove(uuid);
        self.uuids_by_issuer.remove(channel.issuer());
        true
    }
}
