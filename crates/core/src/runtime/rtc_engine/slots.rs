//! worker-local generation slots for packet-loop identities
//!
//! this module separates public transport identity from the storage identity used
//! by one RTC worker
//! public ids like [`TransportSessionKey`] and
//! [`TransportMediaId`] stay stable at command, room and diagnostic boundaries
//! while packet-loop queues keep small generation-checked handles
//!
//! a slot handle is valid only while its slot generation still matches the entry
//! in the store
//! removing an entry increments the generation before the index is
//! reused, so delayed dirty work or timeout work becomes a no-op

use std::{collections::BTreeMap, marker::PhantomData};

use super::{media_registry::RegisteredMediaHandle, state::RtcSessionState};
use crate::runtime::media_transport::{TransportMediaId, TransportSessionKey};

/// handle namespace for live `RtcSessionState` entries
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SessionSlot;

/// handle namespace for registered media entries
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct MediaSlot;

/// handle namespace for downstream RTP rewrite state
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct ConsumerStreamSlot;

/// copy handle used by packet-loop queues to reach session state
pub(super) type SessionHandle = SlotHandle<SessionSlot>;

/// copy handle stored on route destinations for local RTP rewrite state
pub(super) type ConsumerStreamHandle = SlotHandle<ConsumerStreamSlot>;

/// session table keyed by the public session identity at the worker boundary
pub(super) type SessionStore = KeyedSlotStore<TransportSessionKey, RtcSessionState, SessionSlot>;

/// media table keyed by the stable transport media id exposed outside the worker
pub(super) type MediaStore = KeyedSlotStore<TransportMediaId, RegisteredMediaHandle, MediaSlot>;

/// copy identity for one reusable slot
///
/// callers may queue, copy and compare handles, but every access must go through
/// the owning store so generation checks can reject stale work after teardown or
/// replacement
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SlotHandle<Tag> {
    index: usize,
    generation: u64,
    _tag: PhantomData<fn() -> Tag>,
}

impl<Tag> Clone for SlotHandle<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> Copy for SlotHandle<Tag> {}

impl<Tag> Default for SlotHandle<Tag> {
    /// create an invalid handle that cannot match a live generation
    fn default() -> Self {
        Self {
            index: usize::MAX,
            generation: 0,
            _tag: PhantomData,
        }
    }
}

/// dense reusable storage for one worker-local identity class
///
/// indices are recycled after removal to keep handles small
/// generation checks make recycled indices safe for delayed packet-loop work
pub(super) struct SlotStore<T, Tag> {
    entries: Vec<SlotEntry<T>>,
    free: Vec<usize>,
    _tag: PhantomData<fn() -> Tag>,
}

impl<T, Tag> Default for SlotStore<T, Tag> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            _tag: PhantomData,
        }
    }
}

struct SlotEntry<T> {
    generation: u64,
    value: Option<T>,
}

impl<T, Tag> SlotStore<T, Tag> {
    /// allocate or reuse a slot and return the generation that names this value
    pub(super) fn insert(&mut self, value: T) -> SlotHandle<Tag> {
        let index = self.free.pop().unwrap_or_else(|| {
            let index = self.entries.len();
            self.entries.push(SlotEntry {
                generation: 1,
                value: None,
            });
            index
        });
        let Some(entry) = self.entries.get_mut(index) else {
            return SlotHandle::default();
        };
        entry.value = Some(value);
        SlotHandle {
            index,
            generation: entry.generation,
            _tag: PhantomData,
        }
    }

    /// return the value only when the handle still names the current generation
    pub(super) fn get(&self, handle: SlotHandle<Tag>) -> Option<&T> {
        let entry = self.entries.get(handle.index)?;
        (entry.generation == handle.generation)
            .then_some(entry.value.as_ref())
            .flatten()
    }

    /// return the mutable value only when the handle still names the current generation
    pub(super) fn get_mut(&mut self, handle: SlotHandle<Tag>) -> Option<&mut T> {
        let entry = self.entries.get_mut(handle.index)?;
        (entry.generation == handle.generation)
            .then_some(entry.value.as_mut())
            .flatten()
    }

    /// remove the value only when the handle still names the current generation
    ///
    /// successful removal invalidates every copied handle for the old occupant
    pub(super) fn remove(&mut self, handle: SlotHandle<Tag>) -> Option<T> {
        let entry = self.entries.get_mut(handle.index)?;
        if entry.generation != handle.generation {
            return None;
        }
        let value = entry.value.take()?;
        entry.generation = next_generation(entry.generation);
        self.free.push(handle.index);
        Some(value)
    }
}

/// public-key index backed by generation slots
///
/// use the key API at worker boundaries and convert to handles for queued work
/// the key is stored inside the slot so a live handle can be translated back when
/// the packet loop must report ready public sessions
pub(super) struct KeyedSlotStore<K, V, Tag> {
    by_key: BTreeMap<K, SlotHandle<Tag>>,
    slots: SlotStore<KeyedSlot<K, V>, Tag>,
}

struct KeyedSlot<K, V> {
    key: K,
    value: V,
}

impl<K, V, Tag> Default for KeyedSlotStore<K, V, Tag> {
    fn default() -> Self {
        Self {
            by_key: BTreeMap::new(),
            slots: SlotStore::default(),
        }
    }
}

impl<K: Ord + Clone, V, Tag> KeyedSlotStore<K, V, Tag> {
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.by_key.contains_key(key)
    }

    /// read by public key after validating the current slot generation
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.get_by_handle(self.handle_for_key(key)?)
    }

    /// mutably read by public key after validating the current slot generation
    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.get_mut_by_handle(self.handle_for_key(key)?)
    }

    /// replace the value for a public key and invalidate any old handle
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self
            .by_key
            .remove(&key)
            .and_then(|handle| self.slots.remove(handle))
            .map(|entry| entry.value);
        let handle = self.slots.insert(KeyedSlot {
            key: key.clone(),
            value,
        });
        self.by_key.insert(key, handle);
        previous
    }

    /// remove the value for a public key and invalidate its handle
    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let handle = self.by_key.remove(key)?;
        self.slots.remove(handle).map(|entry| entry.value)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.by_key.len()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
        self.by_key.keys()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.by_key
            .iter()
            .filter_map(|(key, handle)| self.slots.get(*handle).map(|entry| (key, &entry.value)))
    }

    /// translate a public key to the handle used by packet-loop queues
    pub(super) fn handle_for_key(&self, key: &K) -> Option<SlotHandle<Tag>> {
        self.by_key.get(key).copied()
    }

    /// translate a live handle back to its public key
    ///
    /// `None` means the handle is stale or invalid for this store
    pub(super) fn key_for_handle(&self, handle: SlotHandle<Tag>) -> Option<&K> {
        self.slots.get(handle).map(|entry| &entry.key)
    }

    /// read by handle after validating the current slot generation
    pub(super) fn get_by_handle(&self, handle: SlotHandle<Tag>) -> Option<&V> {
        self.slots.get(handle).map(|entry| &entry.value)
    }

    /// mutably read by handle after validating the current slot generation
    pub(super) fn get_mut_by_handle(&mut self, handle: SlotHandle<Tag>) -> Option<&mut V> {
        self.slots.get_mut(handle).map(|entry| &mut entry.value)
    }
}

fn next_generation(generation: u64) -> u64 {
    generation.wrapping_add(1).max(1)
}
