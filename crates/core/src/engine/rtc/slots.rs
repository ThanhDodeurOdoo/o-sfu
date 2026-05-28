//! worker-local slots for packet-loop identities
//!
//! slots solve the mismatch between stable transport ids and packet-loop work
//!
//! ```text
//! room commands, diagnostics and teardown
//!   use stable public keys such as TransportSessionKey or TransportMediaId
//!   those keys are meaningful outside one media worker
//!
//! packet-loop queues, timeout heaps and route destinations
//!   use tiny copy handles
//!   those handles are only meaningful inside the store that created them
//! ```
//!
//! the worker must accept that some work is already queued when a session,
//! media entry or consumer route is removed
//! a bare index would let that delayed work reach a replacement occupant after
//! the index is recycled
//! [`SlotHandle`] prevents that by pairing the index with the generation that was
//! current when the handle was created
//! removal advances the generation before reuse, so stale dirty-session marks,
//! timeout heap entries and route destinations become ordinary no-ops instead
//! of touching new state
//!
//! the tag parameter is part of the contract
//! it keeps session handles, media handles and consumer stream handles in
//! separate type namespaces even though every handle is represented by the same
//! compact index plus generation pair

use std::{collections::BTreeMap, marker::PhantomData};

use super::{media_registry::RegisteredMediaHandle, state::RtcSessionState};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey};

/// handle namespace for live `RtcSessionState` entries
///
/// session handles are allowed to live in scheduler queues after the public
/// session key has been removed
/// the store validates the generation before the packet loop polls a session
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SessionSlot;

/// handle namespace for registered media entries
///
/// media slots keep transport media lookup state worker-local while room and
/// diagnostics paths continue to name media by [`TransportMediaId`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct MediaSlot;

/// handle namespace for downstream RTP rewrite state
///
/// route destinations carry these handles so per-packet local forwarding can
/// reach receiver-local rewrite state without rebuilding a lookup key
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct ConsumerStreamSlot;

/// generation-checked session handle used by packet-loop scheduler queues
pub(super) type SessionHandle = SlotHandle<SessionSlot>;

/// generation-checked handle stored on route destinations for local RTP rewrite state
pub(super) type ConsumerStreamHandle = SlotHandle<ConsumerStreamSlot>;

/// session table keyed by the public session identity at the worker boundary
///
/// commands enter through [`TransportSessionKey`]
/// the packet loop converts that key into [`SessionHandle`] only for queued work
pub(super) type SessionStore = KeyedSlotStore<TransportSessionKey, RtcSessionState, SessionSlot>;

/// media table keyed by the stable transport media id exposed outside the worker
pub(super) type MediaStore = KeyedSlotStore<TransportMediaId, RegisteredMediaHandle, MediaSlot>;

/// copy identity for one reusable worker-local slot
///
/// callers may queue, copy and compare handles freely because the value is only
/// an access token
/// every read, write or removal must still go through the owning store so stale
/// generations are rejected at the point where state would be touched
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
/// this is the raw slot table for state that only needs worker-local identity
/// callers keep generation-checked handles while the store keeps ownership of
/// the values and their reuse policy
///
/// indices are recycled after removal to keep handles small and cache-friendly
/// generation checks make that reuse compatible with delayed packet-loop work
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
/// use the key API at worker boundaries where callers speak in transport ids
/// convert to handles only for work that must survive in packet-loop queues,
/// heaps or route-local state
///
/// the key is stored inside the slot so a live handle can be translated back
/// when the packet loop must report ready public sessions
/// if translation fails, the handle is stale and the queued work is already
/// obsolete
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

    /// mutably read by handle while borrowing the matching public key
    pub(super) fn get_key_value_mut_by_handle(
        &mut self,
        handle: SlotHandle<Tag>,
    ) -> Option<(&K, &mut V)> {
        self.slots
            .get_mut(handle)
            .map(|entry| (&entry.key, &mut entry.value))
    }
}

fn next_generation(generation: u64) -> u64 {
    generation.wrapping_add(1).max(1)
}
