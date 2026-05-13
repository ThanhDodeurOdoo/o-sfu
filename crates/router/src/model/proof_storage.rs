#[cfg(not(kani))]
pub(super) use std::collections::{BTreeMap, BTreeSet};

#[cfg(kani)]
const PROOF_STORAGE_CAPACITY: usize = 4;

#[cfg(kani)]
pub(super) type BTreeMap<K, V> = ProvableMap<K, V, PROOF_STORAGE_CAPACITY>;
#[cfg(kani)]
pub(super) type BTreeSet<V> = ProvableSet<V, PROOF_STORAGE_CAPACITY>;

#[cfg(kani)]
#[derive(Debug, Clone, Copy)]
pub(super) struct ProvableMap<K, V, const CAPACITY: usize> {
    entries: [Option<(K, V)>; CAPACITY],
    len: usize,
}

#[cfg(kani)]
impl<K, V, const CAPACITY: usize> ProvableMap<K, V, CAPACITY>
where
    K: Copy + Ord,
    V: Copy,
{
    pub(super) fn new() -> Self {
        assert!(CAPACITY == PROOF_STORAGE_CAPACITY);
        Self {
            entries: [None; CAPACITY],
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if Self::entry_key_matches(&self.entries[0], &key) {
            return Self::replace_entry_value(&mut self.entries[0], value);
        }
        if Self::entry_key_matches(&self.entries[1], &key) {
            return Self::replace_entry_value(&mut self.entries[1], value);
        }
        if Self::entry_key_matches(&self.entries[2], &key) {
            return Self::replace_entry_value(&mut self.entries[2], value);
        }
        if Self::entry_key_matches(&self.entries[3], &key) {
            return Self::replace_entry_value(&mut self.entries[3], value);
        }

        if Self::insert_empty_entry(&mut self.entries[0], &mut self.len, key, value)
            || Self::insert_empty_entry(&mut self.entries[1], &mut self.len, key, value)
            || Self::insert_empty_entry(&mut self.entries[2], &mut self.len, key, value)
            || Self::insert_empty_entry(&mut self.entries[3], &mut self.len, key, value)
        {
            return None;
        }

        panic!("provable router storage capacity exceeded");
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        if let Some(value) = Self::entry_value(&self.entries[0], key) {
            return Some(value);
        }
        if let Some(value) = Self::entry_value(&self.entries[1], key) {
            return Some(value);
        }
        if let Some(value) = Self::entry_value(&self.entries[2], key) {
            return Some(value);
        }
        if let Some(value) = Self::entry_value(&self.entries[3], key) {
            return Some(value);
        }
        None
    }

    fn entry_value<'a>(entry: &'a Option<(K, V)>, key: &K) -> Option<&'a V> {
        if let Some((entry_key, value)) = entry
            && *entry_key == *key
        {
            return Some(value);
        }
        None
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if Self::entry_key_matches(&self.entries[0], key) {
            return Self::entry_value_mut(&mut self.entries[0]);
        }
        if Self::entry_key_matches(&self.entries[1], key) {
            return Self::entry_value_mut(&mut self.entries[1]);
        }
        if Self::entry_key_matches(&self.entries[2], key) {
            return Self::entry_value_mut(&mut self.entries[2]);
        }
        if Self::entry_key_matches(&self.entries[3], key) {
            return Self::entry_value_mut(&mut self.entries[3]);
        }
        None
    }

    fn entry_key_matches(entry: &Option<(K, V)>, key: &K) -> bool {
        if let Some((entry_key, _value)) = entry {
            return *entry_key == *key;
        }
        false
    }

    fn entry_value_mut(entry: &mut Option<(K, V)>) -> Option<&mut V> {
        if let Some((_entry_key, value)) = entry {
            return Some(value);
        }
        None
    }

    fn replace_entry_value(entry: &mut Option<(K, V)>, value: V) -> Option<V> {
        if let Some((_entry_key, entry_value)) = entry {
            let old_value = *entry_value;
            *entry_value = value;
            return Some(old_value);
        }

        panic!("provable router storage entry disappeared");
    }

    fn insert_empty_entry(entry: &mut Option<(K, V)>, len: &mut usize, key: K, value: V) -> bool {
        if entry.is_none() {
            *entry = Some((key, value));
            *len += 1;
            return true;
        }
        false
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        if Self::entry_key_matches(&self.entries[0], key) {
            return Self::remove_entry(&mut self.entries[0], &mut self.len);
        }
        if Self::entry_key_matches(&self.entries[1], key) {
            return Self::remove_entry(&mut self.entries[1], &mut self.len);
        }
        if Self::entry_key_matches(&self.entries[2], key) {
            return Self::remove_entry(&mut self.entries[2], &mut self.len);
        }
        if Self::entry_key_matches(&self.entries[3], key) {
            return Self::remove_entry(&mut self.entries[3], &mut self.len);
        }
        None
    }

    fn remove_entry(entry: &mut Option<(K, V)>, len: &mut usize) -> Option<V> {
        if let Some((_entry_key, value)) = *entry {
            *entry = None;
            *len -= 1;
            return Some(value);
        }

        panic!("provable router storage entry disappeared");
    }

    pub(super) fn values(&self) -> ProvableMapValues<'_, K, V, CAPACITY> {
        ProvableMapValues {
            entries: &self.entries,
            index: 0,
        }
    }

    pub(super) fn iter(&self) -> ProvableMapIter<'_, K, V, CAPACITY> {
        ProvableMapIter {
            entries: &self.entries,
            index: 0,
        }
    }
}

#[cfg(kani)]
pub(super) struct ProvableMapValues<'a, K, V, const CAPACITY: usize> {
    entries: &'a [Option<(K, V)>; CAPACITY],
    index: usize,
}

#[cfg(kani)]
impl<'a, K, V, const CAPACITY: usize> Iterator for ProvableMapValues<'a, K, V, CAPACITY> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < CAPACITY {
            let entry = &self.entries[self.index];
            self.index += 1;
            if let Some((_key, value)) = entry {
                return Some(value);
            }
        }
        None
    }
}

#[cfg(kani)]
pub(super) struct ProvableMapIter<'a, K, V, const CAPACITY: usize> {
    entries: &'a [Option<(K, V)>; CAPACITY],
    index: usize,
}

#[cfg(kani)]
impl<'a, K, V, const CAPACITY: usize> Iterator for ProvableMapIter<'a, K, V, CAPACITY> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < CAPACITY {
            let entry = &self.entries[self.index];
            self.index += 1;
            if let Some((key, value)) = entry {
                return Some((key, value));
            }
        }
        None
    }
}

#[cfg(kani)]
impl<'a, K, V, const CAPACITY: usize> IntoIterator for &'a ProvableMap<K, V, CAPACITY>
where
    K: Copy + Ord,
    V: Copy,
{
    type IntoIter = ProvableMapIter<'a, K, V, CAPACITY>;
    type Item = (&'a K, &'a V);

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(kani)]
#[derive(Debug, Clone, Copy)]
pub(super) struct ProvableSet<V, const CAPACITY: usize> {
    entries: [Option<V>; CAPACITY],
    len: usize,
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> ProvableSet<V, CAPACITY>
where
    V: Copy + Ord,
{
    pub(super) fn new() -> Self {
        assert!(CAPACITY == PROOF_STORAGE_CAPACITY);
        Self {
            entries: [None; CAPACITY],
            len: 0,
        }
    }

    pub(super) fn insert(&mut self, value: V) -> bool {
        if self.contains(&value) {
            return false;
        }

        if Self::insert_empty_entry(&mut self.entries[0], &mut self.len, value)
            || Self::insert_empty_entry(&mut self.entries[1], &mut self.len, value)
            || Self::insert_empty_entry(&mut self.entries[2], &mut self.len, value)
            || Self::insert_empty_entry(&mut self.entries[3], &mut self.len, value)
        {
            return true;
        }

        panic!("provable router storage capacity exceeded");
    }

    pub(super) fn remove(&mut self, value: &V) -> bool {
        if Self::entry_value_matches(&self.entries[0], value) {
            return Self::remove_entry(&mut self.entries[0], &mut self.len);
        }
        if Self::entry_value_matches(&self.entries[1], value) {
            return Self::remove_entry(&mut self.entries[1], &mut self.len);
        }
        if Self::entry_value_matches(&self.entries[2], value) {
            return Self::remove_entry(&mut self.entries[2], &mut self.len);
        }
        if Self::entry_value_matches(&self.entries[3], value) {
            return Self::remove_entry(&mut self.entries[3], &mut self.len);
        }
        false
    }

    fn insert_empty_entry(entry: &mut Option<V>, len: &mut usize, value: V) -> bool {
        if entry.is_none() {
            *entry = Some(value);
            *len += 1;
            return true;
        }
        false
    }

    fn remove_entry(entry: &mut Option<V>, len: &mut usize) -> bool {
        if entry.is_some() {
            *entry = None;
            *len -= 1;
            return true;
        }

        panic!("provable router storage entry disappeared");
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn iter(&self) -> ProvableSetIter<'_, V, CAPACITY> {
        ProvableSetIter {
            entries: &self.entries,
            index: 0,
        }
    }

    pub(super) fn contains(&self, value: &V) -> bool {
        Self::entry_value_matches(&self.entries[0], value)
            || Self::entry_value_matches(&self.entries[1], value)
            || Self::entry_value_matches(&self.entries[2], value)
            || Self::entry_value_matches(&self.entries[3], value)
    }

    fn entry_value_matches(entry: &Option<V>, value: &V) -> bool {
        if let Some(entry_value) = entry {
            return *entry_value == *value;
        }
        false
    }
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> Default for ProvableSet<V, CAPACITY>
where
    V: Copy + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(kani)]
pub(super) struct ProvableSetIter<'a, V, const CAPACITY: usize> {
    entries: &'a [Option<V>; CAPACITY],
    index: usize,
}

#[cfg(kani)]
impl<'a, V, const CAPACITY: usize> Iterator for ProvableSetIter<'a, V, CAPACITY> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < CAPACITY {
            let entry = &self.entries[self.index];
            self.index += 1;
            if let Some(value) = entry {
                return Some(value);
            }
        }
        None
    }
}

#[cfg(kani)]
impl<'a, V, const CAPACITY: usize> IntoIterator for &'a ProvableSet<V, CAPACITY>
where
    V: Copy + Ord,
{
    type IntoIter = ProvableSetIter<'a, V, CAPACITY>;
    type Item = &'a V;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(kani)]
pub(super) struct ProvableSetIntoIter<V, const CAPACITY: usize> {
    set: ProvableSet<V, CAPACITY>,
    index: usize,
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> Iterator for ProvableSetIntoIter<V, CAPACITY>
where
    V: Copy,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < CAPACITY {
            let entry = self.set.entries[self.index];
            self.index += 1;
            if let Some(value) = entry {
                return Some(value);
            }
        }
        None
    }
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> IntoIterator for ProvableSet<V, CAPACITY>
where
    V: Copy,
{
    type IntoIter = ProvableSetIntoIter<V, CAPACITY>;
    type Item = V;

    fn into_iter(self) -> Self::IntoIter {
        ProvableSetIntoIter {
            set: self,
            index: 0,
        }
    }
}
