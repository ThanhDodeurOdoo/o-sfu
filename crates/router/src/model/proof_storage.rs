#[cfg(not(kani))]
pub(super) use std::collections::{BTreeMap, BTreeSet};

#[cfg(kani)]
const PROOF_STORAGE_CAPACITY: usize = 4;

#[cfg(kani)]
pub(super) type BTreeMap<K, V> = ProvableMap<K, V, PROOF_STORAGE_CAPACITY>;
#[cfg(kani)]
pub(super) type BTreeSet<V> = ProvableSet<V, PROOF_STORAGE_CAPACITY>;

#[cfg(kani)]
#[derive(Debug, Clone)]
pub(super) struct ProvableMap<K, V, const CAPACITY: usize> {
    entries: [Option<(K, V)>; CAPACITY],
    len: usize,
}

#[cfg(kani)]
impl<K, V, const CAPACITY: usize> ProvableMap<K, V, CAPACITY>
where
    K: Ord,
{
    pub(super) fn new() -> Self {
        assert!(CAPACITY == PROOF_STORAGE_CAPACITY);
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.key_index(key).is_some()
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some(index) = self.key_index(&key) {
            return Self::replace_entry_value(&mut self.entries[index], value);
        }

        if let Some(index) = self.empty_index() {
            self.entries[index] = Some((key, value));
            self.len += 1;
            return None;
        }

        panic!("provable router storage capacity exceeded");
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        let index = self.key_index(key)?;
        self.entries[index]
            .as_ref()
            .map(|(_entry_key, value)| value)
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let index = self.key_index(key)?;
        self.entries[index]
            .as_mut()
            .map(|(_entry_key, value)| value)
    }

    fn replace_entry_value(entry: &mut Option<(K, V)>, value: V) -> Option<V> {
        if let Some((_entry_key, entry_value)) = entry {
            return Some(std::mem::replace(entry_value, value));
        }

        panic!("provable router storage entry disappeared");
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.key_index(key)?;
        Self::remove_entry(&mut self.entries[index], &mut self.len)
    }

    fn remove_entry(entry: &mut Option<(K, V)>, len: &mut usize) -> Option<V> {
        if let Some((_entry_key, value)) = entry.take() {
            *len -= 1;
            return Some(value);
        }

        panic!("provable router storage entry disappeared");
    }

    fn key_index(&self, key: &K) -> Option<usize> {
        let mut index = 0;
        while index < CAPACITY {
            if let Some((entry_key, _value)) = &self.entries[index] {
                if *entry_key == *key {
                    return Some(index);
                }
            }
            index += 1;
        }
        None
    }

    fn empty_index(&self) -> Option<usize> {
        let mut index = 0;
        while index < CAPACITY {
            if self.entries[index].is_none() {
                return Some(index);
            }
            index += 1;
        }
        None
    }

    pub(super) fn values(&self) -> ProvableMapValues<'_, K, V, CAPACITY> {
        ProvableMapValues {
            entries: &self.entries,
            index: 0,
        }
    }

    pub(super) fn values_mut(&mut self) -> ProvableMapValuesMut<'_, K, V, CAPACITY> {
        ProvableMapValuesMut {
            entries: self.entries.iter_mut(),
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
impl<K, V, const CAPACITY: usize> Default for ProvableMap<K, V, CAPACITY>
where
    K: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(kani)]
impl<K, V, const CAPACITY: usize> FromIterator<(K, V)> for ProvableMap<K, V, CAPACITY>
where
    K: Ord,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
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
pub(super) struct ProvableMapValuesMut<'a, K, V, const CAPACITY: usize> {
    entries: std::slice::IterMut<'a, Option<(K, V)>>,
}

#[cfg(kani)]
impl<'a, K: 'a, V: 'a, const CAPACITY: usize> Iterator
    for ProvableMapValuesMut<'a, K, V, CAPACITY>
{
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.entries.by_ref() {
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
    K: Ord,
{
    type IntoIter = ProvableMapIter<'a, K, V, CAPACITY>;
    type Item = (&'a K, &'a V);

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(kani)]
#[derive(Debug, Clone)]
pub(super) struct ProvableSet<V, const CAPACITY: usize> {
    entries: [Option<V>; CAPACITY],
    len: usize,
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> ProvableSet<V, CAPACITY>
where
    V: Ord,
{
    pub(super) fn new() -> Self {
        assert!(CAPACITY == PROOF_STORAGE_CAPACITY);
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
        }
    }

    pub(super) fn insert(&mut self, value: V) -> bool {
        if self.contains(&value) {
            return false;
        }

        if let Some(index) = self.empty_index() {
            self.entries[index] = Some(value);
            self.len += 1;
            return true;
        }

        panic!("provable router set capacity exceeded");
    }

    pub(super) fn remove(&mut self, value: &V) -> bool {
        let mut index = 0;
        while index < CAPACITY {
            if let Some(entry) = &self.entries[index] {
                if *entry == *value {
                    self.entries[index] = None;
                    self.len -= 1;
                    return true;
                }
            }
            index += 1;
        }
        false
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
        let mut index = 0;
        while index < CAPACITY {
            if let Some(entry) = &self.entries[index] {
                if *entry == *value {
                    return true;
                }
            }
            index += 1;
        }
        false
    }

    fn empty_index(&self) -> Option<usize> {
        let mut index = 0;
        while index < CAPACITY {
            if self.entries[index].is_none() {
                return Some(index);
            }
            index += 1;
        }
        None
    }
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> Default for ProvableSet<V, CAPACITY>
where
    V: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> FromIterator<V> for ProvableSet<V, CAPACITY>
where
    V: Ord,
{
    fn from_iter<T: IntoIterator<Item = V>>(iter: T) -> Self {
        let mut set = Self::new();
        for value in iter {
            set.insert(value);
        }
        set
    }
}

#[cfg(kani)]
pub(super) struct ProvableSetIter<'a, V, const CAPACITY: usize> {
    entries: &'a [Option<V>; CAPACITY],
    index: usize,
}

#[cfg(kani)]
impl<'a, V, const CAPACITY: usize> Iterator for ProvableSetIter<'a, V, CAPACITY>
where
    V: Ord,
{
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
    V: Ord,
{
    type IntoIter = ProvableSetIter<'a, V, CAPACITY>;
    type Item = &'a V;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(kani)]
pub(super) struct ProvableSetIntoIter<V, const CAPACITY: usize> {
    entries: [Option<V>; CAPACITY],
    index: usize,
}

#[cfg(kani)]
impl<V, const CAPACITY: usize> Iterator for ProvableSetIntoIter<V, CAPACITY>
where
    V: Ord,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < CAPACITY {
            let entry = self.entries[self.index].take();
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
    V: Ord,
{
    type IntoIter = ProvableSetIntoIter<V, CAPACITY>;
    type Item = V;

    fn into_iter(self) -> Self::IntoIter {
        ProvableSetIntoIter {
            entries: self.entries,
            index: 0,
        }
    }
}
