use std::mem;

#[cfg(kani)]
pub(super) type BTreeMap<K, V> = ProvableMap<K, V>;
#[cfg(kani)]
pub(super) type BTreeSet<V> = ProvableSet<V>;

#[derive(Debug)]
pub(super) struct ProvableMap<K, V> {
    first: Option<(K, V)>,
    second: Option<(K, V)>,
}

impl<K: PartialEq, V> ProvableMap<K, V> {
    pub(super) const fn new() -> Self {
        Self {
            first: None,
            second: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        usize::from(self.first.is_some()) + usize::from(self.second.is_some())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.first.is_none()
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some((entry_key, entry_value)) = self.first.as_mut()
            && entry_key == &key
        {
            return Some(mem::replace(entry_value, value));
        }
        if let Some((entry_key, entry_value)) = self.second.as_mut()
            && entry_key == &key
        {
            return Some(mem::replace(entry_value, value));
        }
        if self.first.is_none() {
            self.first = Some((key, value));
            return None;
        }
        assert!(
            self.second.is_none(),
            "provable router storage capacity exceeded"
        );
        self.second = Some((key, value));
        None
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        if let Some((entry_key, value)) = self.first.as_ref()
            && entry_key == key
        {
            return Some(value);
        }
        if let Some((entry_key, value)) = self.second.as_ref()
            && entry_key == key
        {
            return Some(value);
        }
        None
    }

    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some((entry_key, value)) = self.first.as_mut()
            && entry_key == key
        {
            return Some(value);
        }
        if let Some((entry_key, value)) = self.second.as_mut()
            && entry_key == key
        {
            return Some(value);
        }
        None
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        if self
            .first
            .as_ref()
            .is_some_and(|(entry_key, _value)| entry_key == key)
        {
            let entry = self.first.take();
            self.first = self.second.take();
            return entry.map(|(_key, value)| value);
        }
        if self
            .second
            .as_ref()
            .is_some_and(|(entry_key, _value)| entry_key == key)
        {
            return self.second.take().map(|(_key, value)| value);
        }
        None
    }

    pub(super) fn values_mut(&mut self) -> Slots<&mut V> {
        Slots {
            first: self.first.as_mut().map(|(_key, value)| value),
            second: self.second.as_mut().map(|(_key, value)| value),
        }
    }

    pub(super) fn iter(&self) -> Slots<(&K, &V)> {
        Slots {
            first: self.first.as_ref().map(|(key, value)| (key, value)),
            second: self.second.as_ref().map(|(key, value)| (key, value)),
        }
    }

    pub(super) fn keys(&self) -> Keys<Slots<(&K, &V)>> {
        Keys(self.iter())
    }
}

impl<K: PartialEq, V> Default for ProvableMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, K: PartialEq, V> IntoIterator for &'a ProvableMap<K, V> {
    type IntoIter = Slots<(&'a K, &'a V)>;
    type Item = (&'a K, &'a V);

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub(super) struct Slots<T> {
    first: Option<T>,
    second: Option<T>,
}

impl<T> Iterator for Slots<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.first.take().or_else(|| self.second.take())
    }
}

pub(super) struct Keys<I>(I);

impl<K, V, I> Iterator for Keys<I>
where
    I: Iterator<Item = (K, V)>,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, _value)| key)
    }
}

#[derive(Debug)]
pub(super) struct ProvableSet<V> {
    values: ProvableMap<V, ()>,
}

impl<V: PartialEq> ProvableSet<V> {
    pub(super) const fn new() -> Self {
        Self {
            values: ProvableMap::new(),
        }
    }

    pub(super) fn insert(&mut self, value: V) -> bool {
        if self.contains(&value) {
            return false;
        }
        self.values.insert(value, ());
        true
    }

    pub(super) fn remove(&mut self, value: &V) -> bool {
        self.values.remove(value).is_some()
    }

    pub(super) fn len(&self) -> usize {
        self.values.len()
    }

    pub(super) fn iter(&self) -> Keys<Slots<(&V, &())>> {
        self.values.keys()
    }

    pub(super) fn contains(&self, value: &V) -> bool {
        self.values.contains_key(value)
    }
}

impl<V: PartialEq> Default for ProvableSet<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, V: PartialEq> IntoIterator for &'a ProvableSet<V> {
    type IntoIter = Keys<Slots<(&'a V, &'a ())>>;
    type Item = &'a V;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<V: PartialEq> IntoIterator for ProvableSet<V> {
    type IntoIter = Keys<Slots<(V, ())>>;
    type Item = V;

    fn into_iter(self) -> Self::IntoIter {
        Keys(Slots {
            first: self.values.first,
            second: self.values.second,
        })
    }
}

#[cfg(test)]
#[path = "TESTS/proof_storage.rs"]
mod tests;
