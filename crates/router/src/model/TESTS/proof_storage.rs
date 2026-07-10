use super::{ProvableMap, ProvableSet};

#[test]
fn map_preserves_two_slot_contract() {
    let mut map = ProvableMap::new();
    assert!(map.is_empty());
    assert_eq!(map.insert(1, 10), None);
    assert_eq!(map.insert(2, 20), None);
    assert_eq!(map.insert(2, 21), Some(20));
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&1), Some(&10));
    assert_eq!(map.get(&2), Some(&21));
    assert_eq!(
        map.get_mut(&2).map(|value| {
            *value += 1;
            *value
        }),
        Some(22)
    );
    assert_eq!(map.keys().copied().collect::<Vec<_>>(), [1, 2]);

    for value in map.values_mut() {
        *value += 1;
    }
    assert_eq!(map.remove(&1), Some(11));
    assert_eq!(map.insert(3, 30), None);
    assert_eq!(
        map.iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>(),
        [(2, 23), (3, 30)]
    );
    assert_eq!(map.remove(&3), Some(30));
    assert_eq!(map.remove(&3), None);
    assert_eq!(map.len(), 1);
}

#[test]
fn set_preserves_uniqueness_and_owned_iteration() {
    let mut set = ProvableSet::new();
    assert!(set.insert(1));
    assert!(set.insert(2));
    assert!(!set.insert(2));
    assert!(set.contains(&1));
    assert_eq!(set.len(), 2);
    assert!(set.remove(&1));
    assert!(!set.remove(&1));
    assert!(set.insert(3));
    assert_eq!(set.iter().copied().collect::<Vec<_>>(), [2, 3]);
    assert_eq!(set.into_iter().collect::<Vec<_>>(), [2, 3]);
}

#[test]
#[should_panic(expected = "provable router storage capacity exceeded")]
fn storage_rejects_a_third_entry() {
    let mut map = ProvableMap::new();
    map.insert(1, ());
    map.insert(2, ());
    map.insert(3, ());
}
