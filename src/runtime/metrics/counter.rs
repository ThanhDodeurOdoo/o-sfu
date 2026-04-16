use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

pub(super) trait MetricLabel: Copy {
    const COUNT: usize;

    fn as_index(self) -> usize;
}

#[derive(Debug, Default)]
pub(super) struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub(super) fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn add(&self, value: usize) {
        if let Ok(value) = u64::try_from(value) {
            self.value.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub(super) fn add_u64(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    pub(super) fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Default)]
pub(super) struct UpDownCounter {
    value: AtomicI64,
}

impl UpDownCounter {
    pub(super) fn add(&self, delta: i64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    pub(super) fn load(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub(super) struct CounterFamily<L: MetricLabel> {
    counters: Box<[Counter]>,
    _label: PhantomData<L>,
}

impl<L: MetricLabel> Default for CounterFamily<L> {
    fn default() -> Self {
        let counters = (0..L::COUNT)
            .map(|_| Counter::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            counters,
            _label: PhantomData,
        }
    }
}

impl<L: MetricLabel> CounterFamily<L> {
    pub(super) fn increment(&self, label: L) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.increment();
        }
    }

    pub(super) fn add(&self, label: L, value: usize) {
        if let Some(counter) = self.counters.get(label.as_index()) {
            counter.add(value);
        }
    }

    pub(super) fn load(&self, label: L) -> u64 {
        self.counters.get(label.as_index()).map_or(0, Counter::load)
    }
}
