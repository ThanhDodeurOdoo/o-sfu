use loom::{
    model,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};
use o_sfu::testing::concurrency::ActiveChannelRegistry;
use std::{panic::resume_unwind, sync::PoisonError};

#[derive(Debug, Default)]
struct ModeledMediaTap {
    any_active: AtomicBool,
    active_channels: RwLock<ActiveChannelRegistry<u64, Arc<AtomicUsize>>>,
}

impl ModeledMediaTap {
    fn sink_for_channel(&self, channel_runtime_id: u64) -> Option<Arc<AtomicUsize>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel_runtime_id)
    }

    fn activate_channel(&self, channel_runtime_id: u64, sink: Arc<AtomicUsize>) {
        self.active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel_runtime_id, sink);
        self.any_active.store(true, Ordering::Release);
    }

    fn deactivate_channel(&self, channel_runtime_id: u64) {
        let mut active_channels = self
            .active_channels
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_channels.remove(&channel_runtime_id);
        self.any_active
            .store(!active_channels.is_empty(), Ordering::Release);
    }
}

#[test]
fn media_tap_fast_path_never_publishes_true_without_the_sink() {
    model(|| {
        let tap = Arc::new(ModeledMediaTap::default());
        let sink = Arc::new(AtomicUsize::new(0));
        let writer_tap = Arc::clone(&tap);
        let reader_tap = Arc::clone(&tap);
        let writer_sink = Arc::clone(&sink);

        let writer = thread::spawn(move || {
            writer_tap.activate_channel(7, writer_sink);
        });
        let reader = thread::spawn(move || {
            if reader_tap.any_active.load(Ordering::Acquire) {
                assert!(reader_tap.sink_for_channel(7).is_some());
            }
        });

        match writer.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }
        match reader.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }

        assert!(tap.sink_for_channel(7).is_some());
    });
}

#[test]
fn media_tap_keeps_other_channels_published_when_one_channel_drains() {
    model(|| {
        let tap = Arc::new(ModeledMediaTap::default());
        tap.activate_channel(7, Arc::new(AtomicUsize::new(0)));
        tap.activate_channel(8, Arc::new(AtomicUsize::new(0)));

        let deactivator_tap = Arc::clone(&tap);
        let reader_tap = Arc::clone(&tap);

        let deactivator = thread::spawn(move || {
            deactivator_tap.deactivate_channel(7);
        });
        let reader = thread::spawn(move || {
            if reader_tap.any_active.load(Ordering::Acquire) {
                assert!(reader_tap.sink_for_channel(8).is_some());
            }
        });

        match deactivator.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }
        match reader.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }

        assert!(tap.sink_for_channel(8).is_some());
        assert!(tap.sink_for_channel(7).is_none());
    });
}
