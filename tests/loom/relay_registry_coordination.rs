use loom::{
    model,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use o_sfu::testing::concurrency::RelayTargetRegistry;
use std::{
    collections::BTreeMap,
    panic::resume_unwind,
    sync::{Arc as StdArc, PoisonError},
};

#[derive(Debug, Default)]
struct ModeledRelayRegistry {
    any_active: AtomicBool,
    active_sources: RwLock<BTreeMap<u64, RelayTargetRegistry<u64, usize>>>,
}

impl ModeledRelayRegistry {
    fn targets_for_source(&self, source_transport_media_id: u64) -> Option<StdArc<[usize]>> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        self.active_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&source_transport_media_id)
            .and_then(|registration| {
                registration
                    .has_active_targets()
                    .then(|| registration.active_targets())
            })
    }

    fn activate_source_target(
        &self,
        source_transport_media_id: u64,
        target_id: u64,
        target: usize,
    ) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        active_sources
            .entry(source_transport_media_id)
            .or_default()
            .add_target(target_id, target);
        let has_active_sources = active_sources
            .values()
            .any(RelayTargetRegistry::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }

    fn deactivate_source_target(&self, source_transport_media_id: u64, target_id: u64) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let remove_source = active_sources
            .get_mut(&source_transport_media_id)
            .is_some_and(|source| source.remove_target(target_id));
        if remove_source {
            active_sources.remove(&source_transport_media_id);
        }
        let has_active_sources = active_sources
            .values()
            .any(RelayTargetRegistry::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }

    fn set_source_target_active(
        &self,
        source_transport_media_id: u64,
        target_id: u64,
        active: bool,
    ) {
        let mut active_sources = self
            .active_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(source_registration) = active_sources.get_mut(&source_transport_media_id) else {
            return;
        };
        source_registration.set_target_active(target_id, active);
        let has_active_sources = active_sources
            .values()
            .any(RelayTargetRegistry::has_active_targets);
        drop(active_sources);
        self.any_active.store(has_active_sources, Ordering::Release);
    }
}

#[test]
fn relay_registry_fast_path_only_publishes_fully_active_targets() {
    model(|| {
        let registry = Arc::new(ModeledRelayRegistry::default());
        let writer_registry = Arc::clone(&registry);
        let reader_registry = Arc::clone(&registry);

        let writer = thread::spawn(move || {
            writer_registry.activate_source_target(5, 9, 27);
            writer_registry.set_source_target_active(5, 9, true);
        });
        let reader = thread::spawn(move || {
            if reader_registry.any_active.load(Ordering::Acquire) {
                let targets = reader_registry.targets_for_source(5);
                assert_eq!(targets.as_deref(), Some(&[27][..]));
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

        assert_eq!(registry.targets_for_source(5).as_deref(), Some(&[27][..]));
    });
}

#[test]
fn relay_registry_reference_counts_routes_before_final_cleanup() {
    model(|| {
        let registry = ModeledRelayRegistry::default();
        registry.activate_source_target(5, 9, 27);
        registry.activate_source_target(5, 9, 27);
        registry.set_source_target_active(5, 9, true);
        registry.set_source_target_active(5, 9, true);

        assert_eq!(registry.targets_for_source(5).as_deref(), Some(&[27][..]));

        registry.deactivate_source_target(5, 9);
        assert_eq!(registry.targets_for_source(5).as_deref(), Some(&[27][..]));

        registry.deactivate_source_target(5, 9);
        assert!(registry.targets_for_source(5).is_none());
    });
}
