use loom::{
    model,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use o_sfu::testing::concurrency::WorkerHandleSlot;
use std::{panic::resume_unwind, sync::PoisonError};

#[derive(Debug, Default)]
struct ModeledWorkerStartup {
    worker_handle: Mutex<WorkerHandleSlot<usize>>,
    starts: AtomicUsize,
}

impl ModeledWorkerStartup {
    fn ensure_started(&self) -> usize {
        let mut worker_handle = self
            .worker_handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(handle) = worker_handle.worker_handle() {
            return handle;
        }
        self.starts.fetch_add(1, Ordering::Relaxed);
        worker_handle.store(17)
    }

    fn clear(&self) {
        self.worker_handle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

#[test]
fn worker_startup_coalesces_concurrent_initialization_attempts() {
    model(|| {
        let startup = Arc::new(ModeledWorkerStartup::default());
        let first_startup = Arc::clone(&startup);
        let second_startup = Arc::clone(&startup);

        let first = thread::spawn(move || first_startup.ensure_started());
        let second = thread::spawn(move || second_startup.ensure_started());

        let first_handle = match first.join() {
            Ok(handle) => handle,
            Err(error) => resume_unwind(error),
        };
        let second_handle = match second.join() {
            Ok(handle) => handle,
            Err(error) => resume_unwind(error),
        };

        assert_eq!(first_handle, 17);
        assert_eq!(second_handle, 17);
        assert_eq!(startup.starts.load(Ordering::Relaxed), 1);
        assert!(
            startup
                .worker_handle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_started()
        );
    });
}

#[test]
fn worker_startup_can_publish_again_after_drain() {
    model(|| {
        let startup = ModeledWorkerStartup::default();
        assert_eq!(startup.ensure_started(), 17);
        startup.clear();
        assert_eq!(startup.ensure_started(), 17);
        assert_eq!(startup.starts.load(Ordering::Relaxed), 2);
    });
}
