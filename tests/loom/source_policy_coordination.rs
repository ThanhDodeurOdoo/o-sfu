use loom::{
    model,
    sync::{Arc, Condvar, Mutex},
    thread,
};
use o_sfu::testing::concurrency::SourcePolicyDirtyState;
use std::{panic::resume_unwind, sync::PoisonError};

#[derive(Debug, Default)]
struct ModeledSourcePolicySignal {
    dirty: SourcePolicyDirtyState,
    notify_state: Mutex<bool>,
    notify: Condvar,
}

impl ModeledSourcePolicySignal {
    fn wait_for_update(&self) {
        loop {
            if self.dirty.take_dirty() {
                return;
            }
            let mut has_permit = self
                .notify_state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if *has_permit {
                *has_permit = false;
                continue;
            }
            has_permit = self
                .notify
                .wait(has_permit)
                .unwrap_or_else(PoisonError::into_inner);
            if *has_permit {
                *has_permit = false;
            }
        }
    }

    fn mark_dirty(&self) {
        if self.dirty.mark_dirty() {
            let mut has_permit = self
                .notify_state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            *has_permit = true;
            drop(has_permit);
            self.notify.notify_one();
        }
    }
}

#[test]
fn source_policy_waiter_never_misses_a_racing_update() {
    model(|| {
        let signal = Arc::new(ModeledSourcePolicySignal::default());
        let waiter_signal = Arc::clone(&signal);
        let marker_signal = Arc::clone(&signal);

        let waiter = thread::spawn(move || {
            waiter_signal.wait_for_update();
        });
        let marker = thread::spawn(move || {
            marker_signal.mark_dirty();
        });

        match waiter.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }
        match marker.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }

        assert!(!signal.dirty.is_dirty());
    });
}

#[test]
fn source_policy_coalesces_duplicate_dirty_marks_but_still_delivers_later_updates() {
    model(|| {
        let signal = Arc::new(ModeledSourcePolicySignal::default());
        signal.mark_dirty();
        signal.mark_dirty();
        signal.wait_for_update();
        assert!(!signal.dirty.is_dirty());

        let waiter_signal = Arc::clone(&signal);
        let marker_signal = Arc::clone(&signal);

        let waiter = thread::spawn(move || {
            waiter_signal.wait_for_update();
        });
        let marker = thread::spawn(move || {
            marker_signal.mark_dirty();
        });

        match waiter.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }
        match marker.join() {
            Ok(()) => {}
            Err(error) => resume_unwind(error),
        }

        assert!(!signal.dirty.is_dirty());
    });
}
