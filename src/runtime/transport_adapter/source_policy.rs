use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub(crate) struct SourcePolicyUpdateSubscription {
    dirty: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl SourcePolicyUpdateSubscription {
    pub(crate) async fn wait_for_update(&self) {
        loop {
            if self.dirty.swap(false, Ordering::AcqRel) {
                return;
            }
            self.notify.notified().await;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SourcePolicySignal {
    dirty: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl SourcePolicySignal {
    #[must_use]
    pub(crate) fn subscribe(&self) -> SourcePolicyUpdateSubscription {
        SourcePolicyUpdateSubscription {
            dirty: Arc::clone(&self.dirty),
            notify: Arc::clone(&self.notify),
        }
    }

    pub(crate) fn mark_dirty(&self) {
        if !self.dirty.swap(true, Ordering::AcqRel) {
            self.notify.notify_one();
        }
    }
}
