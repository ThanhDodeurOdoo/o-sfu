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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::{task::yield_now, time::timeout};

    use super::SourcePolicySignal;

    #[tokio::test]
    async fn wait_for_update_observes_dirty_state_marked_before_wait() {
        let signal = SourcePolicySignal::default();
        let subscription = signal.subscribe();
        signal.mark_dirty();

        assert!(
            timeout(Duration::from_secs(1), subscription.wait_for_update())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn wait_for_update_wakes_task_after_cross_task_mark_dirty() {
        let signal = Arc::new(SourcePolicySignal::default());
        let subscription = signal.subscribe();

        let waiter = tokio::spawn(async move {
            timeout(Duration::from_secs(1), subscription.wait_for_update())
                .await
                .is_ok()
        });

        yield_now().await;
        signal.mark_dirty();

        let waiter_result = waiter.await;
        assert!(waiter_result.is_ok());
        let Ok(woke) = waiter_result else {
            return;
        };
        assert!(woke);
    }
}
