//! Transport-side source-policy wake coordination.
//!
//! The channel layer own the actual source-packet selection policy: it decides,
//! from room membership and publication state, which producer layers should stay
//! routable. The transport layer in the other hand own observations such as active-speaker
//! activity and expiry deadlines that can change wihtout any room mutation. That
//! split means the runtime needs one narrow bridge from transport-side observation
//! changes back into the channel-side policy sync task.
//!
//! This module is that bridge. It deliberatley does not store the policy itself,
//! any room state, or any queued work items. Instead it exposes one coalescing
//! dirty bit plus a wake primitive:
//!
//! - `SourcePolicyDirtyState` tracks whether at least one transport-side change
//!   happened since the last policy sync
//! - `SourcePolicySignal::mark_dirty()` transitions the state from clean to dirty
//!   and only wakes the listener on that edge.
//! - `SourcePolicyUpdateSubscription::wait_for_update()` consumes the dirty state
//!   before sleeping again, so a previously observed update cannot be lost if it
//!   races with the wait path
//!
//! The important property is coalescingL: Multiple RTP packets can arrive
//! while the channel sync task is still busy, but those packets do not need
//! an equal number of wakeups or replayed jobs. The channel task only needs to
//! know that "something changed" and then re-read the current transport-owned
//! observation state from the adapter. This keeps the signaling between packet-loop
//! activity and policy recomputation bounded and avoids turning hot-path packet
//! observation into an unbounded event queue.
//!
//! Tokio's `Notify` handles the async wakeup in production, while the dirty-bit
//! logic remains separately (so it can be tested in Loom)
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(crate) struct SourcePolicyDirtyState {
    dirty: AtomicBool,
}

impl SourcePolicyDirtyState {
    pub(crate) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn mark_dirty(&self) -> bool {
        !self.dirty.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SourcePolicyUpdateSubscription {
    dirty: Arc<SourcePolicyDirtyState>,
    notify: Arc<Notify>,
}

impl SourcePolicyUpdateSubscription {
    pub(crate) async fn wait_for_update(&self) {
        loop {
            if self.dirty.take_dirty() {
                return;
            }
            self.notify.notified().await;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SourcePolicySignal {
    dirty: Arc<SourcePolicyDirtyState>,
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
        if self.dirty.mark_dirty() {
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
