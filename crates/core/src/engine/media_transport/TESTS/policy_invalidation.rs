use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::{task::yield_now, time::timeout};

use super::SourcePolicySignal;
use crate::RoomInstanceId;

#[tokio::test]
async fn wait_for_update_observes_dirty_state_marked_before_wait() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    signal.mark_dirty(RoomInstanceId::from_raw(7));

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(
        updates.ok(),
        Some(BTreeSet::from([RoomInstanceId::from_raw(7)]))
    );
}

#[tokio::test]
async fn wait_for_update_wakes_task_after_cross_task_mark_dirty() {
    let signal = Arc::new(SourcePolicySignal::default());
    let subscription = signal.subscribe();

    let waiter = tokio::spawn(async move {
        timeout(Duration::from_secs(1), subscription.wait_for_update())
            .await
            .ok()
    });

    yield_now().await;
    signal.mark_dirty(RoomInstanceId::from_raw(9));

    let waiter_result = waiter.await;
    assert!(waiter_result.is_ok());
    let Ok(woke) = waiter_result else {
        return;
    };
    assert_eq!(woke, Some(BTreeSet::from([RoomInstanceId::from_raw(9)])));
}

#[tokio::test]
async fn wait_for_update_coalesces_multiple_dirty_marks_per_channel() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    signal.mark_dirty(RoomInstanceId::from_raw(4));
    signal.mark_dirty(RoomInstanceId::from_raw(4));
    signal.mark_dirty(RoomInstanceId::from_raw(6));

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(
        updates.ok(),
        Some(BTreeSet::from([
            RoomInstanceId::from_raw(4),
            RoomInstanceId::from_raw(6),
        ]))
    );
}

#[tokio::test]
async fn wait_for_update_observes_batch_dirty_marks() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    signal.mark_dirty_rooms([
        RoomInstanceId::from_raw(8),
        RoomInstanceId::from_raw(8),
        RoomInstanceId::from_raw(10),
    ]);

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(
        updates.ok(),
        Some(BTreeSet::from([
            RoomInstanceId::from_raw(8),
            RoomInstanceId::from_raw(10),
        ]))
    );
}
