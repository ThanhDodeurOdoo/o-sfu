use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::{
    sync::Barrier,
    time::{Instant, timeout},
};

use super::{SOURCE_POLICY_FOLLOW_UP_DELAY, SourcePolicySignal};
use crate::{RoomInstanceId, engine::sync::lock_unpoisoned};

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
async fn wait_for_update_observes_absolute_deadline() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let room = RoomInstanceId::from_raw(13);
    signal.mark_dirty_at(room, Instant::now() + Duration::from_millis(10));

    let updates = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert_eq!(updates.ok(), Some(BTreeSet::from([room])));
}

#[tokio::test]
async fn wait_for_update_merges_mark_after_first_drain() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let after_first_drain = Arc::new(Barrier::new(2));
    let producer_barrier = Arc::clone(&after_first_drain);
    let producer_signal = signal.clone();
    let producer = tokio::spawn(async move {
        producer_barrier.wait().await;
        producer_signal
            .mark_dirty_rooms([RoomInstanceId::from_raw(9), RoomInstanceId::from_raw(10)]);
    });

    signal.mark_dirty(RoomInstanceId::from_raw(9));
    let first_update = timeout(Duration::from_secs(1), subscription.wait_for_update()).await;
    assert!(first_update.is_ok());
    let Ok(mut woke) = first_update else {
        return;
    };
    after_first_drain.wait().await;
    assert!(producer.await.is_ok());
    woke.extend(subscription.take_pending_updates());
    assert_eq!(
        woke,
        BTreeSet::from([RoomInstanceId::from_raw(9), RoomInstanceId::from_raw(10)])
    );
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

#[test]
fn delayed_marks_keep_one_deadline_per_room() {
    let signal = SourcePolicySignal::default();
    let room = RoomInstanceId::from_raw(12);
    signal.mark_dirty_after(room, SOURCE_POLICY_FOLLOW_UP_DELAY);
    let first_deadline = {
        let pending = lock_unpoisoned(&signal.0.pending);
        pending.scheduled_by_room.get(&room).copied()
    };

    signal.mark_dirty_after(room, SOURCE_POLICY_FOLLOW_UP_DELAY);

    let pending = lock_unpoisoned(&signal.0.pending);
    assert_eq!(pending.scheduled_by_room.len(), 1);
    assert_eq!(pending.scheduled_by_deadline.len(), 1);
    assert_eq!(
        pending.scheduled_by_room.get(&room).copied(),
        first_deadline
    );
    drop(pending);
}
