use std::time::Duration;

use super::*;

#[test]
fn placement_pressure_reads_packet_loop_lag_from_atomic_snapshot() -> Result<(), &'static str> {
    let adapter = RtcWorker::default();
    let now = Instant::now();
    let started_at = now.checked_sub(Duration::from_millis(200)).unwrap_or(now);
    let packet_loop_lag = Arc::new(packet_loop::PacketLoopLagSnapshot::new(started_at));
    packet_loop_lag.publish_for_test(37, started_at + Duration::from_millis(150));
    let (command_tx, _command_rx) = mpsc::channel(1);
    let (relay_tx, _relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
    let debug_channels = super::super::super::test_support::RtcWorkerDebugChannels::new();

    let worker_handle = RtcWorkerHandle {
        command_tx,
        debug_handle: debug_channels.handle(),
        relay_mailbox: RelayPacketMailbox::new(relay_tx),
        bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
        snapshot_state: Arc::new(Mutex::new(
            super::super::super::state::RtcSnapshotState::default(),
        )),
        packet_loop_lag,
    };
    {
        let Ok(mut worker_slot) = adapter.worker_handle.lock() else {
            return Err("worker slot lock poisoned");
        };
        worker_slot.store(worker_handle);
    }

    let snapshot = adapter.placement_pressure_snapshot(&[]);

    assert_eq!(snapshot.packet_loop_lag_ms, 37);
    Ok(())
}
