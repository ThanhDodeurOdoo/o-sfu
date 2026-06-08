use super::*;

#[test]
fn lag_publisher_publishes_maximum_observed_lag_on_interval() {
    let started_at = Instant::now();
    let snapshot = PacketLoopLagSnapshot::new(started_at);
    let mut publisher = PacketLoopLagPublisher::new(started_at);

    publisher.observe(
        &snapshot,
        started_at + Duration::from_millis(5),
        started_at + Duration::from_millis(15),
    );
    publisher.observe(
        &snapshot,
        started_at + Duration::from_millis(20),
        started_at + Duration::from_millis(40),
    );

    assert_eq!(
        snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(40)),
        0
    );

    publisher.observe(
        &snapshot,
        started_at + Duration::from_millis(105),
        started_at + Duration::from_millis(110),
    );

    assert_eq!(
        snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(110)),
        20
    );
}

#[test]
fn lag_snapshot_expires_stale_samples() {
    let started_at = Instant::now();
    let snapshot = PacketLoopLagSnapshot::new(started_at);
    let mut publisher = PacketLoopLagPublisher::new(started_at);

    publisher.observe(
        &snapshot,
        started_at + Duration::from_millis(99),
        started_at + Duration::from_millis(100),
    );

    assert_eq!(
        snapshot.packet_loop_lag_ms_at(started_at + Duration::from_millis(1101)),
        0
    );
}
