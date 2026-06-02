use super::super::relay_registry::{RelayEnqueueOutcome, RelayPacketMailbox, RelayTargetId};
use crate::engine::{
    UserId,
    media_transport::{
        TransportMediaId,
        rtc::{
            state::PacketLoopState,
            test_support::{sample_forwarded_packet, test_transport_session_key},
        },
    },
};

#[test]
fn worker_local_relay_targets_track_active_sources() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(8);
    let relay_target = RelayTargetId::new(1);

    state
        .routes
        .add_relay_target(source_transport_media_id, relay_target, mailbox);
    state
        .routes
        .set_relay_target_active(source_transport_media_id, relay_target, true);
    assert!(
        state
            .routes
            .relay_targets_for_source(source_transport_media_id)
            .is_some()
    );

    state
        .routes
        .remove_relay_target(source_transport_media_id, relay_target);
    assert!(
        state
            .routes
            .relay_targets_for_source(source_transport_media_id)
            .is_none()
    );
}

#[test]
fn worker_local_relay_targets_forward_packets_through_registered_mailboxes() {
    let mut state = PacketLoopState::default();
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(9);
    let session_key = test_transport_session_key(13, 0, 14, UserId::Integer(15));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
    let relay_target = RelayTargetId::new(1);

    state
        .routes
        .add_relay_target(source_transport_media_id, relay_target, mailbox);
    state
        .routes
        .set_relay_target_active(source_transport_media_id, relay_target, true);

    let relay_targets = state
        .routes
        .relay_targets_for_source(source_transport_media_id);
    assert!(relay_targets.is_some());
    if let Some(relay_targets) = relay_targets {
        assert_eq!(relay_targets.len(), 1);
        if let Some(relay_target) = relay_targets.first() {
            relay_target.forward_packet_outcome(&state, &packet, source_transport_media_id);
        }
    }

    let forwarded = relay_rx.try_recv().ok();
    assert!(forwarded.is_some());
    if let Some(mut forwarded) = forwarded {
        assert_eq!(forwarded.payload(), b"payload");
        assert_eq!(
            forwarded.resolve_source_transport_media_id(&PacketLoopState::default()),
            Some(TransportMediaId::new(9))
        );
    }
}

#[test]
fn worker_local_relay_targets_keep_multiple_target_mailboxes_per_source() {
    let mut state = PacketLoopState::default();
    let (first_mailbox, mut first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, mut second_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(11);
    let session_key = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    state.routes.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(1),
        first_mailbox,
    );
    state
        .routes
        .set_relay_target_active(source_transport_media_id, RelayTargetId::new(1), true);
    state.routes.add_relay_target(
        source_transport_media_id,
        RelayTargetId::new(2),
        second_mailbox,
    );
    state
        .routes
        .set_relay_target_active(source_transport_media_id, RelayTargetId::new(2), true);

    let relay_targets = state
        .routes
        .relay_targets_for_source(source_transport_media_id);
    assert!(relay_targets.is_some());
    if let Some(relay_targets) = relay_targets {
        assert_eq!(relay_targets.len(), 2);
        for relay_target in relay_targets {
            relay_target.forward_packet_outcome(&state, &packet, source_transport_media_id);
        }
    }

    assert!(first_rx.try_recv().is_ok());
    assert!(second_rx.try_recv().is_ok());
}

#[test]
fn worker_local_relay_targets_do_not_reference_count_room_owners() {
    let mut state = PacketLoopState::default();
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(12);
    let relay_target = RelayTargetId::new(1);

    state
        .routes
        .add_relay_target(source_transport_media_id, relay_target, mailbox.clone());
    state
        .routes
        .add_relay_target(source_transport_media_id, relay_target, mailbox);
    state
        .routes
        .set_relay_target_active(source_transport_media_id, relay_target, true);
    state
        .routes
        .set_relay_target_active(source_transport_media_id, relay_target, true);
    assert_eq!(
        state.routes.relay_target_count(source_transport_media_id),
        1
    );
    assert_eq!(
        state
            .routes
            .active_relay_target_count(source_transport_media_id),
        1
    );

    state
        .routes
        .remove_relay_target(source_transport_media_id, relay_target);
    assert!(
        state
            .routes
            .relay_targets_for_source(source_transport_media_id)
            .is_none()
    );
}

#[test]
fn worker_local_relay_targets_keep_sources_independent() {
    let mut state = PacketLoopState::default();
    let first_source_transport_media_id = TransportMediaId::new(31);
    let second_source_transport_media_id = TransportMediaId::new(32);
    let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();

    state.routes.add_relay_target(
        first_source_transport_media_id,
        RelayTargetId::new(1),
        first_mailbox,
    );
    state.routes.set_relay_target_active(
        first_source_transport_media_id,
        RelayTargetId::new(1),
        true,
    );
    state.routes.add_relay_target(
        second_source_transport_media_id,
        RelayTargetId::new(2),
        second_mailbox,
    );
    state.routes.set_relay_target_active(
        second_source_transport_media_id,
        RelayTargetId::new(2),
        true,
    );

    assert_eq!(
        state
            .routes
            .relay_target_count(first_source_transport_media_id),
        1
    );
    assert_eq!(
        state
            .routes
            .relay_target_count(second_source_transport_media_id),
        1
    );
    state
        .routes
        .remove_relay_target(first_source_transport_media_id, RelayTargetId::new(1));
    assert!(
        state
            .routes
            .relay_targets_for_source(first_source_transport_media_id)
            .is_none()
    );
    assert!(
        state
            .routes
            .relay_targets_for_source(second_source_transport_media_id)
            .is_some()
    );
}

#[test]
fn worker_local_relay_targets_only_forward_to_targets_with_active_routes() {
    let mut state = PacketLoopState::default();
    let (first_mailbox, _first_rx) = RelayPacketMailbox::channel_for_test();
    let (second_mailbox, _second_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id = TransportMediaId::new(41);
    let first_target = RelayTargetId::new(1);
    let second_target = RelayTargetId::new(2);

    state
        .routes
        .add_relay_target(source_transport_media_id, first_target, first_mailbox);
    state
        .routes
        .add_relay_target(source_transport_media_id, second_target, second_mailbox);
    assert!(
        state
            .routes
            .relay_targets_for_source(source_transport_media_id)
            .is_none()
    );

    state
        .routes
        .set_relay_target_active(source_transport_media_id, second_target, true);
    let relay_targets = state
        .routes
        .relay_targets_for_source(source_transport_media_id);
    assert!(relay_targets.is_some());
    let Some(relay_targets) = relay_targets else {
        return;
    };
    assert_eq!(relay_targets.len(), 1);
    assert_eq!(
        state
            .routes
            .active_relay_target_count(source_transport_media_id),
        1
    );

    state
        .routes
        .set_relay_target_active(source_transport_media_id, second_target, false);
    assert!(
        state
            .routes
            .relay_targets_for_source(source_transport_media_id)
            .is_none()
    );
    assert_eq!(
        state
            .routes
            .active_relay_target_count(source_transport_media_id),
        0
    );
}

#[test]
fn worker_local_relay_targets_report_overload_when_a_bounded_mailbox_is_full() {
    let (mailbox, _rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);
    let source_transport_media_id = TransportMediaId::new(42);
    let session_key = test_transport_session_key(36, 0, 37, UserId::Integer(38));
    let packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
    let state = PacketLoopState::default();

    assert_eq!(
        mailbox
            .forward_packet(&state, &packet, source_transport_media_id)
            .map(|report| report.outcome),
        Some(RelayEnqueueOutcome::Enqueued)
    );
    assert_eq!(
        mailbox
            .forward_packet(&state, &packet, source_transport_media_id)
            .map(|report| report.outcome),
        Some(RelayEnqueueOutcome::Overloaded)
    );
}
