use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use super::*;
use crate::engine::{
    UserId,
    media_transport::{
        TransportMediaId, TransportSessionKey,
        rtc::{
            relay_registry::RelayPacketMailbox,
            test_support::{sample_already_relayed_packet, test_transport_session_key},
        },
    },
    packet_sink_registry::PacketSink as MediaPacketSink,
};

struct CountingSink {
    packets: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            packets: AtomicUsize::new(0),
        }
    }
}

impl MediaPacketSink for CountingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.packets.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn packet_forward_wraps_local_route_destinations_in_the_named_contract() {
    let src_media = TransportMediaId::new(7);
    let forward = PacketForward::from_local_route_destination(4, src_media, 3);

    assert_eq!(forward.pkt_idx(), 4);
    assert!(matches!(
        forward.destination(),
        ForwardingDestination::LocalRtc(destination)
            if *destination == LocalRtcPacketDestination::new(src_media, 3)
    ));
}

#[test]
fn packet_forward_wraps_packet_sinks_in_the_named_contract() {
    let sink = RegisteredPacketSink::new(
        Arc::new(CountingSink::new()),
        RtpForwardDestinationKind::Recording,
    );
    let forward = PacketForward::from_packet_sink(5, TransportMediaId::new(8), sink);

    assert_eq!(forward.pkt_idx(), 5);
    assert!(matches!(
        forward.destination(),
        ForwardingDestination::PacketSink(destination)
            if destination.transport_media_id == TransportMediaId::new(8)
    ));
}

#[test]
fn packet_forward_wraps_intra_node_relay_sinks_in_the_named_contract() {
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let packet = sample_already_relayed_packet(
        test_transport_session_key(11, 0, 12, UserId::Integer(13)),
        TransportMediaId::new(8),
        "aud-up",
        b"payload",
    );
    let forward = PacketForward::from_relay_target(6, TransportMediaId::new(9), mailbox);
    let mut state = PacketLoopState::default();

    assert_eq!(forward.pkt_idx(), 6);
    assert!(matches!(
        forward.destination(),
        ForwardingDestination::Relay(destination)
            if destination.transport_media_id == TransportMediaId::new(9)
    ));
    let _ = forward.destination().send(&mut state, &packet);
    assert!(relay_rx.try_recv().is_ok());
}
