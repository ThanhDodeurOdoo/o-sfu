use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::{net::SocketAddr, sync::Mutex, time::Instant};

use str0m::media::{KeyframeRequestKind, MediaKind, Mid};
use str0m::rtp::Ssrc;
use tokio::sync::mpsc;

use super::{
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    keyframe_requests::{PendingKeyframeRequest, flush_pending_keyframe_requests},
};
use crate::config::MediaCodecFlags;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::recording::{MediaPacketSink, MediaSource, MediaTap, into_packet_sink};
use crate::runtime::rtc_adapter::{
    bootstrap,
    commands::{RemoteSourceControl, RtcWorkerCommand},
    demux::{MediaRouteDestination, MediaRouteEntry},
    media_registry::RegisteredMediaHandle,
    relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayRegistry, RelayTargetId},
    route_control::{KeyframeRequestDecision, PacketLayerGate},
    sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
    state::{RtcBootstrapState, RtcSnapshotState},
};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};
use crate::signaling::shared::SessionId;

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

fn valid_rtp_packet(sequence_number: u16, ssrc: u32) -> Vec<u8> {
    let sequence_number = sequence_number.to_be_bytes();
    let ssrc = ssrc.to_be_bytes();
    vec![
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]
}

#[test]
fn recent_miss_cache_skips_repeated_scans_for_the_same_source() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_001));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_000));
    let packet = valid_rtp_packet(1, 11);

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &packet,
    );
    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &packet,
    );

    assert_eq!(routing_state.fallback_attempts(), 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed, 0);
}

#[test]
fn recent_miss_cache_clears_on_topology_change() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_011));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_010));
    let packet = valid_rtp_packet(2, 22);

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &packet,
    );
    routing_state.clear_on_topology_change();
    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &packet,
    );

    assert_eq!(routing_state.fallback_attempts(), 2);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
}

#[test]
fn recent_miss_cache_does_not_skip_different_packets_from_the_same_source() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_021));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_020));

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &valid_rtp_packet(3, 33),
    );
    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &valid_rtp_packet(4, 44),
    );

    assert_eq!(routing_state.fallback_attempts(), 2);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 0);
}

#[test]
fn source_rate_limiter_bounds_varied_unknown_source_misses() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_026));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_025));

    for (sequence, ssrc) in [
        (5_u16, 55_u32),
        (6, 66),
        (7, 77),
        (8, 88),
        (9, 99),
        (10, 110),
    ] {
        route_packet_to_matching_session(
            &mut bootstrap_state,
            &snapshot_state,
            &mut routing_state,
            &metrics,
            source_addr,
            candidate_addr,
            &valid_rtp_packet(sequence, ssrc),
        );
    }

    assert_eq!(routing_state.fallback_attempts(), 4);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 4);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 4);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache, 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 2);
}

#[test]
fn malformed_udp_datagram_counts_as_malformed_drop_without_scan_metrics() {
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_031));
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_030));

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &[0x01, 0x02, 0x03],
    );

    let snapshot = metrics.snapshot();
    assert_eq!(routing_state.fallback_attempts(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 0);
    assert_eq!(snapshot.rtc_datagram_scan_sessions, 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed, 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited, 0);
}

#[test]
fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_040));
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let first_session = TransportSessionKey::new(51, 0, 52, SessionId::Integer(53));
    let second_session = TransportSessionKey::new(51, 0, 54, SessionId::Integer(55));
    let packet = [22, 0, 0, 0];
    let unknown_source_addr = SocketAddr::from(([127, 0, 0, 1], 45_041));

    let first_created = bootstrap::ensure_session_rtc_state(
        &mut bootstrap_state.sessions,
        &first_session,
        candidate_addr,
        MediaCodecFlags::default(),
    );
    let second_created = bootstrap::ensure_session_rtc_state(
        &mut bootstrap_state.sessions,
        &second_session,
        candidate_addr,
        MediaCodecFlags::default(),
    );

    assert_eq!(first_created, Ok(true));
    assert_eq!(second_created, Ok(true));

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        unknown_source_addr,
        candidate_addr,
        &packet,
    );

    let snapshot = metrics.snapshot();
    assert_eq!(routing_state.fallback_attempts(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans, 1);
    assert_eq!(snapshot.rtc_datagram_scan_sessions, 0);
    assert_eq!(snapshot.rtc_datagram_drops_no_session, 1);
}

#[test]
fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
    let producer_session = TransportSessionKey::new(18, 0, 19, SessionId::Integer(20));
    let mut state = RtcBootstrapState::default();
    let media_tap = MediaTap::default();
    let relay_registry = RelayRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let _source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();

    media_tap.activate_channel(
        producer_session.channel_runtime_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&sink)),
    );
    buffers.pending_packets.push(sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    ));

    super::super::forwarding_planner::populate_forward_routes(
        &state,
        &media_tap,
        &relay_registry,
        &metrics,
        &buffers.pending_packets,
        &mut buffers.forwards,
    );
    flush_forward_routes(&mut state, &metrics, &mut buffers);

    assert_eq!(buffers.forwards.len(), 1);
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.snapshot().rtp_payload_bytes_egress, 0);
    assert_eq!(metrics.snapshot().rtp_forwarded_packets_recording, 1);
}

#[test]
fn flush_forward_routes_records_non_local_forwarding_volume_by_destination() {
    let source_session = TransportSessionKey::new(118, 0, 119, SessionId::Integer(120));
    let source_transport_media_id = TransportMediaId::new(121);
    let mut state = RtcBootstrapState::default();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();
    let (inter_node_sender, mut inter_node_rx) = InterNodeRelaySender::channel_for_test();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload")
        .share_for_relay(source_transport_media_id);

    buffers.pending_packets.push(packet);
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_recording_sink(
            0,
            source_transport_media_id,
            into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        ),
    );
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_intra_node_relay_sink(
            0,
            source_transport_media_id,
            relay_mailbox,
        ),
    );
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_inter_node_relay_sink(
            0,
            source_transport_media_id,
            inter_node_sender,
        ),
    );

    flush_forward_routes(&mut state, &metrics, &mut buffers);

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert!(intra_node_rx.try_recv().is_ok());
    assert!(inter_node_rx.try_recv().is_ok());

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc, 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording, 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay, 1);
    assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay, 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc, 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording, 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay, 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay, 7);
    assert_eq!(snapshot.rtp_payload_bytes_egress, 0);
}

#[test]
fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
    let producer_session = TransportSessionKey::new(28, 0, 29, SessionId::Integer(30));
    let consumer_session = TransportSessionKey::new(28, 0, 31, SessionId::Integer(32));
    let mut state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let media_tap = MediaTap::default();
    let relay_registry = RelayRegistry::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: Mid::from("aud-down"),
            source_transport_media_id,
        });
    state.media_route_index.insert(
        source_transport_media_id,
        MediaRouteEntry {
            source_active: true,
            destinations: vec![MediaRouteDestination {
                dest_session: consumer_session,
                dest_transport_media_id: consumer_transport_media_id,
                dest_mid: Mid::from("aud-down"),
                active: true,
                packet_gate: PacketLayerGate::Open,
            }],
        },
    );
    let mut buffers = PacketLoopBuffers::new();
    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session,
            "aud-up",
            Some(false),
            Some(-72),
            b"payload",
        ));

    record_incoming_stats(&mut state, &snapshot_state, &metrics, &buffers);
    super::super::forwarding_planner::populate_forward_routes(
        &state,
        &media_tap,
        &relay_registry,
        &metrics,
        &buffers.pending_packets,
        &mut buffers.forwards,
    );

    assert!(buffers.forwards.is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped, 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed, 0);
}

#[test]
fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
    let source_session = TransportSessionKey::new(25, 0, 26, SessionId::Integer(27));
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();

    mailbox.forward_packet(&packet, TransportMediaId::new(17));
    drain_relay_packets(
        &mut relay_rx,
        &mut pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
    );

    assert_eq!(pending_packets.len(), 1);
    let forwarded = pending_packets.first();
    assert!(forwarded.is_some());
    let Some(forwarded) = forwarded else {
        return;
    };
    assert_eq!(forwarded.source_session_key(), &source_session);
    assert_eq!(forwarded.payload().as_slice(), b"payload");
    assert_eq!(
        forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
        Some(TransportMediaId::new(17))
    );
}

#[test]
fn drain_relay_packets_stops_at_the_configured_cap() {
    let source_session = TransportSessionKey::new(26, 0, 27, SessionId::Integer(28));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();

    mailbox.forward_packet(&packet, TransportMediaId::new(18));
    mailbox.forward_packet(&packet, TransportMediaId::new(18));

    let drained = drain_relay_packets(&mut relay_rx, &mut pending_packets, 1);

    assert_eq!(drained, 1);
    assert_eq!(pending_packets.len(), 1);
    assert!(relay_rx.try_recv().is_ok());
}

#[test]
fn flush_pending_keyframe_requests_marks_local_source_sessions_dirty() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_050));
    let source_session = TransportSessionKey::new(61, 0, 62, SessionId::Integer(63));
    let consumer_session = TransportSessionKey::new(61, 0, 64, SessionId::Integer(65));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.sessions,
            &source_session,
            candidate_addr,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.sessions.get_mut(&source_session) else {
        return;
    };
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(44_444_u32), None, source_mid, None);

    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    });
    let _consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    buffers.pending_keyframe_requests.push((
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    ));

    flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

    assert!(state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
}

#[test]
fn flush_pending_keyframe_requests_forwards_remote_sources_by_transport_media_id() {
    let source_session = TransportSessionKey::new(71, 0, 72, SessionId::Integer(73));
    let consumer_session = TransportSessionKey::new(71, 1, 74, SessionId::Integer(75));
    let consumer_mid = Mid::from("cam-down");
    let source_transport_media_id = TransportMediaId::new(91);
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(1);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(1)),
            )
            .is_ok()
    );
    let _consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    buffers.pending_keyframe_requests.push((
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    ));

    flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        }) if source_session_key == source_session
            && target_id == RelayTargetId::new(1)
            && forwarded_transport_media_id == source_transport_media_id
    ));
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded, 1);
}

#[test]
fn flush_pending_keyframe_requests_coalesces_duplicate_remote_requests() {
    let source_session = TransportSessionKey::new(81, 0, 82, SessionId::Integer(83));
    let first_consumer_session = TransportSessionKey::new(81, 1, 84, SessionId::Integer(85));
    let second_consumer_session = TransportSessionKey::new(81, 1, 86, SessionId::Integer(87));
    let consumer_mid = Mid::from("cam-down");
    let source_transport_media_id = TransportMediaId::new(101);
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let (control_tx, mut control_rx) = mpsc::channel(2);

    assert!(
        state
            .register_remote_source(
                source_transport_media_id,
                &source_session,
                RemoteSourceControl::new(control_tx, RelayTargetId::new(4)),
            )
            .is_ok()
    );
    let _first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: consumer_mid,
            source_transport_media_id,
        });
    let _second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: Mid::from("cam-down-2"),
            source_transport_media_id,
        });
    buffers.pending_keyframe_requests.push((
        first_consumer_session,
        PendingKeyframeRequest {
            consumer_mid,
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    ));
    buffers.pending_keyframe_requests.push((
        second_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-2"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    ));

    flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

    let command = control_rx.try_recv().ok();
    assert!(matches!(
        command,
        Some(RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id: forwarded_transport_media_id,
            target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        }) if source_session_key == source_session
            && target_id == RelayTargetId::new(4)
            && forwarded_transport_media_id == source_transport_media_id
    ));
    assert!(control_rx.try_recv().is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
}

#[test]
fn flush_pending_keyframe_requests_absorbs_duplicate_local_requests_within_one_flush() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_060));
    let source_session = TransportSessionKey::new(91, 0, 92, SessionId::Integer(93));
    let first_consumer_session = TransportSessionKey::new(91, 0, 94, SessionId::Integer(95));
    let second_consumer_session = TransportSessionKey::new(91, 0, 96, SessionId::Integer(97));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.sessions,
            &source_session,
            candidate_addr,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.sessions.get_mut(&source_session) else {
        return;
    };
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(55_555_u32), None, source_mid, None);

    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    });
    let _first_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: first_consumer_session.clone(),
            mid: Mid::from("cam-down-1"),
            source_transport_media_id,
        });
    let _second_consumer_transport_media_id =
        state.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: second_consumer_session.clone(),
            mid: Mid::from("cam-down-2"),
            source_transport_media_id,
        });
    buffers.pending_keyframe_requests.push((
        first_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-1"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    ));
    buffers.pending_keyframe_requests.push((
        second_consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("cam-down-2"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    ));

    flush_pending_keyframe_requests(&mut state, &metrics, &mut buffers);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(
        state
            .route_control
            .decide_keyframe_request(source_transport_media_id, Instant::now()),
        KeyframeRequestDecision::Absorb
    );
}
