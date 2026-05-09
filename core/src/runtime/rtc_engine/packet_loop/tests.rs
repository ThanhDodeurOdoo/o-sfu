//! Regression coverage for packet-loop contracts.
//!
//! These tests exercise the packet-loop helpers at their boundary points:
//! ingress demux caching, relay fanout, packet sink accounting, route-control
//! observations, keyframe feedback coalescing and scheduling deadlines. They
//! intentionally avoid running a full async worker unless the contract under
//! test requires worker scheduling behavior.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use str0m::{
    crypto::from_feature_flags,
    ice::{StunMessage, TransId},
    media::{KeyframeRequestKind, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tokio::sync::mpsc;

use super::{
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers},
    forward_flush::{drain_relay_packets, flush_forward_routes, record_incoming_stats},
    ingress_routing::route_packet_to_matching_session,
    keyframe_requests::{PendingKeyframeRequest, flush_pending_keyframe_requests},
};
use crate::{
    MediaCodecFlags,
    runtime::{
        RoomInstanceId, UserId,
        media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
        metrics::{
            RtpForwardDestinationKind, RtpMetricsRecorder, RuntimeMetrics,
            test_support::RuntimeMetricsSnapshotTestExt,
        },
        packet_sink_registry::{
            PacketSink as MediaPacketSink, PacketSinkLookup, RegisteredPacketSink,
            RoomPacketSinkRegistry, into_packet_sink,
        },
        rtc_engine::{
            bootstrap,
            commands::{RemoteSourceControl, RtcWorkerCommand},
            demux::{MediaRouteDestination, MediaRouteEntry},
            media_registry::RegisteredMediaHandle,
            relay_registry::{InterNodeRelaySender, RelayPacketMailbox, RelayTargetId},
            route_control::{KeyframeRequestDecision, PacketLayerGate},
            state::{RtcBootstrapState, RtcSnapshotState},
            test_support::{
                sample_forwarded_packet, sample_forwarded_packet_with_audio_activity,
                sample_forwarded_packet_with_rid, sample_forwarded_packet_without_mid,
                test_transport_session_key,
            },
        },
    },
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

fn populate_forward_routes(
    state: &RtcBootstrapState,
    packet_sinks: &impl PacketSinkLookup,
    metrics: &RuntimeMetrics,
    pending_packets: &mut [super::super::forwarded_packet::ForwardedPacket],
    forwards: &mut Vec<super::super::forwarding_destination::PacketForward>,
) {
    for (packet_idx, packet) in pending_packets.iter_mut().enumerate() {
        super::super::forwarding_planner::populate_forward_routes_for_packet(
            state,
            packet_sinks,
            metrics,
            packet_idx,
            packet,
            forwards,
        );
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

fn serialize_stun_message(message: &StunMessage<'_>, password: Option<&[u8]>) -> Option<Vec<u8>> {
    let mut buffer = [0_u8; 1024];
    let crypto_provider = from_feature_flags();
    let sha1_hmac = |key: &[u8], payloads: &[&[u8]]| {
        crypto_provider.sha1_hmac_provider.sha1_hmac(key, payloads)
    };
    let len = message.to_bytes(password, &mut buffer, sha1_hmac).ok()?;
    buffer.get(..len).map(<[u8]>::to_vec)
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 0);
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 2);
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_040));
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();
    let first_session = test_transport_session_key(51, 0, 52, UserId::Integer(53));
    let second_session = test_transport_session_key(51, 0, 54, UserId::Integer(55));
    let packet = [22, 0, 0, 0];
    let unknown_source_addr = SocketAddr::from(([127, 0, 0, 1], 45_041));

    let first_created = bootstrap::ensure_session_rtc_state(
        &mut bootstrap_state.users,
        &first_session,
        candidate_addr,
        10_000_000,
        MediaCodecFlags::default(),
    );
    let second_created = bootstrap::ensure_session_rtc_state(
        &mut bootstrap_state.users,
        &second_session,
        candidate_addr,
        10_000_000,
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
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
}

#[test]
fn indexed_route_stays_cached_without_touching_recent_miss_state() -> Result<(), &'static str> {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_045));
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_046));
    let session_key = test_transport_session_key(51, 0, 56, UserId::Integer(57));
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();

    let created = bootstrap::ensure_session_rtc_state(
        &mut bootstrap_state.users,
        &session_key,
        candidate_addr,
        10_000_000,
        MediaCodecFlags::default(),
    );
    assert_eq!(created, Ok(true));
    let local_ice_credentials = bootstrap_state
        .users
        .get_mut(&session_key)
        .map(|session_state| session_state.rtc.direct_api().local_ice_credentials())
        .ok_or("session state missing after creation")?;
    assert!(
        bootstrap_state
            .remote_addr_demux
            .remember_remote_addr(source_addr, &session_key)
    );
    {
        let Ok(mut snapshot) = snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .remember_remote_addr(source_addr, &session_key)
        );
    }

    let username = format!("{}:remote-ufrag", local_ice_credentials.ufrag);
    let packet = serialize_stun_message(
        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, false),
        Some(local_ice_credentials.pass.as_bytes()),
    )
    .ok_or("failed to serialize STUN binding request")?;
    let miss_key = super::super::routing_miss::PacketLoopRoutingMissKey::new(
        source_addr,
        candidate_addr,
        &packet,
    );
    routing_state.record_miss(miss_key, &packet, source_addr, Instant::now());

    assert!(routing_state.should_skip_scan(miss_key, &packet));
    assert!(routing_state.source_is_tracked(source_addr));

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &packet,
    );

    assert_eq!(routing_state.fallback_attempts(), 0);
    assert_eq!(
        bootstrap_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr),
        Some(&session_key)
    );
    {
        let Ok(snapshot) = snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert_eq!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(source_addr),
            Some(&session_key)
        );
    }
    assert!(routing_state.should_skip_scan(miss_key, &packet));
    assert!(routing_state.source_is_tracked(source_addr));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    Ok(())
}

#[test]
fn stale_indexed_route_clears_worker_and_snapshot_pins() -> Result<(), &'static str> {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_047));
    let source_addr = SocketAddr::from(([127, 0, 0, 1], 45_048));
    let stale_session_key = test_transport_session_key(51, 0, 58, UserId::Integer(59));
    let mut bootstrap_state = RtcBootstrapState::default();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut routing_state = super::super::routing_miss::PacketLoopRoutingState::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap_state
            .remote_addr_demux
            .remember_remote_addr(source_addr, &stale_session_key)
    );
    {
        let Ok(mut snapshot) = snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .remember_remote_addr(source_addr, &stale_session_key)
        );
    }

    route_packet_to_matching_session(
        &mut bootstrap_state,
        &snapshot_state,
        &mut routing_state,
        &metrics,
        source_addr,
        candidate_addr,
        &valid_rtp_packet(11, 111),
    );

    assert!(
        bootstrap_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr)
            .is_none()
    );
    {
        let Ok(snapshot) = snapshot_state.lock() else {
            return Err("snapshot state lock poisoned");
        };
        assert!(
            snapshot
                .remote_addr_demux
                .session_key_for_remote_addr(source_addr)
                .is_none()
        );
    }
    assert_eq!(routing_state.fallback_attempts(), 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    Ok(())
}

#[test]
fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let mut state = RtcBootstrapState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let _source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();

    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        into_packet_sink(Arc::<CountingSink>::clone(&sink)),
        RtpForwardDestinationKind::Recording,
    );
    buffers.pending_packets.push(sample_forwarded_packet(
        producer_session,
        "aud-up",
        b"payload",
    ));

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut buffers.pending_packets,
        &mut buffers.forwards,
    );
    flush_forward_routes(&mut state, &metrics, &rtp_metrics, &mut buffers);

    assert_eq!(buffers.forwards.len(), 1);
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.snapshot().rtp_payload_bytes_egress(), 0);
    assert_eq!(metrics.snapshot().rtp_forwarded_packets_recording(), 1);
}

#[test]
fn flush_forward_routes_writes_hot_rtp_metrics_only_to_the_worker_recorder() {
    let source_session = test_transport_session_key(128, 0, 129, UserId::Integer(130));
    let source_transport_media_id = TransportMediaId::new(131);
    let mut state = RtcBootstrapState::default();
    let sink = Arc::new(CountingSink::new());
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = RtpMetricsRecorder::default();
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");

    buffers.pending_packets.push(packet);
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_packet_sink(
            0,
            source_transport_media_id,
            RegisteredPacketSink::new(
                into_packet_sink(Arc::<CountingSink>::clone(&sink)),
                RtpForwardDestinationKind::Recording,
            ),
        ),
    );

    flush_forward_routes(&mut state, &metrics, &rtp_metrics, &mut buffers);

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 0);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
}

#[test]
fn record_incoming_stats_learns_dynamic_rid_ssrc_bindings_from_rtp_extensions() {
    let producer_session = test_transport_session_key(88, 0, 89, UserId::Integer(90));
    let producer_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &SourcePolicySignal::default(),
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );

    let mut packet_without_extensions =
        sample_forwarded_packet_without_mid(producer_session, 4321, b"payload");
    assert_eq!(
        packet_without_extensions.resolve_source_transport_media_id(&state),
        Some(source_transport_media_id)
    );
    assert_eq!(
        packet_without_extensions
            .resolve_route_control_layer_metadata(&state)
            .rid(),
        Some(Rid::from("hi"))
    );
}

#[test]
fn flush_forward_routes_records_non_local_forwarding_volume_by_destination() {
    let source_session = test_transport_session_key(118, 0, 119, UserId::Integer(120));
    let source_transport_media_id = TransportMediaId::new(121);
    let mut state = RtcBootstrapState::default();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();
    let (inter_node_sender, mut inter_node_rx) = InterNodeRelaySender::channel_for_test();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload")
        .share_for_relay(source_transport_media_id);

    buffers.pending_packets.push(packet);
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_packet_sink(
            0,
            source_transport_media_id,
            RegisteredPacketSink::new(
                into_packet_sink(Arc::<CountingSink>::clone(&sink)),
                RtpForwardDestinationKind::Recording,
            ),
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

    flush_forward_routes(&mut state, &metrics, &rtp_metrics, &mut buffers);

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert!(intra_node_rx.try_recv().is_ok());
    assert!(inter_node_rx.try_recv().is_ok());

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_inter_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_inter_node_relay(), 7);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
}

#[test]
fn flush_forward_routes_marks_local_consumer_sessions_dirty() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_051));
    let producer_session = test_transport_session_key(218, 0, 219, UserId::Integer(220));
    let consumer_session = test_transport_session_key(218, 0, 221, UserId::Integer(222));
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &consumer_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(consumer_session_state) = state.users.get_mut(&consumer_session) else {
        return;
    };
    let mut direct_api = consumer_session_state.rtc.direct_api();
    direct_api.declare_media(consumer_mid, MediaKind::Video);
    direct_api.declare_stream_tx(Ssrc::from(223_001_u32), None, consumer_mid, None);

    buffers.pending_packets.push(sample_forwarded_packet(
        producer_session,
        "cam-up",
        b"payload",
    ));
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_local_route_destination(
            0,
            &MediaRouteDestination {
                dest_session: consumer_session.clone(),
                dest_transport_media_id: TransportMediaId::new(223),
                dest_mid: consumer_mid,
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
            },
        ),
    );

    flush_forward_routes(&mut state, &metrics, &rtp_metrics, &mut buffers);

    assert!(state.dirty_sessions.contains(&consumer_session));
    assert_eq!(metrics.snapshot().rtp_forwarded_packets_local_rtc(), 1);
}

#[test]
fn packet_loop_wakes_immediately_when_forwarding_marks_a_session_dirty() {
    let mut state = RtcBootstrapState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let future_timeout = Instant::now() + Duration::from_secs(30);

    state.update_session_timeout(&session, Some(future_timeout));
    state.mark_session_dirty(&session);

    let deadline = super::loop_driver::next_timeout_deadline(&mut state);

    assert!(deadline.is_some_and(|deadline| deadline <= Instant::now()));
}

#[test]
fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
    let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let consumer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
    let mut state = RtcBootstrapState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
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
                dest_payload_type: None,
                active: true,
                packet_gate: PacketLayerGate::Open,
                pending_packet_gate: None,
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

    record_incoming_stats(
        &mut state,
        &SourcePolicySignal::default(),
        &metrics,
        &rtp_metrics,
        &mut buffers,
    );
    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut buffers.pending_packets,
        &mut buffers.forwards,
    );

    assert!(buffers.forwards.is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 0);
}

#[test]
fn packet_loop_buffers_coalesce_source_policy_dirty_rooms_before_signal_flush() {
    let mut buffers = PacketLoopBuffers::new();
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(42));

    buffers.flush_source_policy_dirty(&signal);

    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([RoomInstanceId::from_raw(41), RoomInstanceId::from_raw(42),])
    );
    assert!(subscription.take_pending_updates().is_empty());
}

#[test]
fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
    let source_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
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
    let forwarded = pending_packets.first_mut();
    assert!(forwarded.is_some());
    let Some(forwarded) = forwarded else {
        return;
    };
    assert_eq!(forwarded.source_session_key(), &source_session);
    assert_eq!(forwarded.payload(), b"payload");
    assert_eq!(
        forwarded.resolve_source_transport_media_id(&RtcBootstrapState::default()),
        Some(TransportMediaId::new(17))
    );
}

#[test]
fn drain_relay_packets_stops_at_the_configured_cap() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
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
fn flush_forward_routes_records_relay_overload_drops() {
    let source_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let source_transport_media_id = TransportMediaId::new(32);
    let mut state = RtcBootstrapState::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();
    let rtp_metrics = metrics.register_rtp_worker();
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload")
        .share_for_relay(source_transport_media_id);

    relay_mailbox.forward_packet(
        &sample_forwarded_packet(
            test_transport_session_key(29, 0, 30, UserId::Integer(31)),
            "aud-up",
            b"prefill",
        ),
        source_transport_media_id,
    );
    buffers.pending_packets.push(packet);
    buffers.forwards.push(
        super::super::forwarding_destination::PacketForward::from_intra_node_relay_sink(
            0,
            source_transport_media_id,
            relay_mailbox,
        ),
    );

    flush_forward_routes(&mut state, &metrics, &rtp_metrics, &mut buffers);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
}

#[test]
fn flush_pending_keyframe_requests_marks_local_source_sessions_dirty() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_050));
    let source_session = test_transport_session_key(61, 0, 62, UserId::Integer(63));
    let consumer_session = test_transport_session_key(61, 0, 64, UserId::Integer(65));
    let source_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(&source_session) else {
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
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_forwards_remote_sources_by_transport_media_id() {
    let source_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 1, 74, UserId::Integer(75));
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
    assert_eq!(metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn flush_pending_keyframe_requests_coalesces_duplicate_remote_requests() {
    let source_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let first_consumer_session = test_transport_session_key(81, 1, 84, UserId::Integer(85));
    let second_consumer_session = test_transport_session_key(81, 1, 86, UserId::Integer(87));
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
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_keyframe_requests_absorbs_duplicate_local_requests_within_one_flush() {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 45_060));
    let source_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let first_consumer_session = test_transport_session_key(91, 0, 94, UserId::Integer(95));
    let second_consumer_session = test_transport_session_key(91, 0, 96, UserId::Integer(97));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let mut buffers = PacketLoopBuffers::new();
    let metrics = RuntimeMetrics::default();

    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &source_session,
            candidate_addr,
            10_000_000,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(&source_session) else {
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
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
    assert!(state.dirty_sessions.contains(&source_session));
    assert_eq!(
        state
            .route_control
            .decide_keyframe_request(source_transport_media_id, Instant::now()),
        KeyframeRequestDecision::Absorb
    );
}
