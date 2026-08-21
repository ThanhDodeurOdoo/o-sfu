use o_sfu_rfc::rtp::RTX_ORIGINAL_SEQUENCE_NUMBER_BYTES;

use super::support::{self as s, media as m, setup as st};

const RECOVERY_TIMEOUT: s::Duration = s::Duration::from_secs(3);
const POST_RECOVERY_SETTLE: s::Duration = s::Duration::from_millis(200);
const RTC_POLL_SLICE: s::Duration = s::Duration::from_millis(50);
const GAP_EXPOSING_PACKET_COUNT: usize = 6;
const RIDLESS_POLICY_WARMUP_PACKET_COUNT: usize = 60;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedPublisherFeedback {
    None,
    NackAndRtx,
}

async fn ready_nack_route(
    issuer: &str,
    publisher_user_id: i64,
    subscriber_user_id: i64,
    rid: Option<&str>,
) -> s::TestResult<(st::ReadyRoomFakePeers, s::FakeMediaSource, s::FakeClock)> {
    let publisher_id = s::UserId::Integer(publisher_user_id);
    let subscriber_id = s::UserId::Integer(subscriber_user_id);
    let (server, room) = st::room_parts(issuer).await?;
    let publisher = if rid.is_none() {
        s::connect_ridless_video_fake_peer(&server, &room, publisher_id.clone(), s::TEST_ROOM_KEY)
            .await
    } else {
        s::connect_fake_peer(&server, &room, publisher_id.clone(), s::TEST_ROOM_KEY).await
    };
    let mut publisher = s::require_some(publisher, "publisher should connect")?;
    let mut subscriber = s::require_some(
        s::connect_fake_peer(&server, &room, subscriber_id, s::TEST_ROOM_KEY).await,
        "subscriber should connect",
    )?;
    s::require_some(
        publisher
            .wait_until_connected(s::Duration::from_secs(5))
            .await,
        "publisher should reach ready state",
    )?;
    s::require_some(
        subscriber
            .wait_until_connected(s::Duration::from_secs(5))
            .await,
        "subscriber should reach ready state",
    )?;
    let mut peers = st::ReadyRoomFakePeers {
        server,
        room,
        publisher,
        subscriber,
    };
    let mut source = s::FakeMediaSource::new(s::SyntheticVp8Stream::new(rid.map(str::to_owned)));
    if rid.is_none() {
        m::publish_video_source(
            &mut peers.publisher,
            &mut peers.subscriber,
            &publisher_id,
            &source,
        )
        .await;
    } else {
        m::publish_video_source_and_ready_route(
            &peers.server,
            &peers.room,
            &mut peers.publisher,
            &mut peers.subscriber,
            &publisher_id,
            &source,
        )
        .await;
    }

    let mut clock = s::FakeClock::default();
    if rid.is_none() {
        let mut forwarded = false;
        for _ in 0..RIDLESS_POLICY_WARMUP_PACKET_COUNT {
            let expected_payload = s::require_some(
                peers
                    .publisher
                    .send_rtp_packet(&mut source, &mut clock)
                    .await,
                "RID-less publisher should send bitrate-policy warmup",
            )?;
            if m::read_expected_rtp_payload(
                &mut peers.publisher,
                &mut peers.subscriber,
                &expected_payload,
                RTC_POLL_SLICE,
            )
            .await
            {
                forwarded = true;
                break;
            }
        }
        assert!(forwarded, "RID-less publisher route should become active");
        m::assert_consumer_route(
            &peers.server,
            &peers.room,
            &peers.subscriber,
            &publisher_id,
            s::StreamType::Camera,
            m::RouteState::Active,
        )
        .await;
    } else {
        m::assert_synthetic_video_packet_forwarded(
            &mut peers.publisher,
            &mut peers.subscriber,
            &mut source,
            &mut clock,
        )
        .await;
    }
    Ok((peers, source, clock))
}

async fn recover_publisher_gap(
    publisher: &mut s::ProtocolFakePeer,
    subscriber: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
    rid: Option<&str>,
) -> s::TestResult<(u8, u32, u32, s::ReceivedRtpPacket)> {
    let (primary_payload_type, repair_payload_type) = s::require_some(
        publisher.repair_payload_types(&s::CodecName::Vp8),
        "publisher should negotiate VP8 RTX",
    )?;
    let (primary_ssrc, repair_ssrc) = s::require_some(
        publisher.send_stream_ssrc_pair(source.media_kind(), rid),
        "publisher should negotiate a matching primary and repair SSRC pair",
    )?;
    let (dropped, trace, expected_payload) = assert_publisher_gap_nack(
        publisher,
        source,
        clock,
        primary_payload_type,
        primary_ssrc,
        ExpectedPublisherFeedback::NackAndRtx,
    )
    .await?;
    let recovered = s::require_some(
        read_payload(subscriber, &expected_payload, RECOVERY_TIMEOUT).await,
        "subscriber should receive the repaired publisher packet",
    )?;
    assert_publisher_recovery(
        &trace,
        dropped,
        repair_payload_type,
        repair_ssrc,
        &expected_payload,
        rid,
    )?;
    assert!(
        read_sequence(subscriber, recovered.sequence_number, POST_RECOVERY_SETTLE)
            .await
            .is_none(),
        "subscriber should receive one normalized repair"
    );
    assert_eq!(trace.keyframe_requests, 0);
    Ok((primary_payload_type, primary_ssrc, repair_ssrc, recovered))
}

async fn assert_camera_route(
    server: &s::TestServer,
    room: &str,
    subscriber: &s::ProtocolFakePeer,
    publisher_id: &s::UserId,
    state: m::RouteState,
) {
    m::assert_consumer_route(
        server,
        room,
        subscriber,
        publisher_id,
        s::StreamType::Camera,
        state,
    )
    .await;
}

async fn warm_publisher_route(
    publisher: &mut s::ProtocolFakePeer,
    subscriber: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
) -> s::TestResult<(Vec<u8>, s::ReceivedRtpPacket)> {
    let mut source_payloads = Vec::with_capacity(GAP_EXPOSING_PACKET_COUNT);
    for _ in 0..GAP_EXPOSING_PACKET_COUNT {
        source_payloads.push(s::require_some(
            publisher.send_rtp_packet(source, clock).await,
            "publisher should send a route warmup packet",
        )?);
        if let Some(packet) = read_matching_packet(subscriber, RTC_POLL_SLICE, |packet| {
            source_payloads
                .iter()
                .any(|payload| packet.payload.get(6..) == payload.get(6..))
        })
        .await
        {
            let source_payload = source_payloads
                .into_iter()
                .find(|payload| packet.payload.get(6..) == payload.get(6..));
            return Ok((
                s::require_some(source_payload, "forwarded warmup should match its source")?,
                packet,
            ));
        }
    }
    s::require_some(None, "publisher route should forward after warmup")
}

async fn assert_nack_suppressed_through_inactive_rebind(
    publisher: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
    primary_payload_type: u8,
    primary_ssrc: u32,
    repair_ssrc: u32,
) -> s::TestResult<(u32, u32)> {
    assert!(
        publisher
            .reset_rtp_ssrc(source.media_kind(), Some("hi"))
            .is_some()
    );
    let (replacement_ssrc, replacement_repair_ssrc) = s::require_some(
        publisher.send_stream_ssrc_pair(source.media_kind(), Some("hi")),
        "publisher should configure a replacement VP8 repair pair",
    )?;
    assert_ne!(replacement_ssrc, primary_ssrc);
    assert_ne!(replacement_repair_ssrc, repair_ssrc);
    assert!(publisher.send_rtp_packet(source, clock).await.is_some());
    assert!(publisher.pump_rtc(POST_RECOVERY_SETTLE).await.is_some());
    assert_publisher_gap_nack(
        publisher,
        source,
        clock,
        primary_payload_type,
        replacement_ssrc,
        ExpectedPublisherFeedback::None,
    )
    .await?;

    let audio_source = s::FakeMediaSource::audio();
    assert!(publisher.publish_track(&audio_source).await.is_some());
    assert!(publisher.complete_next_negotiation().await.is_some());
    assert_publisher_gap_nack(
        publisher,
        source,
        clock,
        primary_payload_type,
        replacement_ssrc,
        ExpectedPublisherFeedback::None,
    )
    .await?;
    Ok((replacement_ssrc, replacement_repair_ssrc))
}

#[tokio::test]
async fn fake_rtc_recovers_publisher_video_loss_with_nack_and_rtx() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (peers, mut source, mut clock) = Box::pin(ready_nack_route(
        "issuer-publisher-nack-recovery",
        110,
        111,
        Some("hi"),
    ))
    .await?;
    let publisher_id = s::UserId::Integer(110);
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = peers;
    let (primary_payload_type, primary_ssrc, repair_ssrc, _) = recover_publisher_gap(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
        Some("hi"),
    )
    .await?;

    assert!(
        publisher
            .set_publication_active(s::StreamType::Camera, false)
            .await
            .is_some()
    );
    assert_camera_route(
        &server,
        &room,
        &subscriber,
        &publisher_id,
        m::RouteState::Inactive,
    )
    .await;

    let (replacement_ssrc, replacement_repair_ssrc) =
        assert_nack_suppressed_through_inactive_rebind(
            &mut publisher,
            &mut source,
            &mut clock,
            primary_payload_type,
            primary_ssrc,
            repair_ssrc,
        )
        .await?;

    assert!(
        publisher
            .set_publication_active(s::StreamType::Camera, true)
            .await
            .is_some()
    );
    assert_camera_route(
        &server,
        &room,
        &subscriber,
        &publisher_id,
        m::RouteState::Active,
    )
    .await;
    let (replacement_source_anchor, replacement_anchor) =
        warm_publisher_route(&mut publisher, &mut subscriber, &mut source, &mut clock).await?;

    let (replacement_dropped, replacement_trace, source_expected_payload) =
        assert_publisher_gap_nack(
            &mut publisher,
            &mut source,
            &mut clock,
            primary_payload_type,
            replacement_ssrc,
            ExpectedPublisherFeedback::NackAndRtx,
        )
        .await?;
    let replacement_rtx = s::require_some(
        matching_rtx(
            &replacement_trace,
            s::RtcTraceDirection::Tx,
            replacement_dropped.sequence_number,
        ),
        "replacement primary should recover through RTX",
    )?;
    assert_eq!(replacement_rtx.ssrc, replacement_repair_ssrc);
    assert_retired_repair_pair_unused(&replacement_trace, primary_ssrc, repair_ssrc);
    let expected_payload = s::require_some(
        s::project_synthetic_vp8_payload(
            &replacement_source_anchor,
            &replacement_anchor.payload,
            source_expected_payload,
        ),
        "replacement repair should have a projected VP8 identity",
    )?;
    s::require_some(
        read_payload(&mut subscriber, &expected_payload, RECOVERY_TIMEOUT).await,
        "subscriber should receive the normalized replacement repair",
    )?;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_recovers_ridless_fid_publisher_video_loss() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (peers, mut source, mut clock) = Box::pin(ready_nack_route(
        "issuer-ridless-fid-recovery",
        116,
        117,
        None,
    ))
    .await?;
    let st::ReadyRoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = peers;
    let (_, _, _, recovered) = recover_publisher_gap(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
        None,
    )
    .await?;
    assert_eq!(recovered.rid, None);
    Ok(())
}

#[tokio::test]
async fn fake_rtc_recovers_low_simulcast_rid_publisher_video_loss() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (peers, mut source, mut clock) = Box::pin(ready_nack_route(
        "issuer-low-rid-nack-recovery",
        118,
        119,
        Some("lo"),
    ))
    .await?;
    let st::ReadyRoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = peers;
    recover_publisher_gap(
        &mut publisher,
        &mut subscriber,
        &mut source,
        &mut clock,
        Some("lo"),
    )
    .await?;
    Ok(())
}

async fn assert_publisher_gap_nack(
    publisher: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
    primary_payload_type: u8,
    primary_ssrc: u32,
    expected: ExpectedPublisherFeedback,
) -> s::TestResult<(s::DroppedRtpPacket, s::RtcPeerTrace, Vec<u8>)> {
    publisher.start_rtc_trace();
    publisher.hold_outbound_rtp(primary_payload_type, primary_ssrc);
    let expected_payload = s::require_some(
        publisher.send_rtp_packet(source, clock).await,
        "publisher should send the packet selected for loss",
    )?;
    publisher.clear_outbound_rtp_hold();
    let mut trace = publisher.take_rtc_trace();
    assert!(
        pump_until_drop(
            publisher,
            &mut trace,
            s::RtcTraceDirection::Tx,
            RECOVERY_TIMEOUT,
        )
        .await
        .is_some()
    );
    let dropped = s::require_some(
        only_dropped_packet(&trace, s::RtcTraceDirection::Tx),
        "publisher should drop exactly one selected packet",
    )?;
    assert!(
        publisher
            .send_rtp_packets(source, clock, GAP_EXPOSING_PACKET_COUNT)
            .await
            .is_some()
    );
    if expected == ExpectedPublisherFeedback::NackAndRtx {
        assert!(
            pump_until_rtx(
                publisher,
                &mut trace,
                s::RtcTraceDirection::Tx,
                dropped.sequence_number,
                RECOVERY_TIMEOUT,
            )
            .await
            .is_some()
        );
    } else {
        assert!(publisher.pump_rtc(POST_RECOVERY_SETTLE).await.is_some());
        merge_trace(&mut trace, publisher.take_rtc_trace());
    }
    assert_eq!(
        nack_contains(
            &trace,
            s::RtcTraceDirection::Rx,
            dropped.ssrc,
            dropped.sequence_number,
        ),
        expected != ExpectedPublisherFeedback::None
    );
    assert_eq!(
        matching_rtx(&trace, s::RtcTraceDirection::Tx, dropped.sequence_number).is_some(),
        expected == ExpectedPublisherFeedback::NackAndRtx
    );
    if expected == ExpectedPublisherFeedback::None {
        assert!(publisher.release_next_held_outbound_rtp().await.is_some());
    } else {
        assert!(publisher.discard_next_held_outbound_rtp());
    }
    assert!(publisher.pump_rtc(POST_RECOVERY_SETTLE).await.is_some());
    Ok((dropped, trace, expected_payload))
}

#[tokio::test]
async fn fake_rtc_does_not_forward_publisher_rtx_after_primary_delivery() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (peers, mut source, mut clock) = Box::pin(ready_nack_route(
        "issuer-publisher-rtx-duplicate",
        114,
        115,
        Some("hi"),
    ))
    .await?;
    let st::ReadyRoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = peers;
    publisher.start_rtc_trace();

    let (primary_payload_type, repair_payload_type) = s::require_some(
        publisher.repair_payload_types(&s::CodecName::Vp8),
        "publisher should negotiate VP8 RTX",
    )?;
    let (primary_ssrc, repair_ssrc) = s::require_some(
        publisher.send_stream_ssrc_pair(source.media_kind(), Some("hi")),
        "publisher should configure a VP8 repair SSRC",
    )?;
    publisher.hold_outbound_rtp(primary_payload_type, primary_ssrc);
    let expected_payload = s::require_some(
        publisher.send_rtp_packet(&mut source, &mut clock).await,
        "publisher should send the primary packet selected for reordering",
    )?;
    publisher.clear_outbound_rtp_hold();
    assert_eq!(publisher.held_outbound_rtp_count(), 1);

    publisher.hold_outbound_rtp(repair_payload_type, repair_ssrc);
    s::require_some(
        publisher
            .send_rtp_packets(&mut source, &mut clock, GAP_EXPOSING_PACKET_COUNT)
            .await,
        "publisher should expose the held primary gap",
    )?;
    s::require_some(
        pump_until_held_outbound_rtp_count(&mut publisher, 2, RECOVERY_TIMEOUT).await,
        "publisher should hold the matching RTX packet",
    )?;
    publisher.clear_outbound_rtp_hold();
    let publisher_trace = publisher.take_rtc_trace();
    let primary = s::require_some(
        matching_primary_payload(
            &publisher_trace,
            s::RtcTraceDirection::Tx,
            &expected_payload,
        ),
        "publisher trace should contain the held primary packet",
    )?;
    let rtx = s::require_some(
        publisher_trace.rtp_packets.iter().find(|packet| {
            packet.direction == s::RtcTraceDirection::Tx
                && packet.original_sequence_number == Some(primary.sequence_number)
        }),
        "publisher trace should contain a valid RTX packet for the held primary",
    )?;
    assert_eq!(rtx.payload_type, repair_payload_type);
    assert_eq!(rtx.ssrc, repair_ssrc);

    s::require_some(
        publisher.release_next_held_outbound_rtp().await,
        "publisher should release the held primary packet first",
    )?;
    s::require_some(
        read_payload(&mut subscriber, &expected_payload, RECOVERY_TIMEOUT).await,
        "subscriber should receive the released primary packet",
    )?;

    s::require_some(
        publisher.release_next_held_outbound_rtp().await,
        "publisher should release the matching RTX packet second",
    )?;
    assert!(
        subscriber
            .read_rtp_packet(POST_RECOVERY_SETTLE)
            .await
            .is_none()
    );
    Ok(())
}

async fn restart_publisher_vp8_and_assert_rewrite(
    publisher: &mut s::ProtocolFakePeer,
    subscriber: &mut s::ProtocolFakePeer,
    source: &mut s::FakeMediaSource,
    clock: &mut s::FakeClock,
) -> s::TestResult<(s::FakeMediaSource, Vec<u8>, s::ReceivedRtpPacket)> {
    let sample_payload = s::require_some(
        publisher.send_rtp_packet(source, clock).await,
        "publisher should send the primary stream sample",
    )?;
    let sample = s::require_some(
        read_payload(subscriber, &sample_payload, RECOVERY_TIMEOUT).await,
        "subscriber should receive the primary stream sample",
    )?;
    s::require_some(
        publisher.reset_rtp_ssrc(source.media_kind(), Some("hi")),
        "publisher should restart its VP8 stream before subscriber loss",
    )?;
    let mut restarted_source = s::FakeMediaSource::vp8_camera_high();
    let restart_payload = s::require_some(
        publisher
            .send_rtp_packet(&mut restarted_source, clock)
            .await,
        "publisher should establish the restarted VP8 stream",
    )?;
    let restart_sample = s::require_some(
        read_matching_packet(subscriber, RECOVERY_TIMEOUT, |packet| {
            packet.payload.get(6..) == restart_payload.get(6..)
        })
        .await,
        "subscriber should receive the rewritten VP8 restart",
    )?;
    assert_eq!(restart_sample.payload_type, sample.payload_type);
    assert_eq!(restart_sample.ssrc, sample.ssrc);
    assert_ne!(restart_sample.payload.as_ref(), restart_payload.as_slice());
    Ok((restarted_source, restart_payload, restart_sample))
}

#[tokio::test]
async fn fake_rtc_recovers_subscriber_video_loss_with_nack_and_rtx() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let (peers, mut source, mut clock) = Box::pin(ready_nack_route(
        "issuer-subscriber-nack-recovery",
        112,
        113,
        Some("hi"),
    ))
    .await?;
    let st::ReadyRoomFakePeers {
        server: _server,
        room: _room,
        mut publisher,
        mut subscriber,
    } = peers;
    let (mut restarted_source, restart_source_anchor, restart_sample) =
        restart_publisher_vp8_and_assert_rewrite(
            &mut publisher,
            &mut subscriber,
            &mut source,
            &mut clock,
        )
        .await?;
    let (_, repair_payload_type) = s::require_some(
        subscriber.repair_payload_types(&s::CodecName::Vp8),
        "subscriber should negotiate VP8 RTX",
    )?;
    let repair_ssrc = s::require_some(
        subscriber.receive_repair_ssrc(restart_sample.ssrc),
        "subscriber should negotiate a VP8 repair SSRC",
    )?;
    publisher.start_rtc_trace();
    subscriber.start_rtc_trace();
    subscriber.drop_next_inbound_rtp(restart_sample.payload_type, restart_sample.ssrc);
    let expected_payload = s::require_some(
        publisher
            .send_rtp_packet(&mut restarted_source, &mut clock)
            .await,
        "publisher should send the packet selected for subscriber loss",
    )?;
    s::require_some(
        publisher
            .send_rtp_packets(&mut restarted_source, &mut clock, GAP_EXPOSING_PACKET_COUNT)
            .await,
        "publisher should send packets after the subscriber gap",
    )?;

    let mut subscriber_trace = s::RtcPeerTrace::default();
    let packet_after_drop = s::require_some(
        read_until_drop(
            &mut subscriber,
            &mut subscriber_trace,
            s::RtcTraceDirection::Rx,
            RECOVERY_TIMEOUT,
        )
        .await,
        "subscriber should receive a packet after the gap",
    )?;
    let dropped = s::require_some(
        only_dropped_packet(&subscriber_trace, s::RtcTraceDirection::Rx),
        "subscriber should drop exactly one selected packet",
    )?;
    let recovered = if packet_after_drop.sequence_number == dropped.sequence_number {
        Some(packet_after_drop)
    } else {
        read_sequence(&mut subscriber, dropped.sequence_number, RECOVERY_TIMEOUT).await
    };
    merge_trace(&mut subscriber_trace, subscriber.take_rtc_trace());
    let recovered = s::require_some(recovered, "subscriber should receive its repaired packet")?;
    let expected_rewritten_payload = s::require_some(
        s::project_synthetic_vp8_payload(
            &restart_source_anchor,
            &restart_sample.payload,
            expected_payload.clone(),
        ),
        "subscriber repair should have a projected VP8 identity",
    )?;

    assert_subscriber_recovery(
        &subscriber_trace,
        dropped,
        repair_payload_type,
        repair_ssrc,
        &recovered,
    )?;
    assert_ne!(recovered.payload.as_ref(), expected_payload.as_slice());
    assert_eq!(
        recovered.payload.as_ref(),
        expected_rewritten_payload.as_slice()
    );
    assert!(
        read_sequence(
            &mut subscriber,
            recovered.sequence_number,
            POST_RECOVERY_SETTLE,
        )
        .await
        .is_none(),
        "subscriber should emit one normalized repair"
    );
    assert_eq!(subscriber_trace.keyframe_requests, 0);

    assert_no_publisher_repair_feedback(&mut publisher).await?;
    Ok(())
}

fn assert_retired_repair_pair_unused(trace: &s::RtcPeerTrace, primary_ssrc: u32, repair_ssrc: u32) {
    assert!(
        !trace.nacks.iter().any(|nack| {
            nack.direction == s::RtcTraceDirection::Rx && nack.ssrc == primary_ssrc
        })
    );
    assert!(!trace.rtp_packets.iter().any(|packet| {
        packet.direction == s::RtcTraceDirection::Tx && packet.ssrc == repair_ssrc
    }));
}

async fn assert_no_publisher_repair_feedback(publisher: &mut s::ProtocolFakePeer) -> s::TestResult {
    s::require_some(
        publisher.pump_rtc(POST_RECOVERY_SETTLE).await,
        "publisher RTC should process any forwarded feedback",
    )?;
    let trace = publisher.take_rtc_trace();
    assert!(
        !trace
            .nacks
            .iter()
            .any(|nack| nack.direction == s::RtcTraceDirection::Rx)
    );
    assert_eq!(trace.keyframe_requests, 0);
    Ok(())
}

fn only_dropped_packet(
    trace: &s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
) -> Option<s::DroppedRtpPacket> {
    let mut packets = trace
        .dropped_rtp_packets
        .iter()
        .filter(|packet| packet.direction == direction);
    let packet = *packets.next()?;
    packets.next().is_none().then_some(packet)
}

fn assert_publisher_recovery(
    trace: &s::RtcPeerTrace,
    dropped: s::DroppedRtpPacket,
    repair_payload_type: u8,
    repair_ssrc: u32,
    expected_payload: &[u8],
    expected_rid: Option<&str>,
) -> s::TestResult {
    // Generic NACK identifies the missing primary sequence and RTX restores it
    // through OSN. RepairedRtpStreamId correlates RID-based repair. Omitting the
    // RTX stream's own RID is an O-SFU profile choice.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3.3
    assert!(nack_contains(
        trace,
        s::RtcTraceDirection::Rx,
        dropped.ssrc,
        dropped.sequence_number,
    ));
    let rtx = s::require_some(
        matching_rtx(trace, s::RtcTraceDirection::Tx, dropped.sequence_number),
        "publisher should emit a matching RTX packet",
    )?;
    let primary = s::require_some(
        matching_primary(trace, s::RtcTraceDirection::Tx, dropped.sequence_number),
        "publisher trace should retain the dropped primary packet",
    )?;
    assert_eq!(primary.payload_type, dropped.payload_type);
    assert_eq!(primary.ssrc, dropped.ssrc);
    assert_eq!(primary.rid.as_deref(), expected_rid);
    assert_eq!(rtx.payload_type, repair_payload_type);
    assert_eq!(rtx.ssrc, repair_ssrc);
    assert_eq!(rtx.timestamp, dropped.timestamp);
    assert_eq!(rtx.marker, dropped.marker);
    assert_eq!(rtx.rid, None);
    assert_eq!(rtx.repaired_rid.as_deref(), expected_rid);
    assert!(rtx.payload.get(RTX_ORIGINAL_SEQUENCE_NUMBER_BYTES..) == Some(expected_payload));
    assert_eq!(
        primary.transport_sequence_number,
        dropped.transport_sequence_number
    );
    assert_fresh_transport_sequence(rtx, dropped);
    Ok(())
}

fn assert_subscriber_recovery(
    trace: &s::RtcPeerTrace,
    dropped: s::DroppedRtpPacket,
    repair_payload_type: u8,
    repair_ssrc: u32,
    recovered: &s::ReceivedRtpPacket,
) -> s::TestResult {
    // Generic NACK identifies the missing primary sequence and RTX restores it
    // through OSN. O-SFU's single RID-less downstream stream omits both stream
    // identifier extensions as a profile choice.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3.3
    assert!(nack_contains(
        trace,
        s::RtcTraceDirection::Tx,
        dropped.ssrc,
        dropped.sequence_number,
    ));
    let rtx = s::require_some(
        matching_rtx(trace, s::RtcTraceDirection::Rx, dropped.sequence_number),
        "subscriber should receive a matching RTX packet",
    )?;
    assert_eq!(rtx.payload_type, repair_payload_type);
    assert_eq!(rtx.ssrc, repair_ssrc);
    assert_eq!(rtx.timestamp, dropped.timestamp);
    assert_eq!(rtx.marker, dropped.marker);
    assert_eq!(rtx.rid, None);
    assert_eq!(rtx.repaired_rid, None);
    assert!(
        rtx.payload.get(RTX_ORIGINAL_SEQUENCE_NUMBER_BYTES..) == Some(recovered.payload.as_ref())
    );
    assert_fresh_transport_sequence(rtx, dropped);
    assert_eq!(recovered.payload_type, dropped.payload_type);
    assert_eq!(recovered.sequence_number, dropped.sequence_number);
    assert_eq!(recovered.timestamp, dropped.timestamp);
    assert_eq!(recovered.marker, dropped.marker);
    assert_eq!(recovered.ssrc, dropped.ssrc);
    assert_eq!(recovered.rid, None);
    Ok(())
}

fn assert_fresh_transport_sequence(rtx: &s::TracedRtpPacket, dropped: s::DroppedRtpPacket) {
    assert!(dropped.transport_sequence_number.is_some());
    assert!(rtx.transport_sequence_number.is_some());
    assert_ne!(
        rtx.transport_sequence_number,
        dropped.transport_sequence_number
    );
}

fn merge_trace(trace: &mut s::RtcPeerTrace, mut next: s::RtcPeerTrace) {
    trace.nacks.append(&mut next.nacks);
    trace.rtp_packets.append(&mut next.rtp_packets);
    trace
        .dropped_rtp_packets
        .append(&mut next.dropped_rtp_packets);
    trace.keyframe_requests += next.keyframe_requests;
}

fn nack_contains(
    trace: &s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    ssrc: u32,
    sequence_number: u16,
) -> bool {
    trace.nacks.iter().any(|nack| {
        nack.direction == direction
            && nack.ssrc == ssrc
            && nack.sequence_numbers.contains(&sequence_number)
    })
}

fn matching_rtx(
    trace: &s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    original_sequence_number: u16,
) -> Option<&s::TracedRtpPacket> {
    trace.rtp_packets.iter().find(|packet| {
        packet.direction == direction
            && packet.original_sequence_number == Some(original_sequence_number)
    })
}

fn matching_primary(
    trace: &s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    sequence_number: u16,
) -> Option<&s::TracedRtpPacket> {
    trace.rtp_packets.iter().find(|packet| {
        packet.direction == direction
            && packet.sequence_number == sequence_number
            && packet.original_sequence_number.is_none()
    })
}

fn matching_primary_payload<'a>(
    trace: &'a s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    payload: &[u8],
) -> Option<&'a s::TracedRtpPacket> {
    trace.rtp_packets.iter().find(|packet| {
        packet.direction == direction
            && packet.original_sequence_number.is_none()
            && packet.payload.as_ref() == payload
    })
}

async fn read_payload(
    peer: &mut s::ProtocolFakePeer,
    payload: &[u8],
    timeout_window: s::Duration,
) -> Option<s::ReceivedRtpPacket> {
    read_matching_packet(peer, timeout_window, |packet| {
        packet.payload.as_ref() == payload
    })
    .await
}

async fn read_sequence(
    peer: &mut s::ProtocolFakePeer,
    sequence_number: u16,
    timeout_window: s::Duration,
) -> Option<s::ReceivedRtpPacket> {
    read_matching_packet(peer, timeout_window, |packet| {
        packet.sequence_number == sequence_number
    })
    .await
}

async fn read_matching_packet(
    peer: &mut s::ProtocolFakePeer,
    timeout_window: s::Duration,
    predicate: impl Fn(&s::ReceivedRtpPacket) -> bool,
) -> Option<s::ReceivedRtpPacket> {
    let deadline = s::Instant::now() + timeout_window;
    loop {
        let now = s::Instant::now();
        if now >= deadline {
            return None;
        }
        let packet = peer.read_rtp_packet(deadline - now).await?;
        if predicate(&packet) {
            return Some(packet);
        }
    }
}

async fn read_until_drop(
    peer: &mut s::ProtocolFakePeer,
    trace: &mut s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    timeout_window: s::Duration,
) -> Option<s::ReceivedRtpPacket> {
    let deadline = s::Instant::now() + timeout_window;
    loop {
        let now = s::Instant::now();
        if now >= deadline {
            return None;
        }
        let packet = peer.read_rtp_packet(deadline - now).await?;
        merge_trace(trace, peer.take_rtc_trace());
        if trace
            .dropped_rtp_packets
            .iter()
            .any(|packet| packet.direction == direction)
        {
            return Some(packet);
        }
    }
}

async fn pump_until_drop(
    peer: &mut s::ProtocolFakePeer,
    trace: &mut s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    timeout_window: s::Duration,
) -> Option<()> {
    let deadline = s::Instant::now() + timeout_window;
    loop {
        if trace
            .dropped_rtp_packets
            .iter()
            .any(|packet| packet.direction == direction)
        {
            return Some(());
        }
        let now = s::Instant::now();
        if now >= deadline {
            return None;
        }
        peer.pump_rtc(RTC_POLL_SLICE.min(deadline - now)).await?;
        merge_trace(trace, peer.take_rtc_trace());
    }
}

async fn pump_until_rtx(
    peer: &mut s::ProtocolFakePeer,
    trace: &mut s::RtcPeerTrace,
    direction: s::RtcTraceDirection,
    original_sequence_number: u16,
    timeout_window: s::Duration,
) -> Option<()> {
    let deadline = s::Instant::now() + timeout_window;
    loop {
        if trace.rtp_packets.iter().any(|packet| {
            packet.direction == direction
                && packet.original_sequence_number == Some(original_sequence_number)
        }) {
            return Some(());
        }
        let now = s::Instant::now();
        if now >= deadline {
            return None;
        }
        peer.pump_rtc(RTC_POLL_SLICE.min(deadline - now)).await?;
        merge_trace(trace, peer.take_rtc_trace());
    }
}

async fn pump_until_held_outbound_rtp_count(
    peer: &mut s::ProtocolFakePeer,
    expected_count: usize,
    timeout_window: s::Duration,
) -> Option<()> {
    let deadline = s::Instant::now() + timeout_window;
    loop {
        if peer.held_outbound_rtp_count() >= expected_count {
            return Some(());
        }
        let now = s::Instant::now();
        if now >= deadline {
            return None;
        }
        peer.pump_rtc(s::Duration::from_millis(5).min(deadline - now))
            .await?;
    }
}
