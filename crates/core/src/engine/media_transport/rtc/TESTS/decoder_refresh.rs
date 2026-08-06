use std::time::{Duration, Instant};

use o_sfu_rfc::rtp::h264::{self, PacketizationMode};
use str0m::{
    media::{Pt, Rid},
    rtp::{SeqNo, Ssrc},
};

use super::super::{
    decoder_refresh::{
        DecoderRefreshAdmission, DecoderRefreshAdmissionInput, DecoderRefreshEvidence,
        DecoderRefreshNeed, DecoderRefreshPacket, DecoderRefreshRelease,
        MAX_PENDING_REFRESH_FRAMES, MAX_PENDING_REFRESH_FRAMES_PER_SOURCE, PendingDecoderRefreshes,
        Vp8RefreshFragment,
    },
    source_route::PacketCodec,
    test_support::{sample_forwarded_packet, test_transport_session_key},
};
use crate::engine::{RoomInstanceId, UserId, media_transport::TransportMediaId};

const VP8_KEYFRAME_PREFIX: [u8; 10] = [0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01];

struct Admission {
    input: DecoderRefreshAdmissionInput,
    packet: super::super::ForwardedPacket,
}

fn h264_admission(
    source_id: TransportMediaId,
    header: TestRtpHeader,
    payload: &[u8],
    observed_at: Instant,
) -> Admission {
    let mode = PacketizationMode::NonInterleaved;
    let mut admission = admission(source_id, None, header, payload, observed_at);
    admission.input.packet.payload_type = Pt::from(112);
    admission.input.packet.codec = Some(PacketCodec::H264(mode));
    admission.input.packet.evidence = Some(DecoderRefreshEvidence::H264 {
        starts_idr: h264::payload_starts_idr(payload, mode),
    });
    admission
}

#[derive(Clone, Copy)]
struct TestRtpHeader {
    ssrc: u32,
    sequence_number: u64,
    timestamp: u32,
    marker: bool,
}

const fn header(ssrc: u32, sequence_number: u64, timestamp: u32, marker: bool) -> TestRtpHeader {
    TestRtpHeader {
        ssrc,
        sequence_number,
        timestamp,
        marker,
    }
}

fn admission(
    source_id: TransportMediaId,
    rid: Option<Rid>,
    header: TestRtpHeader,
    payload: &[u8],
    observed_at: Instant,
) -> Admission {
    admission_with_need(
        source_id,
        rid,
        header,
        payload,
        DecoderRefreshNeed::PendingDestination,
        observed_at,
    )
}

fn admission_with_need(
    source_id: TransportMediaId,
    rid: Option<Rid>,
    header: TestRtpHeader,
    payload: &[u8],
    need: DecoderRefreshNeed,
    observed_at: Instant,
) -> Admission {
    let session_key = test_transport_session_key(1, 0, 1, UserId::Integer(1));
    Admission {
        input: DecoderRefreshAdmissionInput {
            room_instance_id: RoomInstanceId::from_raw(1),
            source_id,
            rid,
            need,
            packet: DecoderRefreshPacket {
                sequence_number: SeqNo::from(header.sequence_number),
                timestamp: header.timestamp,
                marker: header.marker,
                ssrc: Ssrc::from(header.ssrc),
                payload_type: Pt::from(111),
                codec: Some(PacketCodec::Vp8),
                evidence: Vp8RefreshFragment::parse(payload).map(DecoderRefreshEvidence::Vp8),
            },
            observed_at,
        },
        packet: sample_forwarded_packet(session_key, "cam-up", payload),
    }
}

#[test]
fn fragmented_keyframe_without_refresh_demand_passes_intact() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(1);
    let mut pending = PendingDecoderRefreshes::default();
    let mut ready = Vec::new();
    let first = [0x10, 0x30, 0x00, 0x00, 0x9d];
    let second = [0x00, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01];

    for admission in [
        admission_with_need(
            source_id,
            None,
            header(1, 10, 7, false),
            &first,
            DecoderRefreshNeed::None,
            now,
        ),
        admission_with_need(
            source_id,
            None,
            header(1, 11, 7, true),
            &second,
            DecoderRefreshNeed::None,
            now,
        ),
    ] {
        assert!(matches!(
            admit(&mut pending, admission, &mut ready),
            DecoderRefreshAdmission::Ready { .. }
        ));
    }

    assert_eq!(ready.len(), 2);
    assert_eq!(pending.lane_count(), 0);
    assert!(!pending.has_released_packets());
}

fn admit(
    pending: &mut PendingDecoderRefreshes,
    admission: Admission,
    ready: &mut Vec<super::super::ForwardedPacket>,
) -> DecoderRefreshAdmission {
    pending.admit(admission.input, admission.packet, ready)
}

fn stage_frame(
    pending: &mut PendingDecoderRefreshes,
    source_id: TransportMediaId,
    ssrc: u32,
    observed_at: Instant,
    packet_count: usize,
) {
    stage_frame_in_room(
        pending,
        RoomInstanceId::from_raw(1),
        source_id,
        ssrc,
        observed_at,
        packet_count,
    );
}

fn stage_frame_in_room(
    pending: &mut PendingDecoderRefreshes,
    room_instance_id: RoomInstanceId,
    source_id: TransportMediaId,
    ssrc: u32,
    observed_at: Instant,
    packet_count: usize,
) {
    let mut first_payload = vec![0x10];
    first_payload.extend_from_slice(&VP8_KEYFRAME_PREFIX);
    let mut ready = Vec::new();
    for index in 0..packet_count {
        let payload = if index == 0 {
            first_payload.as_slice()
        } else {
            &[0x00, 0xff]
        };
        assert!(matches!(
            {
                let mut admission = admission(
                    source_id,
                    None,
                    header(
                        ssrc,
                        u64::try_from(index).expect("test packet index should fit"),
                        1,
                        index + 1 == packet_count,
                    ),
                    payload,
                    observed_at,
                );
                admission.input.room_instance_id = room_instance_id;
                admit(pending, admission, &mut ready)
            },
            DecoderRefreshAdmission::Held
        ));
    }
    assert!(ready.is_empty());
}

#[test]
fn fragmented_keyframe_releases_only_after_marker_and_complete_prefix() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(1);
    let mut pending = PendingDecoderRefreshes::default();
    let mut ready = Vec::new();
    let first = [0x10, 0x30, 0x00, 0x00, 0x9d];
    let second = [0x00, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01];

    assert!(matches!(
        admit(
            &mut pending,
            admission(source_id, None, header(1, 10, 7, false), &first, now),
            &mut ready,
        ),
        DecoderRefreshAdmission::Held
    ));
    assert_eq!(pending.lane_count(), 1);
    assert!(!pending.has_released_packets());
    assert!(matches!(
        admit(
            &mut pending,
            admission(source_id, None, header(1, 11, 7, true), &second, now),
            &mut ready,
        ),
        DecoderRefreshAdmission::Held
    ));
    assert!(ready.is_empty());
    assert_eq!(pending.lane_count(), 0);
    assert!(pending.has_released_packets());

    let mut releases = Vec::new();
    assert_eq!(pending.drain_released(&mut ready, 64, &mut releases), 2);
    assert_eq!(ready.len(), 2);
    assert_eq!(releases.len(), 1);
    assert!(releases[0].activation.is_some());
    assert_eq!(pending.retained_packet_count(), 0);
    assert_eq!(pending.buffered_bytes(), 0);
}

#[test]
fn fragmented_h264_idr_releases_only_after_the_marker() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(10);
    let mut pending = PendingDecoderRefreshes::default();
    let mut ready = Vec::new();

    assert!(matches!(
        admit(
            &mut pending,
            h264_admission(
                source_id,
                header(10, 20, 30, false),
                &[0x7c, 0x85, 0xaa],
                now,
            ),
            &mut ready,
        ),
        DecoderRefreshAdmission::Held
    ));
    assert!(!pending.has_released_packets());
    assert!(matches!(
        admit(
            &mut pending,
            h264_admission(
                source_id,
                header(10, 21, 30, true),
                &[0x7c, 0x05, 0xbb],
                now,
            ),
            &mut ready,
        ),
        DecoderRefreshAdmission::Held
    ));

    let mut releases = Vec::new();
    assert_eq!(pending.drain_released(&mut ready, 64, &mut releases), 2);
    assert_eq!(ready.len(), 2);
    assert_eq!(releases.len(), 1);
    assert!(releases[0].activation.is_some());
}

#[test]
fn sequence_gap_discards_the_incomplete_candidate() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(2);
    let mut pending = PendingDecoderRefreshes::default();
    let mut ready = Vec::new();
    let first = [0x10, 0x30, 0x00, 0x00, 0x9d];
    let second = [0x00, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01];

    let _ = admit(
        &mut pending,
        admission(source_id, None, header(2, 10, 7, false), &first, now),
        &mut ready,
    );
    let _ = admit(
        &mut pending,
        admission(source_id, None, header(2, 12, 7, true), &second, now),
        &mut ready,
    );

    assert!(ready.is_empty());
    assert!(!pending.has_released_packets());
    assert_eq!(pending.lane_count(), 0);
    assert_eq!(pending.retained_packet_count(), 0);
}

#[test]
fn pending_frame_limits_fail_open_without_retaining_more_packets() {
    let now = Instant::now();
    let mut per_source = PendingDecoderRefreshes::default();
    let source_id = TransportMediaId::new(3);
    let mut ready = Vec::new();
    for (index, rid) in ["a", "b", "c", "d"].into_iter().enumerate() {
        let _ = admit(
            &mut per_source,
            admission(
                source_id,
                Some(Rid::from(rid)),
                header(
                    u32::try_from(index + 1).expect("test index should fit"),
                    1,
                    u32::try_from(index).expect("test index should fit"),
                    false,
                ),
                &[0x10, 0x30],
                now,
            ),
            &mut ready,
        );
    }
    assert_eq!(
        per_source.lane_count(),
        MAX_PENDING_REFRESH_FRAMES_PER_SOURCE
    );
    let overflow = admit(
        &mut per_source,
        admission(
            source_id,
            Some(Rid::from("e")),
            header(5, 1, 5, false),
            &[0x10, 0x30],
            now,
        ),
        &mut ready,
    );
    assert!(matches!(overflow, DecoderRefreshAdmission::Ready { .. }));
    assert_eq!(ready.len(), 1);

    let mut global = PendingDecoderRefreshes::default();
    let mut global_ready = Vec::new();
    for index in 0..MAX_PENDING_REFRESH_FRAMES {
        let source_id = TransportMediaId::new(
            u64::try_from(index + 100).expect("test source index should fit"),
        );
        let _ = admit(
            &mut global,
            admission(
                source_id,
                None,
                header(
                    u32::try_from(index + 100).expect("test source index should fit"),
                    1,
                    u32::try_from(index).expect("test frame index should fit"),
                    false,
                ),
                &[0x10, 0x30],
                now,
            ),
            &mut global_ready,
        );
    }
    let overflow = admit(
        &mut global,
        admission(
            TransportMediaId::new(999),
            None,
            header(999, 1, 999, false),
            &[0x10, 0x30],
            now,
        ),
        &mut global_ready,
    );
    assert!(matches!(overflow, DecoderRefreshAdmission::Ready { .. }));
    assert_eq!(global.lane_count(), MAX_PENDING_REFRESH_FRAMES);
    assert_eq!(global_ready.len(), 1);
}

#[test]
fn expired_candidate_releases_its_packet_and_byte_budget() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(4);
    let mut pending = PendingDecoderRefreshes::default();
    let mut ready = Vec::new();
    let payload = [0x10, 0x30];

    let _ = admit(
        &mut pending,
        admission(source_id, None, header(4, 1, 1, false), &payload, now),
        &mut ready,
    );
    assert_eq!(pending.retained_packet_count(), 1);
    assert_eq!(pending.buffered_bytes(), payload.len());

    pending.expire(now + Duration::from_secs(2));

    assert_eq!(pending.retained_packet_count(), 0);
    assert_eq!(pending.buffered_bytes(), 0);
    assert_eq!(pending.lane_count(), 1);
    pending.expire(now + Duration::from_secs(4));
    assert_eq!(pending.lane_count(), 0);
}

#[test]
fn released_frames_drain_round_robin_with_a_per_batch_quantum() {
    let now = Instant::now();
    let first_source = TransportMediaId::new(5);
    let second_source = TransportMediaId::new(6);
    let mut pending = PendingDecoderRefreshes::default();
    stage_frame(&mut pending, first_source, 5, now, 10);
    stage_frame(&mut pending, second_source, 6, now, 10);

    let mut ready = Vec::new();
    let mut releases = Vec::<DecoderRefreshRelease>::new();
    assert_eq!(pending.drain_released(&mut ready, 16, &mut releases), 16);
    assert_eq!(
        releases
            .iter()
            .map(|release| (release.source_id, release.packet_range.len()))
            .collect::<Vec<_>>(),
        vec![(first_source, 8), (second_source, 8)]
    );

    ready.clear();
    releases.clear();
    assert_eq!(pending.drain_released(&mut ready, 16, &mut releases), 4);
    assert_eq!(
        releases
            .iter()
            .map(|release| (release.source_id, release.packet_range.len()))
            .collect::<Vec<_>>(),
        vec![(first_source, 2), (second_source, 2)]
    );
    assert!(!pending.has_released_packets());
}

#[test]
fn released_frames_alternate_rooms_before_same_room_batches() {
    let now = Instant::now();
    let first_room_source = TransportMediaId::new(50);
    let same_room_source = TransportMediaId::new(51);
    let second_room_source = TransportMediaId::new(52);
    let mut pending = PendingDecoderRefreshes::default();
    stage_frame_in_room(
        &mut pending,
        RoomInstanceId::from_raw(1),
        first_room_source,
        50,
        now,
        10,
    );
    stage_frame_in_room(
        &mut pending,
        RoomInstanceId::from_raw(1),
        same_room_source,
        51,
        now,
        10,
    );
    stage_frame_in_room(
        &mut pending,
        RoomInstanceId::from_raw(2),
        second_room_source,
        52,
        now,
        10,
    );

    let mut ready = Vec::new();
    let mut releases = Vec::new();
    assert_eq!(pending.drain_released(&mut ready, 16, &mut releases), 16);
    assert_eq!(
        releases
            .iter()
            .map(|release| (release.source_id, release.packet_range.len()))
            .collect::<Vec<_>>(),
        vec![(first_room_source, 8), (second_room_source, 8)]
    );
}

#[test]
fn newer_rooms_cannot_starve_an_older_partial_release() {
    let now = Instant::now();
    let old_source = TransportMediaId::new(60);
    let mut pending = PendingDecoderRefreshes::default();
    stage_frame_in_room(
        &mut pending,
        RoomInstanceId::from_raw(1),
        old_source,
        60,
        now,
        10,
    );

    let mut ready = Vec::new();
    let mut releases = Vec::new();
    assert_eq!(pending.drain_released(&mut ready, 8, &mut releases), 8);
    for room in 2_u64..=9 {
        stage_frame_in_room(
            &mut pending,
            RoomInstanceId::from_raw(room),
            TransportMediaId::new(60 + room),
            u32::try_from(60 + room).expect("test SSRC should fit"),
            now,
            8,
        );
    }

    ready.clear();
    releases.clear();
    assert_eq!(pending.drain_released(&mut ready, 64, &mut releases), 64);
    assert!(
        releases
            .iter()
            .any(|release| { release.source_id == old_source && release.packet_range.len() == 2 })
    );
}

#[test]
fn later_stream_packets_queue_behind_a_released_refresh() {
    let now = Instant::now();
    let source_id = TransportMediaId::new(7);
    let mut pending = PendingDecoderRefreshes::default();
    stage_frame(&mut pending, source_id, 7, now, 2);
    let later = admission(source_id, None, header(7, 3, 2, false), &[0x00, 0xff], now);

    assert!(pending.has_matching_release(
        later.input.room_instance_id,
        source_id,
        None,
        later.input.packet,
    ));
    assert!(pending.defer_behind_release(
        later.input.room_instance_id,
        source_id,
        None,
        later.input.packet,
        later.packet,
    ));

    let mut ready = Vec::new();
    let mut releases = Vec::new();
    assert_eq!(pending.drain_released(&mut ready, 64, &mut releases), 3);
    assert_eq!(ready.len(), 3);
    assert_eq!(releases.len(), 1);
    assert!(releases[0].activation.is_some());
}
