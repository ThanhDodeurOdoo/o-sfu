#![allow(
    clippy::panic,
    reason = "local send rewrite tests use panic only for mandatory fixture setup failures"
)]

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind,
    rtp::{MediaFormat, MediaStream, PayloadType},
};
use str0m::media::Pt;

use super::*;

const VP8_PAYLOAD_TYPE: u8 = 96;

fn projected(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    codec_packet: codec::Packet,
) -> ProjectedIdentity {
    let source_seq_no = streams
        .streams
        .get(stream_handle)
        .and_then(|stream| stream.rtp.next_source_seq(source_ssrc))
        .unwrap_or_default();
    projected_packet(
        streams,
        stream_handle,
        0,
        source_ssrc,
        source_seq_no,
        source_timestamp,
        codec_packet,
    )
}

fn projected_packet(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    delivery_generation: u64,
    source_ssrc: Ssrc,
    source_seq_no: SeqNo,
    source_timestamp: u32,
    codec_packet: codec::Packet,
) -> ProjectedIdentity {
    let Some(identity) = streams.project_identity(
        stream_handle,
        delivery_generation,
        source_ssrc,
        source_seq_no,
        source_timestamp,
        codec_packet.identity(),
    ) else {
        panic!("consumer stream handle should be live");
    };
    identity
}

fn allocate_at(streams: &mut ConsumerStreamStore, next_seq_no: u64) -> ConsumerStreamHandle {
    streams.streams.insert(ConsumerStream {
        rtp: RtpProjection::new(next_seq_no.into()),
        ..ConsumerStream::default()
    })
}

fn vp8_inspector() -> codec::PacketInspector {
    codec::PacketInspector::from_parameters(&MediaStream::new(
        vec![MediaFormat::new(
            MediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(VP8_PAYLOAD_TYPE),
            90_000,
        )],
        vec![],
        vec![],
    ))
}

fn vp8_packet(
    inspector: &codec::PacketInspector,
    picture_id: u16,
    tl0_pic_idx: u8,
) -> codec::Packet {
    let [picture_id_high, picture_id_low] = (picture_id & 0x7fff).to_be_bytes();
    inspector.inspect(
        Pt::from(VP8_PAYLOAD_TYPE),
        &[
            0x90,
            0xe0,
            0x80 | picture_id_high,
            picture_id_low,
            tl0_pic_idx,
            0,
            0,
        ],
        true,
    )
}

#[test]
fn projected_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
}

#[test]
fn reordered_packets_do_not_move_codec_high_water_before_source_switch() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();
    let source_ssrc = Ssrc::from(111);
    let switched_ssrc = Ssrc::from(222);
    let inspector = vp8_inspector();
    let first_packet = vp8_packet(&inspector, 10, 10);
    let after_loss_packet = vp8_packet(&inspector, 13, 13);
    let reordered_packet = vp8_packet(&inspector, 12, 12);
    let switched_packet = vp8_packet(&inspector, 1, 1);

    let first = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        10_u64.into(),
        10_000,
        first_packet,
    );
    let after_loss = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        13_u64.into(),
        13_000,
        after_loss_packet,
    );
    let reordered = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        12_u64.into(),
        12_000,
        reordered_packet,
    );
    let switched = projected_packet(
        &mut streams,
        stream_handle,
        0,
        switched_ssrc,
        1_u64.into(),
        20_000,
        switched_packet,
    );

    assert_eq!(*after_loss.seq_no - *first.seq_no, 3);
    assert_eq!(*reordered.seq_no - *first.seq_no, 2);
    assert!(after_loss.seq_no.is_next(switched.seq_no));
    assert_eq!(
        switched.transition,
        SourceTransition::Switched {
            previous_ssrc: source_ssrc
        }
    );

    let mut projection = codec::Projection::default();
    assert_eq!(
        first.codec,
        projection.project(first_packet.identity(), false)
    );
    assert_eq!(
        after_loss.codec,
        projection.project(after_loss_packet.identity(), false)
    );
    let mut reordered_projection = projection;
    assert_eq!(
        reordered.codec,
        reordered_projection.project(reordered_packet.identity(), false)
    );
    assert_eq!(
        switched.codec,
        codec::Projection::default().project(vp8_packet(&inspector, 14, 14).identity(), false)
    );
    assert!(switched_packet.rewrite(switched.codec).is_some());
}

#[test]
fn delivery_generation_compacts_only_intentional_gaps() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();
    let source_ssrc = Ssrc::from(111);
    let inspector = vp8_inspector();
    let before_pause_packet = vp8_packet(&inspector, 10, 10);
    let after_resume_packet = vp8_packet(&inspector, 1_000, 200);

    let before_pause = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        10_u64.into(),
        10_000,
        before_pause_packet,
    );
    let after_resume = projected_packet(
        &mut streams,
        stream_handle,
        1,
        source_ssrc,
        1_000_u64.into(),
        20_000,
        after_resume_packet,
    );

    assert!(before_pause.seq_no.is_next(after_resume.seq_no));
    let mut projection = codec::Projection::default();
    assert_eq!(
        before_pause.codec,
        projection.project(before_pause_packet.identity(), false)
    );
    assert_eq!(
        after_resume.codec,
        projection.project(after_resume_packet.identity(), true)
    );
    assert!(
        streams
            .project_identity(
                stream_handle,
                0,
                source_ssrc,
                11_u64.into(),
                11_000,
                vp8_packet(&inspector, 11, 11).identity(),
            )
            .is_none()
    );
}

#[test]
fn projected_sequence_numbers_are_scoped_by_consumer_stream() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = allocate_at(&mut streams, 10);
    let other_stream_handle = allocate_at(&mut streams, 10);

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let other = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
    assert_eq!(other.seq_no, first.seq_no);
}

#[test]
fn forgetting_transport_media_streams_drops_all_stream_state_for_that_consumer() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = allocate_at(&mut streams, 10);

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    streams.release(stream_handle);
    let replacement_stream_handle = allocate_at(&mut streams, 10);

    let reset = projected(
        &mut streams,
        replacement_stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    assert!(first.seq_no.is_next(second.seq_no));
    assert_eq!(reset.seq_no, first.seq_no);
}

#[test]
fn released_consumer_stream_handle_is_ignored_after_slot_reuse() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    streams.release(stream_handle);
    let replacement_handle = streams.allocate();

    let replacement = projected(
        &mut streams,
        replacement_handle,
        Ssrc::from(222),
        5678,
        codec::Packet::default(),
    );

    assert!(
        streams
            .project_identity(
                stream_handle,
                0,
                Ssrc::from(111),
                0_u64.into(),
                1234,
                codec::Packet::default().identity(),
            )
            .is_none()
    );
    let replacement_next = projected(
        &mut streams,
        replacement_handle,
        Ssrc::from(222),
        5678,
        codec::Packet::default(),
    );
    assert!(replacement.seq_no.is_next(replacement_next.seq_no));
}

#[test]
fn projected_timestamps_preserve_source_deltas_on_one_ssrc() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        10_000,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        13_000,
        codec::Packet::default(),
    );

    assert_eq!(first.rtp_timestamp, 10_000);
    assert_eq!(second.rtp_timestamp, 13_000);
    assert_eq!(first.transition, SourceTransition::Unchanged);
    assert_eq!(second.transition, SourceTransition::Unchanged);
}

#[test]
fn projected_timestamps_stay_monotonic_across_simulcast_ssrc_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        codec::Packet::default(),
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        codec::Packet::default(),
    );
    let high_next = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        codec::Packet::default(),
    );

    assert_eq!(low.rtp_timestamp, 90_000);
    assert_eq!(high.rtp_timestamp, 90_001);
    assert_eq!(high_next.rtp_timestamp, 93_001);
    assert_eq!(low.transition, SourceTransition::Unchanged);
    assert_eq!(
        high.transition,
        SourceTransition::Switched {
            previous_ssrc: Ssrc::from(111)
        }
    );
    assert_eq!(high_next.transition, SourceTransition::Unchanged);
}
