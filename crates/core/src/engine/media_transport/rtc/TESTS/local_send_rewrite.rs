#![allow(
    clippy::panic,
    reason = "local send rewrite tests use panic only for mandatory fixture setup failures"
)]

use super::*;

fn projected(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
) -> ProjectedIdentity {
    let Some(identity) =
        streams.project_identity(stream_handle, source_ssrc, source_timestamp, vp8_payload)
    else {
        panic!("consumer stream handle should be live");
    };
    identity
}

fn allocate_at(streams: &mut ConsumerStreamStore, next_seq_no: u64) -> ConsumerStreamHandle {
    streams.streams.insert(ConsumerStream {
        next_seq_no: next_seq_no.into(),
        ..ConsumerStream::default()
    })
}

fn vp8(picture_id: u16, tl0_pic_idx: u8) -> Vp8PayloadIdentity {
    Vp8PayloadIdentity {
        picture_id: Some(picture_id),
        tl0_pic_idx: Some(tl0_pic_idx),
    }
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
        Vp8PayloadIdentity::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );

    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
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
        Vp8PayloadIdentity::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );
    let other = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
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
        Vp8PayloadIdentity::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );

    streams.release(stream_handle);
    let replacement_stream_handle = allocate_at(&mut streams, 10);

    let reset = projected(
        &mut streams,
        replacement_stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
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
        Vp8PayloadIdentity::default(),
    );

    assert!(
        streams
            .project_identity(
                stream_handle,
                Ssrc::from(111),
                1234,
                Vp8PayloadIdentity::default(),
            )
            .is_none()
    );
    let replacement_next = projected(
        &mut streams,
        replacement_handle,
        Ssrc::from(222),
        5678,
        Vp8PayloadIdentity::default(),
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
        Vp8PayloadIdentity::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        13_000,
        Vp8PayloadIdentity::default(),
    );

    assert_eq!(first.rtp_timestamp, 10_000);
    assert_eq!(second.rtp_timestamp, 13_000);
    assert!(!first.source_switched);
    assert!(!second.source_switched);
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
        Vp8PayloadIdentity::default(),
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        Vp8PayloadIdentity::default(),
    );
    let high_next = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        Vp8PayloadIdentity::default(),
    );

    assert_eq!(low.rtp_timestamp, 90_000);
    assert_eq!(high.rtp_timestamp, 90_001);
    assert_eq!(high_next.rtp_timestamp, 93_001);
    assert!(!low.source_switched);
    assert_eq!(high.previous_source_ssrc, Some(Ssrc::from(111)));
    assert!(high.source_switched);
    assert!(!high_next.source_switched);
}

#[test]
fn projected_vp8_identifiers_wrap_across_source_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        vp8(32_767, 255),
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        vp8(12, 4),
    );
    let high_next = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        vp8(14, 6),
    );

    assert_eq!(low.vp8_payload.picture_id, Some(32_767));
    assert_eq!(low.vp8_payload.tl0_pic_idx, Some(255));
    assert_eq!(high.vp8_payload.picture_id, Some(0));
    assert_eq!(high.vp8_payload.tl0_pic_idx, Some(0));
    assert_eq!(high_next.vp8_payload.picture_id, Some(2));
    assert_eq!(high_next.vp8_payload.tl0_pic_idx, Some(2));
}

#[test]
fn missing_vp8_fields_preserve_last_projected_identity_across_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        vp8(100, 30),
    );
    let high_gap = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        Vp8PayloadIdentity::default(),
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        vp8(12, 4),
    );

    assert_eq!(low.vp8_payload.picture_id, Some(100));
    assert_eq!(low.vp8_payload.tl0_pic_idx, Some(30));
    assert_eq!(high_gap.vp8_payload, Vp8PayloadIdentity::default());
    assert_eq!(high.vp8_payload.picture_id, Some(101));
    assert_eq!(high.vp8_payload.tl0_pic_idx, Some(31));
}

#[test]
fn missing_vp8_fields_are_tracked_independently_across_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let _low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        vp8(100, 30),
    );
    let missing_picture_id = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        Vp8PayloadIdentity {
            picture_id: None,
            tl0_pic_idx: Some(4),
        },
    );
    let picture_id_resumed = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        vp8(12, 5),
    );

    assert_eq!(missing_picture_id.vp8_payload.picture_id, None);
    assert_eq!(missing_picture_id.vp8_payload.tl0_pic_idx, Some(31));
    assert_eq!(picture_id_resumed.vp8_payload.picture_id, Some(101));
    assert_eq!(picture_id_resumed.vp8_payload.tl0_pic_idx, Some(32));

    let other_stream_handle = streams.allocate();
    let _low = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(333),
        90_000,
        vp8(200, 40),
    );
    let missing_tl0 = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(444),
        1_000,
        Vp8PayloadIdentity {
            picture_id: Some(12),
            tl0_pic_idx: None,
        },
    );
    let tl0_resumed = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(444),
        4_000,
        vp8(13, 4),
    );

    assert_eq!(missing_tl0.vp8_payload.picture_id, Some(201));
    assert_eq!(missing_tl0.vp8_payload.tl0_pic_idx, None);
    assert_eq!(tl0_resumed.vp8_payload.picture_id, Some(202));
    assert_eq!(tl0_resumed.vp8_payload.tl0_pic_idx, Some(41));
}
