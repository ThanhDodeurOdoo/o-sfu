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
    let Some(identity) = next_projected_rtp_identity(
        streams,
        stream_handle,
        source_ssrc,
        source_timestamp,
        vp8_payload,
    ) else {
        panic!("consumer stream handle should be live");
    };
    identity
}

#[test]
fn projected_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
    let source_seq: SeqNo = 131_072.into();
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

    assert_eq!(source_seq.roc(), 2);
    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
}

#[test]
fn projected_sequence_numbers_are_scoped_by_consumer_stream() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();
    let other_stream_handle = streams.allocate();

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
    assert_eq!(other.seq_no.roc(), 0);
}

#[test]
fn forgetting_transport_media_streams_drops_all_stream_state_for_that_consumer() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );
    let _second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );

    forget_transport_media_stream(&mut streams, stream_handle);
    let replacement_stream_handle = streams.allocate();

    let reset = projected(
        &mut streams,
        replacement_stream_handle,
        Ssrc::from(111),
        1234,
        Vp8PayloadIdentity::default(),
    );
    assert_eq!(first.seq_no.roc(), 0);
    assert_eq!(reset.seq_no.roc(), 0);
    assert_eq!(reset.seq_no.roc(), first.seq_no.roc());
}

#[test]
fn released_consumer_stream_handle_is_ignored() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    forget_transport_media_stream(&mut streams, stream_handle);

    assert!(
        next_projected_rtp_identity(
            &mut streams,
            stream_handle,
            Ssrc::from(111),
            1234,
            Vp8PayloadIdentity::default(),
        )
        .is_none()
    );
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
    assert!(high.source_switched);
    assert_eq!(high.previous_source_ssrc, Some(Ssrc::from(111)));
}

#[test]
fn projected_vp8_picture_ids_stay_continuous_across_simulcast_ssrc_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate();

    let low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        Vp8PayloadIdentity {
            picture_id: Some(32_760),
            tl0_pic_idx: Some(250),
        },
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        Vp8PayloadIdentity {
            picture_id: Some(12),
            tl0_pic_idx: Some(4),
        },
    );
    let high_next = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        Vp8PayloadIdentity {
            picture_id: Some(14),
            tl0_pic_idx: Some(5),
        },
    );

    assert_eq!(low.vp8_payload.picture_id, Some(32_760));
    assert_eq!(low.vp8_payload.tl0_pic_idx, Some(250));
    assert_eq!(high.vp8_payload.picture_id, Some(32_761));
    assert_eq!(high.vp8_payload.tl0_pic_idx, Some(251));
    assert_eq!(high_next.vp8_payload.picture_id, Some(32_763));
    assert_eq!(high_next.vp8_payload.tl0_pic_idx, Some(252));
}
