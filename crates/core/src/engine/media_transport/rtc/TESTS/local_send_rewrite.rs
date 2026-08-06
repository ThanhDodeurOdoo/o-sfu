#![allow(
    clippy::panic,
    reason = "local send rewrite tests use panic only for mandatory fixture setup failures"
)]

use super::*;

fn projected(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    delivery: DeliveryGenerations,
    source_ssrc: u32,
    source_seq_no: u64,
    source_timestamp: u32,
    vp8_payload: Vp8PayloadIdentity,
) -> ProjectedIdentity {
    let Some(identity) = streams.project_identity(
        stream_handle,
        delivery,
        source_ssrc.into(),
        source_seq_no.into(),
        source_timestamp,
        vp8_payload,
    ) else {
        panic!("packet should project through the active consumer stream");
    };
    identity
}

const fn delivery(epoch: u64, source_filter: SourceFilterGeneration) -> DeliveryGenerations {
    DeliveryGenerations::new(epoch, source_filter)
}

fn vp8(picture_id: u16, tl0_pic_idx: u8) -> Vp8PayloadIdentity {
    Vp8PayloadIdentity {
        picture_id: Some(picture_id),
        tl0_pic_idx: Some(tl0_pic_idx),
    }
}

#[test]
fn source_sequence_gaps_remain_visible_to_the_receiver() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let filter = SourceFilterGeneration::default();

    let first = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        10,
        1_000,
        vp8(10, 10),
    );
    let after_loss = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        12,
        3_000,
        vp8(12, 12),
    );
    let reordered = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        11,
        2_000,
        vp8(11, 11),
    );
    let next = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        13,
        4_000,
        vp8(13, 13),
    );

    assert_eq!(*after_loss.seq_no, *first.seq_no + 2);
    assert_eq!(*reordered.seq_no, *first.seq_no + 1);
    assert_eq!(*next.seq_no, *first.seq_no + 3);
    assert_eq!(after_loss.rtp_timestamp, 3_000);
    assert_eq!(reordered.rtp_timestamp, 2_000);
    assert_eq!(next.rtp_timestamp, 4_000);
}

#[test]
fn delivery_epoch_compacts_an_intentional_gap() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let filter = SourceFilterGeneration::default();

    let before_pause = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        10,
        10_000,
        vp8(10, 10),
    );
    let after_resume = projected(
        &mut streams,
        stream,
        delivery(1, filter),
        111,
        100,
        90_000,
        vp8(100, 100),
    );

    assert!(before_pause.seq_no.is_next(after_resume.seq_no));
    assert_eq!(after_resume.rtp_timestamp, before_pause.rtp_timestamp + 1);
    assert_eq!(after_resume.vp8_payload, vp8(11, 11));
    assert_eq!(after_resume.transition, SourceTransition::Unchanged);
}

#[test]
fn stale_delivery_epoch_cannot_reanchor_projection() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let filter = SourceFilterGeneration::default();

    let _ = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        10,
        1_000,
        vp8(10, 10),
    );
    let current = projected(
        &mut streams,
        stream,
        delivery(1, filter),
        111,
        20,
        2_000,
        vp8(20, 20),
    );
    assert!(
        streams
            .project_identity(
                stream,
                DeliveryGenerations::new(0, filter),
                111.into(),
                11_u64.into(),
                1_100,
                vp8(11, 11),
            )
            .is_none()
    );
    let next = projected(
        &mut streams,
        stream,
        delivery(1, filter),
        111,
        21,
        2_100,
        vp8(21, 21),
    );

    assert!(current.seq_no.is_next(next.seq_no));
    assert_eq!(next.rtp_timestamp, current.rtp_timestamp + 100);
}

#[test]
fn source_filter_generation_compacts_only_the_sequence_gap() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let initial_filter = SourceFilterGeneration::default();
    let reopened_filter = initial_filter.next();

    let before_filter = projected(
        &mut streams,
        stream,
        delivery(0, initial_filter),
        111,
        10,
        10_000,
        vp8(10, 10),
    );
    let after_filter = projected(
        &mut streams,
        stream,
        delivery(0, reopened_filter),
        111,
        100,
        90_000,
        vp8(100, 100),
    );

    assert!(before_filter.seq_no.is_next(after_filter.seq_no));
    assert_eq!(after_filter.rtp_timestamp, 90_000);
    assert_eq!(after_filter.vp8_payload, vp8(100, 100));
    assert!(
        streams
            .project_identity(
                stream,
                DeliveryGenerations::new(0, initial_filter),
                111.into(),
                101_u64.into(),
                90_100,
                vp8(101, 101),
            )
            .is_none()
    );
}

#[test]
fn source_switch_requires_a_new_delivery_epoch() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let filter = SourceFilterGeneration::default();

    let low = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        10,
        90_000,
        vp8(10, 10),
    );
    assert!(
        streams
            .project_identity(
                stream,
                DeliveryGenerations::new(0, filter),
                222.into(),
                20_u64.into(),
                1_000,
                vp8(20, 20),
            )
            .is_none()
    );
    let high = projected(
        &mut streams,
        stream,
        delivery(1, filter),
        222,
        20,
        1_000,
        vp8(20, 20),
    );

    assert!(low.seq_no.is_next(high.seq_no));
    assert_eq!(high.rtp_timestamp, low.rtp_timestamp + 1);
    assert_eq!(high.vp8_payload, vp8(11, 11));
    assert_eq!(
        high.transition,
        SourceTransition::Switched {
            previous_ssrc: 111.into()
        }
    );
}

#[test]
fn vp8_counters_wrap_in_their_wire_spaces() {
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate();
    let filter = SourceFilterGeneration::default();

    let first = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        10,
        1_000,
        vp8(LONG_PICTURE_ID_MODULUS - 1, u8::MAX),
    );
    let second = projected(
        &mut streams,
        stream,
        delivery(0, filter),
        111,
        11,
        2_000,
        vp8(0, 0),
    );

    assert_eq!(first.vp8_payload, vp8(LONG_PICTURE_ID_MODULUS - 1, u8::MAX));
    assert_eq!(second.vp8_payload, vp8(0, 0));
}

#[test]
fn released_consumer_stream_handle_is_rejected_after_slot_reuse() {
    let mut streams = ConsumerStreamStore::default();
    let stale = streams.allocate();
    streams.release(stale);
    let replacement = streams.allocate();
    let delivery = DeliveryGenerations::new(0, SourceFilterGeneration::default());

    assert!(
        streams
            .project_identity(
                stale,
                delivery,
                111.into(),
                10_u64.into(),
                1_000,
                Vp8PayloadIdentity::default(),
            )
            .is_none()
    );
    assert!(
        streams
            .project_identity(
                replacement,
                delivery,
                222.into(),
                20_u64.into(),
                2_000,
                Vp8PayloadIdentity::default(),
            )
            .is_some()
    );
}
