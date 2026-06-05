use std::net::SocketAddr;

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    CodecSetting, MediaFormat, MediaKind as RouterMediaKind, MediaStream as RouterRtpParameters,
    StreamBinding,
};
use str0m::media::{Mid, Rid};

use super::*;
use crate::{
    Bitrate, MediaCodecFlags,
    engine::{
        UserId,
        media_transport::rtc::{
            bootstrap::ensure_session_rtc_state,
            forwarded_packet::test_support::{
                sample_forwarded_packet, sample_forwarded_packet_with_frame_mark,
                sample_forwarded_packet_with_rid, sample_forwarded_packet_without_mid,
                sample_local_forwarded_packet,
            },
            media_registry::RegisteredMediaHandle,
            test_support::test_transport_session_key,
        },
    },
};

fn install_test_session(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Option<SessionHandle> {
    assert!(
        ensure_session_rtc_state(
            &mut state.users,
            session_key,
            SocketAddr::from(([127, 0, 0, 1], 9)),
            Bitrate::from_bps(1_000_000),
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    state.users.handle_for_key(session_key)
}

#[test]
fn forwarded_packet_resolves_transport_media_id_through_the_registry() {
    let session_key = test_transport_session_key(41, 0, 9, UserId::Integer(7));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn local_forwarded_packet_resolves_transport_media_id_through_session_handle() {
    let session_key = test_transport_session_key(51, 0, 19, UserId::Integer(17));
    let mut state = PacketLoopState::default();
    let session_handle = install_test_session(&mut state, &session_key);
    assert!(session_handle.is_some());
    let Some(session_handle) = session_handle else {
        return;
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_local_forwarded_packet(session_handle, "aud-up", b"payload");

    assert_eq!(packet.src_key(&state), Some(&session_key));
    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn stale_local_forwarded_packet_does_not_resolve_through_reused_slot() {
    let session_key = test_transport_session_key(52, 0, 20, UserId::Integer(18));
    let replacement_session_key = test_transport_session_key(52, 0, 21, UserId::Integer(19));
    let mut state = PacketLoopState::default();
    let stale_handle = install_test_session(&mut state, &session_key);
    assert!(stale_handle.is_some());
    let Some(stale_handle) = stale_handle else {
        return;
    };
    let _removed = state.users.remove(&session_key);
    let _replacement_handle = install_test_session(&mut state, &replacement_session_key);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: replacement_session_key,
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_local_forwarded_packet(stale_handle, "aud-up", b"payload");

    assert!(packet.src_key(&state).is_none());
    assert_eq!(packet.resolve_facts(&state), None);
    assert!(
        packet
            .share_for_relay(&state, TransportMediaId::new(99))
            .is_none()
    );
}

#[test]
fn forwarded_packet_facts_detect_h264_idr_for_decoder_refresh() {
    let session_key = test_transport_session_key(48, 0, 16, UserId::Integer(14));
    let producer_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let parameters = RouterRtpParameters::new(
        vec![MediaFormat::new(
            RouterMediaKind::Video,
            CodecName::H264,
            102,
            90_000,
        )],
        vec![],
        vec![],
    );
    state
        .routes
        .refresh_decoder_codec(transport_media_id, &parameters);
    let mut packet = sample_forwarded_packet(session_key, "cam-up", &[0x65, 0x88]);
    let facts = packet.resolve_facts(&state);
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };

    assert_eq!(facts.src_media, transport_media_id);
    assert!(facts.decoder_refresh);
}

#[test]
fn forwarded_packet_h264_refresh_detection_uses_packetization_mode() {
    let session_key = test_transport_session_key(48, 0, 16, UserId::Integer(14));
    let producer_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let stap_a_idr = &[0x78, 0x00, 0x02, 0x65, 0x88];
    let mode_0_parameters = RouterRtpParameters::new(
        vec![
            MediaFormat::new(RouterMediaKind::Video, CodecName::H264, 102, 90_000)
                .with_setting(CodecSetting::H264PacketizationMode(0)),
        ],
        vec![],
        vec![],
    );
    state
        .routes
        .refresh_decoder_codec(transport_media_id, &mode_0_parameters);
    let mut mode_0_packet = sample_forwarded_packet(session_key.clone(), "cam-up", stap_a_idr);
    let mode_0_facts = mode_0_packet.resolve_facts(&state);
    assert!(mode_0_facts.is_some());
    let Some(mode_0_facts) = mode_0_facts else {
        return;
    };
    assert!(!mode_0_facts.decoder_refresh);

    let mode_1_parameters = RouterRtpParameters::new(
        vec![
            MediaFormat::new(RouterMediaKind::Video, CodecName::H264, 102, 90_000)
                .with_setting(CodecSetting::H264PacketizationMode(1)),
        ],
        vec![],
        vec![],
    );
    state
        .routes
        .refresh_decoder_codec(transport_media_id, &mode_1_parameters);
    let mut mode_1_packet = sample_forwarded_packet(session_key, "cam-up", stap_a_idr);
    let mode_1_facts = mode_1_packet.resolve_facts(&state);
    assert!(mode_1_facts.is_some());
    let Some(mode_1_facts) = mode_1_facts else {
        return;
    };
    assert!(mode_1_facts.decoder_refresh);
}

#[test]
fn forwarded_packet_facts_detect_relayed_h264_idr_from_source_media_id() {
    let session_key = test_transport_session_key(49, 0, 17, UserId::Integer(15));
    let src_media = TransportMediaId::new(23);
    let mut state = PacketLoopState::default();
    state.routes.refresh_decoder_codec(
        src_media,
        &RouterRtpParameters::new(
            vec![MediaFormat::new(
                RouterMediaKind::Video,
                CodecName::H264,
                102,
                90_000,
            )],
            vec![],
            vec![],
        ),
    );
    let source_packet = sample_forwarded_packet(session_key, "cam-up", &[0x65, 0x88]);
    let relay_packet = source_packet.share_for_relay(&state, src_media);
    assert!(relay_packet.is_some());
    let Some(mut relay_packet) = relay_packet else {
        return;
    };
    let facts = relay_packet.resolve_facts(&state);
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };

    assert_eq!(facts.src_media, src_media);
    assert!(facts.decoder_refresh);
}

#[test]
fn forwarded_packet_exposes_recording_payload_and_received_at() {
    let session_key = test_transport_session_key(42, 0, 10, UserId::Integer(8));
    let state = PacketLoopState::default();
    let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");

    assert_eq!(packet.src_key(&state), Some(&session_key));
    assert_eq!(packet.payload(), b"payload");
    assert_eq!(packet.payload_len(), 7);
    assert!(packet.received_at() <= Instant::now());
}

#[test]
fn forwarded_packet_relay_clone_keeps_payload_and_explicit_source_media_id() {
    let session_key = test_transport_session_key(43, 0, 11, UserId::Integer(9));
    let state = PacketLoopState::default();
    let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");
    let relay_packet = packet.share_for_relay(&state, TransportMediaId::new(18));
    assert!(relay_packet.is_some());
    let Some(mut relay_packet) = relay_packet else {
        return;
    };

    assert_eq!(relay_packet.src_key(&state), Some(&session_key));
    assert_eq!(relay_packet.payload(), b"payload");
    assert_eq!(
        relay_packet.resolve_src_media(&PacketLoopState::default()),
        Some(TransportMediaId::new(18))
    );
}

#[test]
fn forwarded_packet_projects_rid_and_frame_marking_for_route_control() {
    let session_key = test_transport_session_key(46, 0, 14, UserId::Integer(12));
    let frame_mark = u32::from(frame_marking::TEMPORAL_LAYER_ID_MAX) << 24;
    let mut packet = sample_forwarded_packet_with_frame_mark(
        session_key,
        "cam-up",
        Some("hi"),
        frame_mark,
        b"payload",
    );

    assert_eq!(
        packet.resolve_route_control_layer_metadata(&PacketLoopState::default()),
        PacketLayerMetadata::new(
            Some(Rid::from("hi")),
            Some(frame_marking::TEMPORAL_LAYER_ID_MAX)
        )
    );
}

#[test]
fn forwarded_packet_facts_expose_vp8_payload_identity() {
    let session_key = test_transport_session_key(50, 0, 18, UserId::Integer(16));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("cam-up"),
    });
    let mut packet = sample_forwarded_packet_with_rid(
        session_key,
        "cam-up",
        Some("hi"),
        &[0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00],
    );
    let facts = packet.resolve_facts(&state);
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };
    let vp8_payload = facts.vp8_payload;
    assert!(vp8_payload.is_some());
    let Some(vp8_payload) = vp8_payload else {
        return;
    };

    assert_eq!(facts.src_media, transport_media_id);
    assert_eq!(
        facts.layer_metadata,
        PacketLayerMetadata::new(Some(Rid::from("hi")), None)
    );
    assert_eq!(vp8_payload.identity.picture_id, Some(2));
    assert_eq!(vp8_payload.identity.tl0_pic_idx, Some(9));
    assert_eq!(packet.local_vp8_payload(), Some(vp8_payload));
}

#[test]
fn forwarded_packet_recovers_rid_from_ssrc_binding_when_extension_is_absent() {
    let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 76_543_u32;
    let mut state = PacketLoopState::default();
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    state.refresh_producer_ssrcs(
        &session_key,
        producer_mid,
        &RouterRtpParameters::new(
            vec![],
            vec![],
            vec![StreamBinding::new().with_ssrc(producer_ssrc).with_rid("hi")],
        )
        .with_mid(producer_mid.to_string()),
    );
    let mut packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

    assert_eq!(
        packet.resolve_route_control_layer_metadata(&state),
        PacketLayerMetadata::new(Some(Rid::from("hi")), None)
    );
}

#[test]
fn forwarded_packet_relay_clone_preserves_resolved_rid_from_source_worker() {
    let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 76_543_u32;
    let src_media = TransportMediaId::new(22);
    let mut source_state = PacketLoopState::default();
    source_state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    source_state.refresh_producer_ssrcs(
        &session_key,
        producer_mid,
        &RouterRtpParameters::new(
            vec![],
            vec![],
            vec![StreamBinding::new().with_ssrc(producer_ssrc).with_rid("hi")],
        )
        .with_mid(producer_mid.to_string()),
    );
    let mut packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

    assert_eq!(
        packet.resolve_route_control_layer_metadata(&source_state),
        PacketLayerMetadata::new(Some(Rid::from("hi")), None)
    );
    let relay_packet = packet.share_for_relay(&source_state, src_media);
    assert!(relay_packet.is_some());
    let Some(mut relay_packet) = relay_packet else {
        return;
    };

    assert_eq!(
        relay_packet.resolve_route_control_layer_metadata(&PacketLoopState::default()),
        PacketLayerMetadata::new(Some(Rid::from("hi")), None)
    );
}

#[test]
fn forwarded_packet_falls_back_to_ssrc_when_mid_is_missing() {
    let session_key = test_transport_session_key(44, 0, 12, UserId::Integer(10));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 65_432_u32;
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    state.refresh_producer_ssrcs(
        &session_key,
        producer_mid,
        &RouterRtpParameters::new(
            vec![],
            vec![],
            vec![StreamBinding::new().with_ssrc(producer_ssrc)],
        )
        .with_mid(producer_mid.to_string()),
    );
    let mut packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn forwarded_packet_caches_the_resolved_src_media() {
    let session_key = test_transport_session_key(45, 0, 13, UserId::Integer(11));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
    state.remove_media_handle(transport_media_id);
    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn forwarded_packet_facts_cache_the_resolved_src_media() {
    let session_key = test_transport_session_key(51, 0, 19, UserId::Integer(17));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");
    let facts = packet.resolve_facts(&state);
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };

    assert_eq!(facts.src_media, transport_media_id);
    state.remove_media_handle(transport_media_id);
    assert_eq!(
        packet.resolve_facts(&state).map(|facts| facts.src_media),
        Some(transport_media_id)
    );
}
