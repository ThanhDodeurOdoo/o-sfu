use std::net::SocketAddr;

use str0m::media::{KeyframeRequestKind, MediaKind, Mid};
use str0m::rtp::Ssrc;
use tokio::sync::oneshot;

use super::{
    RemoteKeyframeRequest, respond_request_remote_keyframe, respond_set_source_packet_gate,
};
use crate::config::MediaCodecFlags;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::rtc_adapter::{
    bootstrap,
    media_registry::RegisteredMediaHandle,
    relay_registry::{RelayPacketMailbox, RelayRegistry, RelayTargetId},
    route_control::PacketLayerGate,
    state::RtcBootstrapState,
};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};
use crate::signaling::shared::SessionId;

fn prepare_source_session(
    state: &mut RtcBootstrapState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
) -> TransportMediaId {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.sessions,
            source_session,
            candidate_addr,
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.sessions.get_mut(source_session) else {
        return TransportMediaId::default();
    };
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(ssrc), None, source_mid, None);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    })
}

#[test]
fn remote_keyframe_requests_drop_when_the_relay_target_is_inactive() {
    let source_session = TransportSessionKey::new(101, 0, 102, SessionId::Integer(103));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let relay_registry = RelayRegistry::default();
    let (_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 66_666);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &relay_registry,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: RelayTargetId::new(7),
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );

    assert!(!state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 0);
    assert_eq!(snapshot.rtc_route_control_absorbed, 0);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 1);
}

#[test]
fn remote_keyframe_requests_forward_once_and_then_absorb_within_the_window() {
    let source_session = TransportSessionKey::new(111, 0, 112, SessionId::Integer(113));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let metrics = RuntimeMetrics::default();
    let relay_registry = RelayRegistry::default();
    let (mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 77_777);
    let relay_target_id = RelayTargetId::new(8);

    relay_registry.activate_source_target(
        source_transport_media_id,
        relay_target_id,
        mailbox.into(),
    );
    relay_registry.set_source_target_active(source_transport_media_id, relay_target_id, true);

    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &relay_registry,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    );
    respond_request_remote_keyframe(
        &mut state,
        &metrics,
        &relay_registry,
        &RemoteKeyframeRequest {
            source_session_key: &source_session,
            source_transport_media_id,
            target_id: relay_target_id,
            rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    );

    assert!(state.dirty_sessions.contains(&source_session));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded, 1);
    assert_eq!(snapshot.rtc_route_control_absorbed, 1);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops, 0);
}

#[test]
fn set_source_packet_gate_updates_the_effective_gate_for_a_local_source() {
    let source_session = TransportSessionKey::new(121, 0, 122, SessionId::Integer(123));
    let source_mid = Mid::from("cam-up");
    let mut state = RtcBootstrapState::default();
    let source_transport_media_id =
        prepare_source_session(&mut state, &source_session, source_mid, 88_888);

    let (response_tx, response_rx) = oneshot::channel();
    respond_set_source_packet_gate(
        &mut state,
        &source_session,
        source_transport_media_id,
        Some(PacketLayerGate::Rid("hi".into())),
        response_tx,
    );
    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        Some(PacketLayerGate::Rid("hi".into()))
    );

    let (response_tx, response_rx) = oneshot::channel();
    respond_set_source_packet_gate(
        &mut state,
        &source_session,
        source_transport_media_id,
        None,
        response_tx,
    );
    assert_eq!(response_rx.blocking_recv(), Ok(Ok(())));
    assert_eq!(
        state
            .route_control
            .effective_packet_gate(source_transport_media_id),
        None
    );
}
