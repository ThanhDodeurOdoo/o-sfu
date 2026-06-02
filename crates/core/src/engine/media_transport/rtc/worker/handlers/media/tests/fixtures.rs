#![allow(
    clippy::panic,
    reason = "media worker fixtures use panic only for mandatory setup failures"
)]

use std::{net::SocketAddr, sync::Arc, time::Instant};

use str0m::{
    media::{KeyframeRequestKind, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tokio::sync::{mpsc, oneshot};

use super::super::apply_route_control_request;
use crate::{
    Bitrate, MediaCodecFlags,
    engine::{
        UserId,
        media_transport::{
            TransportConsumerRoute, TransportMediaId, TransportResult, TransportSessionKey,
            TransportSourceKey,
            rtc::{
                bootstrap,
                commands::{
                    RemoteSourceControl, RouteControlRequest, RtcMediaControlCommand,
                    RtcWorkerCommand,
                },
                media_registry::RegisteredMediaHandle,
                relay_registry::RelayTargetId,
                route_control::PacketLayerGate,
                state::PacketLoopState,
                test_support::{
                    MediaWorkerScenario, collect_ready_session_keys, test_transport_session_key,
                },
            },
        },
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

const SOURCE_MID: &str = "cam-up";
const CONSUMER_MID: &str = "cam-down";

pub(super) fn drain_ready_sessions(state: &mut PacketLoopState) -> Vec<TransportSessionKey> {
    collect_ready_session_keys(state, Instant::now())
}

pub(super) fn expect_response<T>(
    response: oneshot::Receiver<TransportResult<T>>,
) -> TransportResult<T> {
    response.blocking_recv().unwrap_or_else(|error| {
        panic!("worker response channel should deliver a result: {error:?}")
    })
}

pub(super) fn prepare_source_session(
    state: &mut PacketLoopState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
) -> TransportMediaId {
    prepare_source_session_with_rid(state, source_session, source_mid, ssrc, None)
}

pub(super) fn prepare_source_session_with_rid(
    state: &mut PacketLoopState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
    rid: Option<Rid>,
) -> TransportMediaId {
    let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 47_000));
    assert!(
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            source_session,
            candidate_addr,
            Bitrate::from_mbps(10),
            MediaCodecFlags::default(),
        )
        .is_ok()
    );
    let Some(source_session_state) = state.users.get_mut(source_session) else {
        panic!("source session should exist after RTC state bootstrap");
    };
    let mut direct_api = source_session_state.rtc.direct_api();
    direct_api.declare_media(source_mid, MediaKind::Video);
    direct_api.expect_stream_rx(Ssrc::from(ssrc), None, source_mid, rid);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: source_session.clone(),
        mid: source_mid,
    })
}

pub(super) fn add_source_rid_stream(
    state: &mut PacketLoopState,
    source_session: &TransportSessionKey,
    source_mid: Mid,
    ssrc: u32,
    rid: Rid,
) {
    let Some(source_session_state) = state.users.get_mut(source_session) else {
        panic!("source session should exist before adding RID stream");
    };
    source_session_state.rtc.direct_api().expect_stream_rx(
        Ssrc::from(ssrc),
        None,
        source_mid,
        Some(rid),
    );
}

pub(super) fn assert_consumer_packet_gate(
    state: &PacketLoopState,
    source_transport_media_id: TransportMediaId,
    consumer_session: &TransportSessionKey,
    packet_gate: &PacketLayerGate,
    pending_packet_gate: Option<&PacketLayerGate>,
) {
    assert!(
        state
            .routes
            .local_route(source_transport_media_id)
            .is_some_and(
                |route_entry| route_entry.destinations.iter().any(|destination| {
                    destination.dest_session == *consumer_session
                        && &destination.packet_gate == packet_gate
                        && destination.pending_packet_gate.as_ref() == pending_packet_gate
                })
            )
    );
}

pub(super) fn install_video_route_with_gate(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    consumer_session: &TransportSessionKey,
    consumer_mid: Mid,
    packet_gate: PacketLayerGate,
) -> TransportMediaId {
    let mut scenario = MediaWorkerScenario::new(state);
    scenario.destination_with_gate(
        source_transport_media_id,
        consumer_session.clone(),
        consumer_mid,
        packet_gate,
    )
}

pub(super) fn request_consumer_keyframe(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    consumer_session: &TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
) {
    let (response_tx, response_rx) = oneshot::channel();
    let route = TransportConsumerRoute::new(
        consumer_session.clone(),
        consumer_transport_media_id,
        TransportSourceKey::new(source_session.clone(), source_transport_media_id),
    );
    apply_route_control_request(
        state,
        metrics,
        RouteControlRequest::RequestConsumerKeyframe { route },
        Instant::now(),
        Some(response_tx),
    );
    assert_eq!(expect_response(response_rx), Ok(()));
}

pub(super) fn register_remote_source(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    target_id: RelayTargetId,
) -> mpsc::Receiver<RtcWorkerCommand> {
    register_remote_source_with_metrics(
        state,
        source_transport_media_id,
        source_session,
        target_id,
        Arc::new(RtcMetricsRecorder::default()),
    )
}

pub(super) fn register_remote_source_with_metrics(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) -> mpsc::Receiver<RtcWorkerCommand> {
    let (control_tx, control_rx) = mpsc::channel(1);
    let source = TransportSourceKey::new(source_session.clone(), source_transport_media_id);
    assert!(
        state
            .routes
            .register_remote_source(
                &source,
                RemoteSourceControl::with_metrics(control_tx, target_id, rtc_metrics),
            )
            .is_ok()
    );
    control_rx
}

pub(super) fn register_saturated_remote_source(
    state: &mut PacketLoopState,
    source_transport_media_id: TransportMediaId,
    source_session: &TransportSessionKey,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) -> mpsc::Receiver<RtcWorkerCommand> {
    let (control_tx, control_rx) = mpsc::channel(1);
    let source = TransportSourceKey::new(source_session.clone(), source_transport_media_id);
    assert!(
        control_tx
            .try_send(RtcWorkerCommand::MediaControl(
                RtcMediaControlCommand::Apply {
                    request: RouteControlRequest::SetRemoteSourcePacketGate {
                        source: source.clone(),
                        target_id,
                        packet_gate: PacketLayerGate::Open,
                    },
                    response: None,
                },
            ))
            .is_ok()
    );
    assert!(
        state
            .routes
            .register_remote_source(
                &source,
                RemoteSourceControl::with_metrics(control_tx, target_id, rtc_metrics),
            )
            .is_ok()
    );
    control_rx
}

pub(super) fn assert_remote_keyframe_command(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    source_session: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    rid: Option<Rid>,
) {
    loop {
        match control_rx.try_recv().ok() {
            Some(RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
                request: RouteControlRequest::SetRemoteSourcePacketGate { .. },
                response: None,
            })) => {}
            command => {
                assert!(matches!(
                    command,
                    Some(RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
                        request: RouteControlRequest::RequestRemoteKeyframe {
                            source,
                            target_id: forwarded_target_id,
                            rid: forwarded_rid,
                            kind: KeyframeRequestKind::Pli,
                        },
                        response: None,
                    })) if source.session_key() == source_session
                        && source.transport_media_id() == source_transport_media_id
                        && forwarded_target_id == target_id
                        && forwarded_rid == rid
                ));
                return;
            }
        }
    }
}

pub(super) fn assert_remote_packet_gate_command(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    source_session: &TransportSessionKey,
    source_transport_media_id: TransportMediaId,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    assert!(matches!(
        control_rx.try_recv().ok(),
        Some(RtcWorkerCommand::MediaControl(RtcMediaControlCommand::Apply {
            request: RouteControlRequest::SetRemoteSourcePacketGate {
                source,
                target_id: forwarded_target_id,
                packet_gate: forwarded_packet_gate,
            },
            response: None,
        })) if source.session_key() == source_session
            && source.transport_media_id() == source_transport_media_id
            && forwarded_target_id == target_id
            && forwarded_packet_gate == packet_gate
    ));
}

pub(super) struct LocalVideoRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub source_transport_media_id: TransportMediaId,
    pub consumer_transport_media_id: TransportMediaId,
}

impl LocalVideoRoute {
    pub fn new(seed: u64, ssrc: u32) -> Self {
        Self::build(seed, ssrc, None, PacketLayerGate::Open, false)
    }

    pub fn with_rid(seed: u64, ssrc: u32, rid: Rid) -> Self {
        Self::build(seed, ssrc, Some(rid), PacketLayerGate::Open, false)
    }

    pub fn with_pending_rid_gate(seed: u64, ssrc: u32, rid: Rid) -> Self {
        Self::build(seed, ssrc, Some(rid), PacketLayerGate::Rid(rid), true)
    }

    pub fn with_rid_gate(seed: u64, ssrc: u32, rid: Rid, packet_gate: PacketLayerGate) -> Self {
        Self::build(seed, ssrc, Some(rid), packet_gate, false)
    }

    fn build(
        seed: u64,
        ssrc: u32,
        rid: Option<Rid>,
        packet_gate: PacketLayerGate,
        pending_gate: bool,
    ) -> Self {
        let source_session = source_session(seed);
        let consumer_session = consumer_session(seed);
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let source_transport_media_id = match rid {
            Some(rid) => prepare_source_session_with_rid(
                &mut state,
                &source_session,
                Mid::from(SOURCE_MID),
                ssrc,
                Some(rid),
            ),
            None => {
                prepare_source_session(&mut state, &source_session, Mid::from(SOURCE_MID), ssrc)
            }
        };
        let consumer_transport_media_id = if pending_gate {
            let mut scenario = MediaWorkerScenario::new(&mut state);
            scenario.destination_with_pending_gate(
                source_transport_media_id,
                consumer_session.clone(),
                Mid::from(CONSUMER_MID),
                packet_gate,
            )
        } else {
            install_video_route_with_gate(
                &mut state,
                source_transport_media_id,
                &consumer_session,
                Mid::from(CONSUMER_MID),
                packet_gate,
            )
        };
        Self {
            state,
            metrics,
            source_session,
            consumer_session,
            source_transport_media_id,
            consumer_transport_media_id,
        }
    }

    pub fn request_keyframe(&mut self) {
        request_consumer_keyframe(
            &mut self.state,
            &self.metrics,
            &self.consumer_session,
            self.consumer_transport_media_id,
            &self.source_session,
            self.source_transport_media_id,
        );
    }

    pub fn consumer_route(&self) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.consumer_session.clone(),
            self.consumer_transport_media_id,
            TransportSourceKey::new(self.source_session.clone(), self.source_transport_media_id),
        )
    }
}

pub(super) struct RemoteVideoRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub source_transport_media_id: TransportMediaId,
    pub consumer_transport_media_id: TransportMediaId,
    pub target_id: RelayTargetId,
    pub control_rx: mpsc::Receiver<RtcWorkerCommand>,
}

impl RemoteVideoRoute {
    pub fn new(seed: u64, source_transport_media_id: u64, target_id: u64) -> Self {
        Self::with_gate(
            seed,
            source_transport_media_id,
            target_id,
            PacketLayerGate::Open,
        )
    }

    pub fn with_gate(
        seed: u64,
        source_transport_media_id: u64,
        target_id: u64,
        packet_gate: PacketLayerGate,
    ) -> Self {
        let source_session = source_session(seed);
        let consumer_session = consumer_session_on_worker(seed, 1);
        let source_transport_media_id = TransportMediaId::new(source_transport_media_id);
        let target_id = RelayTargetId::new(target_id);
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let control_rx = register_remote_source(
            &mut state,
            source_transport_media_id,
            &source_session,
            target_id,
        );
        let consumer_transport_media_id = install_video_route_with_gate(
            &mut state,
            source_transport_media_id,
            &consumer_session,
            Mid::from(CONSUMER_MID),
            packet_gate,
        );
        Self {
            state,
            metrics,
            source_session,
            consumer_session,
            source_transport_media_id,
            consumer_transport_media_id,
            target_id,
            control_rx,
        }
    }

    pub fn request_keyframe(&mut self) {
        request_consumer_keyframe(
            &mut self.state,
            &self.metrics,
            &self.consumer_session,
            self.consumer_transport_media_id,
            &self.source_session,
            self.source_transport_media_id,
        );
    }
}

pub(super) struct PendingSelectedRidRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub source_transport_media_id: TransportMediaId,
    pub selected_rid: Rid,
    pub fallback_rid: Rid,
}

pub(super) fn prepare_pending_selected_rid_route() -> PendingSelectedRidRoute {
    let source_session = source_session(231);
    let consumer_session = consumer_session(231);
    let source_mid = Mid::from(SOURCE_MID);
    let consumer_mid = Mid::from(CONSUMER_MID);
    let selected_rid = Rid::from("hi");
    let fallback_rid = Rid::from("lo");
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let source_transport_media_id = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_301,
        Some(selected_rid),
    );
    add_source_rid_stream(
        &mut state,
        &source_session,
        source_mid,
        88_302,
        fallback_rid,
    );
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let consumer_transport_media_id = scenario.destination(
        source_transport_media_id,
        consumer_session.clone(),
        consumer_mid,
    );
    let route = TransportConsumerRoute::new(
        consumer_session.clone(),
        consumer_transport_media_id,
        TransportSourceKey::new(source_session.clone(), source_transport_media_id),
    );
    let command_now = Instant::now();
    let (response_tx, response_rx) = oneshot::channel();
    apply_route_control_request(
        &mut state,
        &metrics,
        RouteControlRequest::SetConsumerPacketGate {
            route,
            packet_gate: PacketLayerGate::Rid(selected_rid),
        },
        command_now,
        Some(response_tx),
    );
    assert_eq!(expect_response(response_rx), Ok(()));
    PendingSelectedRidRoute {
        state,
        metrics,
        source_session,
        consumer_session,
        source_transport_media_id,
        selected_rid,
        fallback_rid,
    }
}

fn source_session(seed: u64) -> TransportSessionKey {
    test_transport_session_key(seed, 0, seed + 1, test_user_id(seed + 2))
}

fn consumer_session(seed: u64) -> TransportSessionKey {
    consumer_session_on_worker(seed, 0)
}

fn consumer_session_on_worker(seed: u64, media_worker_id: usize) -> TransportSessionKey {
    test_transport_session_key(seed, media_worker_id, seed + 3, test_user_id(seed + 4))
}

fn test_user_id(value: u64) -> UserId {
    let Ok(value) = i64::try_from(value) else {
        panic!("test user id seed should fit in i64");
    };
    UserId::Integer(value)
}
