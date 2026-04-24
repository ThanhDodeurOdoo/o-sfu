//! Test-only worker debug commands.
//!
//! These helpers expose inspection and state hooks that adapter

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::media::Mid;
use tokio::sync::oneshot;

use super::super::{
    bitrate::RtcBitrateState,
    commands::debug::{
        DebugPacketGate, DebugRouteDestination, DebugRouteEntry, DebugRtcWorkerCommand,
    },
    state::{RtcBootstrapState, RtcSnapshotState},
};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

pub(super) fn handle_debug_command(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    command: DebugRtcWorkerCommand,
) {
    match command {
        DebugRtcWorkerCommand::ResolveMid {
            transport_media_id,
            response,
        } => respond_debug_resolve_mid(state, transport_media_id, response),
        DebugRtcWorkerCommand::RemoteAddrOwner {
            source_addr,
            response,
        } => respond_debug_remote_addr_owner(snapshot_state, source_addr, response),
        DebugRtcWorkerCommand::HasAnyRemoteAddrSession { response } => {
            respond_debug_has_any_remote_addr_session(snapshot_state, response);
        }
        DebugRtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => respond_debug_remember_remote_addr(
            state,
            snapshot_state,
            source_addr,
            &session_key,
            response,
        ),
        DebugRtcWorkerCommand::SessionStreamRxSsrc {
            session_key,
            mid,
            response,
        } => respond_debug_session_stream_rx_ssrc(state, &session_key, mid, response),
        DebugRtcWorkerCommand::SessionStreamTxSsrc {
            session_key,
            mid,
            response,
        } => respond_debug_session_stream_tx_ssrc(state, &session_key, mid, response),
        DebugRtcWorkerCommand::SessionMaxBitrateIn {
            session_key,
            response,
        } => respond_debug_session_max_bitrate_in(state, &session_key, response),
        DebugRtcWorkerCommand::SessionMaxBitrateOut {
            session_key,
            response,
        } => respond_debug_session_max_bitrate_out(state, &session_key, response),
        DebugRtcWorkerCommand::RemoteSourceOwner {
            source_transport_media_id,
            response,
        } => respond_debug_remote_source_owner(state, source_transport_media_id, response),
        DebugRtcWorkerCommand::RouteEntry {
            source_session_key,
            source_mid,
            response,
        } => respond_debug_route_entry(state, &source_session_key, source_mid, response),
        DebugRtcWorkerCommand::RouteEntryByConsumerMid {
            consumer_session_key,
            consumer_mid,
            response,
        } => respond_debug_route_entry_by_consumer_mid(
            state,
            &consumer_session_key,
            consumer_mid,
            response,
        ),
        DebugRtcWorkerCommand::RouteEntryByMediaId {
            source_transport_media_id,
            response,
        } => respond_debug_route_entry_by_media_id(state, source_transport_media_id, response),
        DebugRtcWorkerCommand::RecordIncomingMedia {
            session_key,
            transport_media_id,
            payload_bytes,
            now,
            response,
        } => respond_debug_record_incoming_media(
            state,
            bitrate_state,
            &session_key,
            transport_media_id,
            payload_bytes,
            now,
            response,
        ),
        DebugRtcWorkerCommand::ObserveAudioActivity {
            transport_media_id,
            voice_activity,
            audio_level_dbov,
            now,
            response,
        } => respond_debug_observe_audio_activity(
            state,
            transport_media_id,
            voice_activity,
            audio_level_dbov,
            now,
            response,
        ),
    }
}

fn respond_debug_resolve_mid(
    state: &RtcBootstrapState,
    transport_media_id: TransportMediaId,
    response: oneshot::Sender<Option<Mid>>,
) {
    let _ = response.send(state.resolve_mid(transport_media_id));
}

fn respond_debug_remote_addr_owner(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    response: oneshot::Sender<Option<TransportSessionKey>>,
) {
    let value = snapshot_state.lock().ok().and_then(|snapshot| {
        snapshot
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr)
            .cloned()
    });
    let _ = response.send(value);
}

fn respond_debug_has_any_remote_addr_session(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    response: oneshot::Sender<bool>,
) {
    let value = snapshot_state
        .lock()
        .ok()
        .is_some_and(|snapshot| !snapshot.remote_addr_demux.is_empty());
    let _ = response.send(value);
}

fn respond_debug_remember_remote_addr(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_addr: SocketAddr,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<()>,
) {
    if state
        .remote_addr_demux
        .remember_remote_addr(source_addr, session_key)
        && let Ok(mut snapshot) = snapshot_state.lock()
    {
        let _ = snapshot
            .remote_addr_demux
            .remember_remote_addr(source_addr, session_key);
    }
    let _ = response.send(());
}

fn respond_debug_session_stream_rx_ssrc(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    mid: Mid,
    response: oneshot::Sender<Option<u32>>,
) {
    let value = state
        .sessions
        .get_mut(session_key)
        .and_then(|session_state| {
            let mut direct_api = session_state.rtc.direct_api();
            direct_api
                .stream_rx_by_mid(mid, None)
                .map(|stream_rx| *stream_rx.ssrc())
        });
    let _ = response.send(value);
}

fn respond_debug_session_stream_tx_ssrc(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    mid: Mid,
    response: oneshot::Sender<Option<u32>>,
) {
    let value = state
        .sessions
        .get_mut(session_key)
        .and_then(|session_state| {
            let mut direct_api = session_state.rtc.direct_api();
            direct_api
                .stream_tx_by_mid(mid, None)
                .map(|stream_tx| *stream_tx.ssrc())
        });
    let _ = response.send(value);
}

fn respond_debug_session_max_bitrate_in(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Option<u64>>,
) {
    let value = state
        .sessions
        .get(session_key)
        .and_then(|session_state| session_state.max_bitrate_in_bps);
    let _ = response.send(value);
}

fn respond_debug_session_max_bitrate_out(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    response: oneshot::Sender<Option<u64>>,
) {
    let value = state
        .sessions
        .get(session_key)
        .and_then(|session_state| session_state.max_bitrate_out_bps);
    let _ = response.send(value);
}

fn respond_debug_remote_source_owner(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    response: oneshot::Sender<Option<TransportSessionKey>>,
) {
    let value = state
        .remote_source_registration(source_transport_media_id)
        .map(|registration| registration.source_session_key().clone());
    let _ = response.send(value);
}

fn respond_debug_route_entry(
    state: &RtcBootstrapState,
    source_session_key: &TransportSessionKey,
    source_mid: Mid,
    response: oneshot::Sender<Option<DebugRouteEntry>>,
) {
    let value = state
        .source_transport_media_id_for_mid(source_session_key, source_mid)
        .and_then(|source_transport_media_id| {
            build_debug_route_entry(state, source_transport_media_id)
        });
    let _ = response.send(value);
}

fn respond_debug_route_entry_by_consumer_mid(
    state: &RtcBootstrapState,
    consumer_session_key: &TransportSessionKey,
    consumer_mid: Mid,
    response: oneshot::Sender<Option<DebugRouteEntry>>,
) {
    let value = state
        .consumer_source_transport_media_id_for_mid(consumer_session_key, consumer_mid)
        .and_then(|source_transport_media_id| {
            build_debug_route_entry(state, source_transport_media_id)
        });
    let _ = response.send(value);
}

fn respond_debug_route_entry_by_media_id(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
    response: oneshot::Sender<Option<DebugRouteEntry>>,
) {
    let _ = response.send(build_debug_route_entry(state, source_transport_media_id));
}

fn build_debug_route_entry(
    state: &RtcBootstrapState,
    source_transport_media_id: TransportMediaId,
) -> Option<DebugRouteEntry> {
    state
        .media_route_index
        .get(&source_transport_media_id)
        .map(|entry| DebugRouteEntry {
            source_transport_media_id,
            source_active: entry.source_active,
            effective_packet_gate: state
                .route_control
                .effective_packet_gate(source_transport_media_id)
                .as_ref()
                .map_or(DebugPacketGate::Open, into_debug_packet_gate),
            destinations: entry
                .destinations
                .iter()
                .map(|destination| DebugRouteDestination {
                    dest_session: destination.dest_session.clone(),
                    dest_transport_media_id: destination.dest_transport_media_id,
                    dest_mid: destination.dest_mid,
                    active: destination.active,
                })
                .collect(),
        })
}

fn into_debug_packet_gate(
    packet_gate: &super::super::route_control::PacketLayerGate,
) -> DebugPacketGate {
    match packet_gate {
        super::super::route_control::PacketLayerGate::Open => DebugPacketGate::Open,
        super::super::route_control::PacketLayerGate::Block => DebugPacketGate::Block,
        super::super::route_control::PacketLayerGate::Rid(rid) => {
            DebugPacketGate::Rid(rid.to_string())
        }
    }
}

fn respond_debug_record_incoming_media(
    state: &mut RtcBootstrapState,
    bitrate_state: &Arc<Mutex<RtcBitrateState>>,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    payload_bytes: usize,
    now: Instant,
    response: oneshot::Sender<()>,
) {
    if state
        .record_incoming_bitrate(transport_media_id, now, payload_bytes)
        .is_none()
        && let Ok(mut bitrate) = bitrate_state.lock()
    {
        let counter = bitrate.register_incoming_media(session_key, transport_media_id, now);
        counter.record(now, payload_bytes);
        state.register_incoming_bitrate_counter(transport_media_id, counter);
    }
    let _ = response.send(());
}

fn respond_debug_observe_audio_activity(
    state: &mut RtcBootstrapState,
    transport_media_id: TransportMediaId,
    voice_activity: Option<bool>,
    audio_level_dbov: Option<i8>,
    now: Instant,
    response: oneshot::Sender<()>,
) {
    state.route_control.observe_audio_activity(
        transport_media_id,
        voice_activity,
        audio_level_dbov,
        now,
    );
    let _ = response.send(());
}
