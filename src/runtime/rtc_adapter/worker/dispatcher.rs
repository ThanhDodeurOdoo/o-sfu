//! command dispatcher for worker-local RTC state.
//!
//! This module exists to keep the mailbox match in one place while the actual
//! state mutation lives in focused submodules. It should stay simple:
//!  decode one worker command, forward it to the owning module, and passe
//! through the immutable runtime context that those handlers need.

use std::{
    collections::BTreeSet,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::ChannelRuntimeId;
use crate::runtime::metrics::RuntimeMetrics;
use crate::runtime::transport_adapter::{ActiveSpeakerSource, TransportAdapterError};
use tokio::sync::oneshot;

#[cfg(test)]
use super::super::commands::debug::DebugRtcWorkerCommand;
#[cfg(test)]
use super::debug;
use super::{
    super::{
        commands::RtcWorkerCommand,
        relay_registry::RelayRegistry,
        state::{RtcBitrateState, RtcBootstrapState, RtcSnapshotState},
    },
    media,
    negotiation::{self, OfferBootstrapConfig},
    publication, session,
};

pub(crate) struct WorkerCommandContext<'a> {
    pub(crate) bitrate_state: &'a Arc<Mutex<RtcBitrateState>>,
    pub(crate) snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub(crate) relay_registry: &'a RelayRegistry,
    pub(crate) public_ip: IpAddr,
    pub(crate) max_bitrate_in_bps: u64,
    pub(crate) max_bitrate_out_bps: u64,
    pub(crate) rtc_port_range: RtcPortRange,
    pub(crate) codec_flags: MediaCodecFlags,
    pub(crate) metrics: &'a RuntimeMetrics,
}

/// Dispatch one production worker command against the shard-local RTC state.
///
/// Callers must already serialize access to `state`, this function assumes it
/// runs on the packet-loop task that owns the shard.
pub(crate) fn handle_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => session::respond_remember_remote_addr(
            state,
            context.snapshot_state,
            source_addr,
            &session_key,
            response,
        ),
        command => {
            handle_core_worker_command(state, context, command);
        }
    }
}

#[cfg(test)]
/// Dispatch one test-only debug command against the same shard-local worker
/// state used by production commands.
pub(crate) fn handle_debug_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: DebugRtcWorkerCommand,
) {
    debug::handle_debug_command(
        state,
        context.bitrate_state,
        context.snapshot_state,
        command,
    );
}

fn handle_core_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer { .. }
        | RtcWorkerCommand::ActiveSpeakerSourceSnapshot { .. }
        | RtcWorkerCommand::NextActiveSpeakerDeadline { .. }
        | RtcWorkerCommand::ExpiredActiveSpeakerChannelRuntimeIds { .. }
        | RtcWorkerCommand::CreateSessionRenegotiationOffer { .. }
        | RtcWorkerCommand::ApplySessionAnswer { .. } => {
            handle_negotiation_command(state, context, command);
        }
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => session::respond_close_session(
            state,
            context.bitrate_state,
            context.snapshot_state,
            &session_key,
            context.metrics,
            response,
        ),
        RtcWorkerCommand::ResolveNegotiatedProducerParameters {
            session_key,
            transport_media_id,
            response,
        } => publication::respond_resolve_negotiated_producer_parameters(
            state,
            &session_key,
            transport_media_id,
            response,
        ),
        RtcWorkerCommand::ResolveMediaMid {
            transport_media_id,
            response,
        } => media::respond_resolve_media_mid(state, transport_media_id, response),
        RtcWorkerCommand::RemoveMedia { .. }
        | RtcWorkerCommand::AddRecvMedia { .. }
        | RtcWorkerCommand::AddSendMedia { .. }
        | RtcWorkerCommand::RequestRemoteKeyframe { .. }
        | RtcWorkerCommand::SetRemoteSourceRouteActive { .. }
        | RtcWorkerCommand::SetRemoteSourcePacketGate { .. }
        | RtcWorkerCommand::SetSourcePacketGate { .. }
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. } => {
            handle_media_command(
                state,
                context.max_bitrate_in_bps,
                context.metrics,
                context.relay_registry,
                command,
            );
        }
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr { .. } => {}
    }
}

fn handle_negotiation_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer {
            session_key,
            response,
        } => negotiation::respond_create_initial_session_offer(
            state,
            context.snapshot_state,
            OfferBootstrapConfig {
                public_ip: context.public_ip,
                max_bitrate_out_bps: context.max_bitrate_out_bps,
                rtc_port_range: context.rtc_port_range,
                codec_flags: context.codec_flags,
                metrics: context.metrics,
            },
            &session_key,
            response,
        ),
        RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response } => {
            respond_active_speaker_source_snapshot(state, response);
        }
        RtcWorkerCommand::NextActiveSpeakerDeadline { response } => {
            respond_next_active_speaker_deadline(state, response);
        }
        RtcWorkerCommand::ExpiredActiveSpeakerChannelRuntimeIds { now, response } => {
            respond_expired_active_speaker_channel_runtime_ids(state, now, response);
        }
        RtcWorkerCommand::CreateSessionRenegotiationOffer {
            session_key,
            response,
        } => negotiation::respond_create_session_renegotiation_offer(state, &session_key, response),
        RtcWorkerCommand::ApplySessionAnswer {
            session_key,
            answer_sdp,
            response,
        } => negotiation::respond_apply_session_answer(
            state,
            context.max_bitrate_in_bps,
            &session_key,
            &answer_sdp,
            response,
        ),
        _ => {}
    }
}

fn respond_active_speaker_source_snapshot(
    state: &RtcBootstrapState,
    response: oneshot::Sender<Result<Vec<ActiveSpeakerSource>, TransportAdapterError>>,
) {
    let snapshot = state.active_speaker_source_snapshot(Instant::now());
    let _ = response.send(Ok(snapshot));
}

fn respond_next_active_speaker_deadline(
    state: &RtcBootstrapState,
    response: oneshot::Sender<Result<Option<Instant>, TransportAdapterError>>,
) {
    let deadline = state
        .route_control
        .next_active_speaker_deadline(Instant::now());
    let _ = response.send(Ok(deadline));
}

fn respond_expired_active_speaker_channel_runtime_ids(
    state: &RtcBootstrapState,
    now: Instant,
    response: oneshot::Sender<Result<BTreeSet<ChannelRuntimeId>, TransportAdapterError>>,
) {
    let channel_runtime_ids = state.expired_active_speaker_channel_runtime_ids(now);
    let _ = response.send(Ok(channel_runtime_ids));
}

fn handle_media_command(
    state: &mut RtcBootstrapState,
    max_bitrate_in_bps: u64,
    metrics: &RuntimeMetrics,
    relay_registry: &RelayRegistry,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::RemoveMedia {
            session_key,
            transport_media_id,
            response,
        } => media::respond_remove_media(state, &session_key, transport_media_id, response),
        RtcWorkerCommand::AddRecvMedia {
            session_key,
            media_kind,
            rtp_parameters,
            response,
        } => media::respond_add_recv_media(
            state,
            max_bitrate_in_bps,
            &session_key,
            media_kind,
            &rtp_parameters,
            response,
        ),
        RtcWorkerCommand::AddSendMedia {
            consumer_session_key,
            media_kind,
            source_session_key,
            source_transport_media_id,
            remote_source_control,
            consumer_rtp_parameters,
            response,
        } => media::respond_add_send_media(
            state,
            media::AddSendMediaRequest {
                consumer_session_key: &consumer_session_key,
                media_kind,
                source_session_key: &source_session_key,
                source_transport_media_id,
                remote_source_control,
                consumer_rtp_parameters: &consumer_rtp_parameters,
            },
            response,
        ),
        RtcWorkerCommand::RequestRemoteKeyframe { .. }
        | RtcWorkerCommand::SetRemoteSourceRouteActive { .. }
        | RtcWorkerCommand::SetRemoteSourcePacketGate { .. }
        | RtcWorkerCommand::SetSourcePacketGate { .. }
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. } => {
            handle_media_route_control_command(state, metrics, relay_registry, command);
        }
        _ => {}
    }
}

fn handle_media_route_control_command(
    state: &mut RtcBootstrapState,
    metrics: &RuntimeMetrics,
    relay_registry: &RelayRegistry,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::RequestRemoteKeyframe {
            source_session_key,
            source_transport_media_id,
            target_id,
            rid,
            kind,
        } => media::respond_request_remote_keyframe(
            state,
            metrics,
            relay_registry,
            &media::RemoteKeyframeRequest {
                source_session_key: &source_session_key,
                source_transport_media_id,
                target_id,
                rid,
                kind,
            },
        ),
        RtcWorkerCommand::SetRemoteSourceRouteActive {
            source_session_key,
            source_transport_media_id,
            target_id,
            active,
        } => media::respond_set_remote_source_route_active(
            state,
            relay_registry,
            &source_session_key,
            source_transport_media_id,
            target_id,
            active,
        ),
        RtcWorkerCommand::SetRemoteSourcePacketGate {
            source_session_key,
            source_transport_media_id,
            target_id,
            packet_gate,
        } => media::respond_set_remote_source_packet_gate(
            state,
            &source_session_key,
            source_transport_media_id,
            target_id,
            packet_gate,
        ),
        RtcWorkerCommand::SetSourcePacketGate {
            source_session_key,
            source_transport_media_id,
            packet_gate,
            response,
        } => media::respond_set_source_packet_gate(
            state,
            &source_session_key,
            source_transport_media_id,
            packet_gate,
            response,
        ),
        RtcWorkerCommand::SetProducerActive {
            session_key,
            transport_media_id,
            active,
            response,
        } => media::respond_set_producer_active(
            state,
            &session_key,
            transport_media_id,
            active,
            response,
        ),
        RtcWorkerCommand::SetConsumerActive {
            consumer_session_key,
            consumer_transport_media_id,
            source_session_key,
            source_transport_media_id,
            active,
            response,
        } => media::respond_set_consumer_active(
            state,
            &consumer_session_key,
            consumer_transport_media_id,
            &source_session_key,
            source_transport_media_id,
            active,
            response,
        ),
        _ => {}
    }
}
