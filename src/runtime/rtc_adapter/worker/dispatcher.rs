use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};

use crate::config::{MediaCodecFlags, RtcPortRange};
use crate::runtime::metrics::RuntimeMetrics;

#[cfg(any(test, feature = "internal-benchmarks"))]
use super::bootstrap;
#[cfg(test)]
use super::debug;
use super::{
    super::{
        commands::RtcWorkerCommand,
        relay_registry::RelayRegistry,
        state::{RtcBootstrapState, RtcSnapshotState},
    },
    media,
    negotiation::{self, OfferBootstrapConfig},
    publication, session,
};

pub(crate) struct WorkerCommandContext<'a> {
    pub(crate) snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub(crate) relay_registry: &'a RelayRegistry,
    pub(crate) public_ip: IpAddr,
    pub(crate) rtc_port_range: RtcPortRange,
    pub(crate) codec_flags: MediaCodecFlags,
    pub(crate) metrics: &'a RuntimeMetrics,
}

pub(crate) fn handle_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        #[cfg(test)]
        RtcWorkerCommand::Debug(command) => {
            debug::handle_debug_command(state, context.snapshot_state, command);
        }
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => {
            session::respond_remember_remote_addr(
                state,
                context.snapshot_state,
                source_addr,
                &session_key,
                response,
            );
        }
        command => {
            handle_core_worker_command(state, context, command);
        }
    }
}

fn handle_core_worker_command(
    state: &mut RtcBootstrapState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        #[cfg(any(test, feature = "internal-benchmarks"))]
        RtcWorkerCommand::BuildBootstrap {
            session_key,
            router_capabilities,
            response,
        } => bootstrap::respond_build_bootstrap(
            state,
            context.snapshot_state,
            bootstrap::WorkerBootstrapConfig::new(
                context.public_ip,
                context.rtc_port_range,
                context.codec_flags,
            ),
            &session_key,
            &router_capabilities,
            context.metrics,
            response,
        ),
        #[cfg(test)]
        RtcWorkerCommand::ConnectTransport {
            session_key,
            direction,
            parsed_dtls_parameters,
            remote_ice_credentials,
            response,
        } => bootstrap::respond_connect_transport(
            state,
            &session_key,
            direction,
            &parsed_dtls_parameters,
            remote_ice_credentials.as_ref(),
            response,
        ),
        RtcWorkerCommand::CreateInitialSessionOffer { .. }
        | RtcWorkerCommand::CreateSessionRenegotiationOffer { .. }
        | RtcWorkerCommand::ApplySessionAnswer { .. } => {
            handle_negotiation_command(state, context, command);
        }
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => session::respond_close_session(
            state,
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
        RtcWorkerCommand::RemoveMedia { .. }
        | RtcWorkerCommand::AddRecvMedia { .. }
        | RtcWorkerCommand::AddSendMedia { .. }
        | RtcWorkerCommand::RequestRemoteKeyframe { .. }
        | RtcWorkerCommand::SetRemoteSourceRouteActive { .. }
        | RtcWorkerCommand::SetRemoteSourcePacketGate { .. }
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. }
        | RtcWorkerCommand::SetSourceRoutingPolicy { .. } => {
            handle_media_command(state, context.metrics, context.relay_registry, command);
        }
        #[cfg(test)]
        RtcWorkerCommand::Debug(_command) => {}
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
                rtc_port_range: context.rtc_port_range,
                codec_flags: context.codec_flags,
                metrics: context.metrics,
            },
            &session_key,
            response,
        ),
        RtcWorkerCommand::CreateSessionRenegotiationOffer {
            session_key,
            response,
        } => negotiation::respond_create_session_renegotiation_offer(state, &session_key, response),
        RtcWorkerCommand::ApplySessionAnswer {
            session_key,
            answer_sdp,
            response,
        } => negotiation::respond_apply_session_answer(state, &session_key, &answer_sdp, response),
        _ => {}
    }
}

fn handle_media_command(
    state: &mut RtcBootstrapState,
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
        | RtcWorkerCommand::SetProducerActive { .. }
        | RtcWorkerCommand::SetConsumerActive { .. }
        | RtcWorkerCommand::SetSourceRoutingPolicy { .. } => {
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
        RtcWorkerCommand::SetSourceRoutingPolicy {
            session_key,
            source_transport_media_id,
            policy,
            response,
        } => media::respond_set_source_routing_policy(
            state,
            &session_key,
            source_transport_media_id,
            policy,
            response,
        ),
        _ => {}
    }
}
