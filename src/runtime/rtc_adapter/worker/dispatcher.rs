use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};

use crate::config::RtcPortRange;

#[cfg(test)]
use super::debug;
use super::{
    super::{
        commands::RtcWorkerCommand,
        state::{RtcBootstrapState, RtcSnapshotState},
    },
    bootstrap, media, session,
};

pub(crate) fn handle_worker_command(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    command: RtcWorkerCommand,
) {
    match command {
        #[cfg(test)]
        RtcWorkerCommand::Debug(command) => {
            debug::handle_debug_command(state, snapshot_state, command);
        }
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key,
            response,
        } => {
            session::respond_remember_remote_addr(
                state,
                snapshot_state,
                source_addr,
                &session_key,
                response,
            );
        }
        command => {
            handle_core_worker_command(state, snapshot_state, public_ip, rtc_port_range, command);
        }
    }
}

fn handle_core_worker_command(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::BuildBootstrap {
            session_key,
            router_capabilities,
            response,
        } => bootstrap::respond_build_bootstrap(
            state,
            snapshot_state,
            public_ip,
            rtc_port_range,
            &session_key,
            &router_capabilities,
            response,
        ),
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
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => session::respond_close_session(state, snapshot_state, &session_key, response),
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
            consumer_rtp_parameters,
            response,
        } => media::respond_add_send_media(
            state,
            &consumer_session_key,
            media_kind,
            &source_session_key,
            source_transport_media_id,
            &consumer_rtp_parameters,
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
        #[cfg(test)]
        RtcWorkerCommand::Debug(_command) => {}
        #[cfg(feature = "internal-benchmarks")]
        RtcWorkerCommand::RememberRemoteAddr { .. } => {}
    }
}
