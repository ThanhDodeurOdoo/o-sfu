use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
};

use o_sfu_router::RtpCapabilities;
use tokio::sync::oneshot;
use tracing::debug;

use crate::config::MediaCodecFlags;
use crate::config::RtcPortRange;
use crate::runtime::{
    transport_adapter::{TransportAdapterError, TransportConnectDirection, TransportSessionKey},
    transport_bootstrap::SessionTransportBootstrap,
};

use super::super::{
    bootstrap, dtls,
    state::{ParsedRemoteIceCredentials, RtcBootstrapState, RtcSnapshotState},
    validation,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct WorkerBootstrapConfig {
    public_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    codec_flags: MediaCodecFlags,
}

impl WorkerBootstrapConfig {
    #[must_use]
    pub(super) const fn new(
        public_ip: IpAddr,
        rtc_port_range: RtcPortRange,
        codec_flags: MediaCodecFlags,
    ) -> Self {
        Self {
            public_ip,
            rtc_port_range,
            codec_flags,
        }
    }
}

pub(super) fn respond_build_bootstrap(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: WorkerBootstrapConfig,
    session_key: &TransportSessionKey,
    router_capabilities: &RtpCapabilities,
    response: oneshot::Sender<Result<SessionTransportBootstrap, TransportAdapterError>>,
) {
    let _ = response.send(worker_build_bootstrap_payload(
        state,
        snapshot_state,
        config,
        session_key,
        router_capabilities,
    ));
}

pub(super) fn respond_connect_transport(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
    response: oneshot::Sender<Result<(), TransportAdapterError>>,
) {
    let result = worker_ensure_transport_connect_compatibility(
        state,
        session_key,
        parsed_dtls_parameters,
        remote_ice_credentials,
    )
    .and_then(|()| {
        worker_apply_transport_connect(
            state,
            session_key,
            direction,
            parsed_dtls_parameters,
            remote_ice_credentials,
        )
    });
    let _ = response.send(result);
}

fn worker_build_bootstrap_payload(
    state: &mut RtcBootstrapState,
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    config: WorkerBootstrapConfig,
    session_key: &TransportSessionKey,
    router_capabilities: &RtpCapabilities,
) -> Result<SessionTransportBootstrap, TransportAdapterError> {
    let candidate_addr = if let Some(shared_socket) = state.shared_socket.as_ref() {
        shared_socket.candidate_addr
    } else {
        let shared_socket =
            bootstrap::bind_shared_rtc_socket(config.public_ip, config.rtc_port_range)?;
        let candidate_addr = shared_socket.candidate_addr;
        state.shared_socket = Some(shared_socket);
        candidate_addr
    };
    bootstrap::ensure_session_rtc_state(
        &mut state.sessions,
        session_key,
        candidate_addr,
        config.codec_flags,
    )?;
    state.mark_session_dirty(session_key);
    if let Ok(mut snapshot) = snapshot_state.lock() {
        snapshot.add_session(session_key);
    }
    let Some(session_state) = state.sessions.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    Ok(SessionTransportBootstrap::new(
        router_capabilities,
        bootstrap::build_transport_bootstrap(
            session_state.transport_ids.download.as_str(),
            candidate_addr,
            &session_state.local_ice_credentials,
            &session_state.local_dtls_fingerprint,
        ),
        bootstrap::build_transport_bootstrap(
            session_state.transport_ids.upload.as_str(),
            candidate_addr,
            &session_state.local_ice_credentials,
            &session_state.local_dtls_fingerprint,
        ),
    ))
}

fn worker_ensure_transport_connect_compatibility(
    state: &RtcBootstrapState,
    session_key: &TransportSessionKey,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
) -> Result<(), TransportAdapterError> {
    let Some(session_state) = state.sessions.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let Some(primary_fingerprint) = parsed_dtls_parameters.fingerprints().first() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let fingerprint_literal = format!(
        "{} {}",
        primary_fingerprint.algorithm(),
        primary_fingerprint.value()
    );
    validation::ensure_remote_fingerprint_compatibility(session_state, &fingerprint_literal)?;
    validation::ensure_remote_ice_credentials_compatibility(session_state, remote_ice_credentials)?;
    Ok(())
}

fn worker_apply_transport_connect(
    state: &mut RtcBootstrapState,
    session_key: &TransportSessionKey,
    direction: TransportConnectDirection,
    parsed_dtls_parameters: &dtls::ParsedDtlsParameters,
    remote_ice_credentials: Option<&ParsedRemoteIceCredentials>,
) -> Result<(), TransportAdapterError> {
    let Some(session_state) = state.sessions.get_mut(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let Some(primary_fingerprint) = parsed_dtls_parameters.fingerprints().first() else {
        return Err(TransportAdapterError::InvalidInput);
    };
    let fingerprint_literal = format!(
        "{} {}",
        primary_fingerprint.algorithm(),
        primary_fingerprint.value()
    );
    let remote_fingerprint = validation::parse_remote_fingerprint(primary_fingerprint)?;
    let should_start_dtls = !session_state.dtls_started;
    {
        let mut direct_api = session_state.rtc.direct_api();
        if let Some(remote_ice_credentials) = remote_ice_credentials
            && session_state.remote_ice_credentials.is_none()
        {
            direct_api.set_remote_ice_credentials(remote_ice_credentials.as_ice_creds());
            session_state.remote_ice_credentials = Some(remote_ice_credentials.clone());
        }
        if session_state.remote_dtls_fingerprint.is_none() {
            direct_api.set_remote_fingerprint(remote_fingerprint);
            session_state.remote_dtls_fingerprint = Some(fingerprint_literal);
        }
        if should_start_dtls {
            let local_active_role =
                validation::local_dtls_active_role(parsed_dtls_parameters.role());
            direct_api
                .start_dtls(local_active_role)
                .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
            session_state.dtls_started = true;
            debug!(
                ?direction,
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                local_active_role,
                "started rtc DTLS handshake after transport connect"
            );
        } else {
            debug!(
                ?direction,
                session_id = ?session_key.session_id(),
                channel_runtime_id = session_key.channel_runtime_id(),
                "rtc DTLS handshake already started for session"
            );
        }
    }
    state.mark_session_dirty(session_key);
    Ok(())
}
