use std::net::SocketAddr;

use crate::runtime::transport_adapter::{TransportAdapterError, TransportSessionKey};

use super::super::commands::RtcWorkerCommand;
use super::facade::RtcTransportAdapter;

impl RtcTransportAdapter {
    pub(crate) async fn benchmark_register_remote_addr(
        &self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RememberRemoteAddr {
            source_addr,
            session_key: session_key.clone(),
            response,
        })
        .await
    }

    pub(crate) fn benchmark_cached_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .remote_addr_demux
            .session_key_for_remote_addr(source_addr)
            .is_some_and(|session_key| snapshot_state.live_sessions.contains(session_key))
    }

    pub(crate) fn benchmark_linear_remote_addr_lookup(&self, source_addr: SocketAddr) -> bool {
        let Some(worker_handle) = self.worker_handle().ok().flatten() else {
            return false;
        };
        let Ok(snapshot_state) = worker_handle.snapshot_state.lock() else {
            return false;
        };
        snapshot_state
            .remote_addr_demux
            .session_entries()
            .any(|(session_key, session_addrs)| {
                snapshot_state.live_sessions.contains(session_key)
                    && session_addrs.contains(&source_addr)
            })
    }
}
