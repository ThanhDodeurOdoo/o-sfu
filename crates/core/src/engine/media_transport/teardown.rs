use std::sync::Arc;

use tracing::warn;

use super::{
    MediaTransport, TransportAdapterError, TransportMediaId, TransportSessionKey,
    TransportSourceKey, rtc::RtcWorkerCommand,
};
use crate::engine::MediaWorkerId;

#[derive(Debug)]
pub(crate) enum TransportTeardown {
    CloseSession {
        session_key: TransportSessionKey,
    },
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
    },
    ReleaseRelayRoute {
        source: TransportSourceKey,
        target_media_worker_id: MediaWorkerId,
    },
}

impl TransportTeardown {
    pub(crate) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::CloseSession { session_key } | Self::RemoveMedia { session_key, .. } => {
                session_key
            }
            Self::ReleaseRelayRoute { source, .. } => source.session_key(),
        }
    }
}

impl MediaTransport {
    pub(crate) async fn teardown(&self, teardowns: impl IntoIterator<Item = TransportTeardown>) {
        for teardown in teardowns {
            let result = match &teardown {
                TransportTeardown::CloseSession { session_key } => {
                    self.close_session(session_key).await
                }
                TransportTeardown::RemoveMedia {
                    session_key,
                    transport_media_id,
                } => self.remove_media(session_key, *transport_media_id).await,
                TransportTeardown::ReleaseRelayRoute {
                    source,
                    target_media_worker_id,
                } => {
                    self.release_relay_route(source, *target_media_worker_id)
                        .await
                }
            };
            let Err(error) = result else {
                continue;
            };
            let session_key = teardown.session_key();
            let (transport_media_id, target_media_worker_id) = match &teardown {
                TransportTeardown::CloseSession { .. } => (None, None),
                TransportTeardown::RemoveMedia {
                    transport_media_id, ..
                } => (Some(*transport_media_id), None),
                TransportTeardown::ReleaseRelayRoute {
                    source,
                    target_media_worker_id,
                } => (
                    Some(source.transport_media_id()),
                    Some(*target_media_worker_id),
                ),
            };
            self.metrics.record_transport_cleanup_failure();
            warn!(
                ?session_key,
                ?transport_media_id,
                ?target_media_worker_id,
                ?error,
                "media transport teardown reached terminal failure"
            );
            if matches!(
                &teardown,
                TransportTeardown::RemoveMedia { .. } | TransportTeardown::ReleaseRelayRoute { .. }
            ) && let Err(fallback_error) = self.close_session(session_key).await
            {
                warn!(
                    ?session_key,
                    ?transport_media_id,
                    ?target_media_worker_id,
                    ?fallback_error,
                    "media transport teardown session fallback failed"
                );
            }
        }
    }

    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let worker = self.require_worker_for_user(session_key)?;
        let Some(handle) = worker.worker_handle()? else {
            return Ok(());
        };
        match worker
            .send_worker_command(&handle, |response| RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response,
            })
            .await
        {
            Err(TransportAdapterError::TransportUnavailable) => Ok(()),
            result => result,
        }
    }

    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let worker = self.require_worker_for_user(session_key)?;
        let Some(handle) = worker.worker_handle()? else {
            return Ok(());
        };
        match worker
            .send_worker_command(&handle, |response| RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            })
            .await
        {
            Err(TransportAdapterError::TransportUnavailable) => Ok(()),
            result => result,
        }
    }

    async fn release_relay_route(
        &self,
        source: &TransportSourceKey,
        target_media_worker_id: MediaWorkerId,
    ) -> Result<(), TransportAdapterError> {
        let source_worker = self.require_worker_for_user(source.session_key())?;
        let target_worker = self.require_worker_for_media_worker_id(target_media_worker_id)?;
        if Arc::ptr_eq(&source_worker, &target_worker) {
            return Ok(());
        }
        let Some(handle) = source_worker.worker_handle()? else {
            return Ok(());
        };
        let request = target_worker.relay_release_request(source.clone());
        match source_worker
            .send_worker_command(&handle, |response| {
                RtcWorkerCommand::media_control(request, response)
            })
            .await
        {
            Err(TransportAdapterError::TransportUnavailable) => Ok(()),
            result => result,
        }
    }
}
