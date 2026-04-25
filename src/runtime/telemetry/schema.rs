#[allow(
    dead_code,
    reason = "The event catalog intentionally grows ahead of call-site rollout so observability names stay centralized across phases."
)]
pub(crate) mod event {
    pub(crate) const RUNTIME_LOG: &str = "runtime.log";
    pub(crate) const HTTP_LISTENER_READY: &str = "http.listener.ready";
    pub(crate) const RUNTIME_BOOT: &str = "runtime.boot";
    pub(crate) const RUNTIME_SHUTDOWN: &str = "runtime.shutdown";
    pub(crate) const RUNTIME_TELEMETRY_INITIALIZED: &str = "runtime.telemetry_initialized";
    pub(crate) const ROOM_CREATED: &str = "room.created";
    pub(crate) const USER_JOINED: &str = "user.joined";
    pub(crate) const USER_CLOSED: &str = "user.closed";
    pub(crate) const USER_DISCONNECTED: &str = "user.disconnected";
    pub(crate) const WS_CONNECTION_ACCEPTED: &str = "ws.accepted";
    pub(crate) const WS_CONNECTION_CLOSED: &str = "ws.closed";
    pub(crate) const WS_AUTH_REJECTED: &str = "ws.auth_rejected";
    pub(crate) const WS_HANDSHAKE_REJECTED: &str = "ws.handshake_rejected";
    pub(crate) const WS_JOIN_FAILED: &str = "ws.join_failed";
    pub(crate) const WS_JOIN_SUCCEEDED: &str = "ws.join_succeeded";
    pub(crate) const WS_USER_ESTABLISHED: &str = "ws.user.established";
    pub(crate) const NEGOTIATION_STARTED: &str = "negotiation.started";
    pub(crate) const NEGOTIATION_SUCCEEDED: &str = "negotiation.succeeded";
    pub(crate) const NEGOTIATION_FAILED: &str = "negotiation.failed";
    pub(crate) const PUBLISH_PREPARED: &str = "publish.prepared";
    pub(crate) const PUBLISH_COMMITTED: &str = "publish.committed";
    pub(crate) const PUBLISH_ABORTED: &str = "publish.aborted";
    pub(crate) const SUBSCRIBE_PREPARED: &str = "subscribe.prepared";
    pub(crate) const SUBSCRIBE_SUCCEEDED: &str = "subscribe.succeeded";
    pub(crate) const SUBSCRIBE_REJECTED: &str = "subscribe.rejected";
    pub(crate) const SUBSCRIPTION_ACTIVITY_CHANGED: &str = "subscription.activity_changed";
    pub(crate) const PUBLICATION_ACTIVITY_CHANGED: &str = "publication.activity_changed";
    pub(crate) const TRANSPORT_ICE_STATE_CHANGED: &str = "transport.ice_state.changed";
    pub(crate) const TRANSPORT_HEALTH_CHANGED: &str = "transport.health.changed";
    pub(crate) const TRANSPORT_DTLS_CONNECTED: &str = "transport.dtls.connected";
    pub(crate) const RECORDING_STARTED: &str = "recording.started";
    pub(crate) const RECORDING_STOPPED: &str = "recording.stopped";
    pub(crate) const RECORDING_FINALIZED: &str = "recording.finalized";
}

#[allow(
    dead_code,
    reason = "The field catalog intentionally grows ahead of broad JSON-log and trace rollout so correlation names stay centralized."
)]
pub(crate) mod field {
    pub(crate) const ROOM_ID: &str = "room_id";
    pub(crate) const CLOSE_CODE: &str = "close_code";
    pub(crate) const CONNECTION_ID: &str = "connection_id";
    pub(crate) const DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";
    pub(crate) const DURATION_MS: &str = "duration_ms";
    pub(crate) const ERROR_KIND: &str = "error_kind";
    pub(crate) const EVENT: &str = "event";
    pub(crate) const LEVEL: &str = "level";
    pub(crate) const MESSAGE: &str = "message";
    pub(crate) const MEDIA_WORKER_ID: &str = "media_worker_id";
    pub(crate) const OPERATION: &str = "operation";
    pub(crate) const OUTCOME: &str = "outcome";
    pub(crate) const REASON: &str = "reason";
    pub(crate) const REMOTE_ADDRESS: &str = "remote_address";
    pub(crate) const SERVICE_INSTANCE_ID: &str = "service.instance.id";
    pub(crate) const SERVICE_NAME: &str = "service.name";
    pub(crate) const SERVICE_VERSION: &str = "service.version";
    pub(crate) const USER_ID: &str = "user_id";
    pub(crate) const TARGET: &str = "target";
    pub(crate) const TIMESTAMP: &str = "timestamp";
    pub(crate) const TRACE_ID: &str = "trace_id";
    pub(crate) const TRANSPORT_MEDIA_ID: &str = "transport_media_id";
}

pub(crate) const COMMON_FIELD_NAMES: &[&str] = &[
    field::TIMESTAMP,
    field::LEVEL,
    field::EVENT,
    field::MESSAGE,
    field::TARGET,
    field::SERVICE_NAME,
    field::SERVICE_VERSION,
    field::SERVICE_INSTANCE_ID,
    field::DEPLOYMENT_ENVIRONMENT,
];

pub(crate) const CORRELATION_FIELD_NAMES: &[&str] = &[
    field::ROOM_ID,
    field::USER_ID,
    field::CONNECTION_ID,
    field::REMOTE_ADDRESS,
    field::TRANSPORT_MEDIA_ID,
    field::MEDIA_WORKER_ID,
    field::TRACE_ID,
    field::OPERATION,
    field::OUTCOME,
    field::REASON,
    field::CLOSE_CODE,
    field::ERROR_KIND,
    field::DURATION_MS,
];
