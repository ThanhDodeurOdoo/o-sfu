pub(crate) mod event {
    pub(crate) const HTTP_LISTENER_READY: &str = "http.listener.ready";
    pub(crate) const RUNTIME_BOOT: &str = "runtime.boot";
    pub(crate) const RUNTIME_TELEMETRY_INITIALIZED: &str = "runtime.telemetry_initialized";
    pub(crate) const WS_CONNECTION_ACCEPTED: &str = "ws.connection.accepted";
    pub(crate) const WS_CONNECTION_CLOSED: &str = "ws.connection.closed";
    pub(crate) const WS_HANDSHAKE_REJECTED: &str = "ws.handshake.rejected";
    pub(crate) const WS_SESSION_ESTABLISHED: &str = "ws.session.established";
}

pub(crate) mod field {
    pub(crate) const CHANNEL_UUID: &str = "channel_uuid";
    pub(crate) const CONNECTION_ID: &str = "connection_id";
    pub(crate) const DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";
    pub(crate) const EVENT: &str = "event";
    pub(crate) const MESSAGE: &str = "message";
    pub(crate) const REASON: &str = "reason";
    pub(crate) const SERVICE_INSTANCE_ID: &str = "service.instance.id";
    pub(crate) const SERVICE_NAME: &str = "service.name";
    pub(crate) const SESSION_ID: &str = "session_id";
    pub(crate) const TARGET: &str = "target";
    pub(crate) const TIMESTAMP: &str = "timestamp";
    pub(crate) const TRACE_ID: &str = "trace_id";
}

pub(crate) const COMMON_FIELD_NAMES: &[&str] = &[
    field::TIMESTAMP,
    field::EVENT,
    field::MESSAGE,
    field::TARGET,
    field::SERVICE_NAME,
    field::SERVICE_INSTANCE_ID,
    field::DEPLOYMENT_ENVIRONMENT,
];

pub(crate) const CORRELATION_FIELD_NAMES: &[&str] = &[
    field::CHANNEL_UUID,
    field::SESSION_ID,
    field::CONNECTION_ID,
    field::TRACE_ID,
    field::REASON,
];
