use std::{
    io,
    sync::{Arc, Mutex},
};

use tracing::{Subscriber, subscriber};
#[cfg(feature = "otel-tracing")]
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{fmt::MakeWriter, prelude::*};

use super::*;
use crate::{TelemetryLogFormat, TelemetryResource, TraceExportConfig};

#[derive(Clone, Debug, Default)]
struct SharedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl<'writer> MakeWriter<'writer> for SharedWriter {
    type Writer = SharedBufferGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedBufferGuard {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

#[derive(Debug)]
struct SharedBufferGuard {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for SharedBufferGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        {
            let lock = self.buffer.lock();
            assert!(lock.is_ok());
            if let Ok(mut guard) = lock {
                guard.extend_from_slice(buf);
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn clone_buffer(writer: &SharedWriter) -> Option<Vec<u8>> {
    let lock = writer.buffer.lock();
    assert!(lock.is_ok());
    lock.map_or(None, |guard| Some(guard.clone()))
}

fn json_field<'value>(value: &'value Value, key: &str) -> Option<&'value Value> {
    value.get(key)
}

fn json_string<'value>(value: &'value Value, key: &str) -> Option<&'value str> {
    json_field(value, key).and_then(Value::as_str)
}

fn json_is_string(value: &Value, key: &str) -> bool {
    json_field(value, key).is_some_and(Value::is_string)
}

fn assert_json_string(value: &Value, key: &str, expected: &str) {
    assert_eq!(json_string(value, key), Some(expected));
}

#[cfg(feature = "otel-tracing")]
#[test]
fn normalize_trace_export_endpoint_appends_default_http_trace_path() {
    assert_eq!(
        normalize_trace_export_endpoint("http://collector:4318"),
        "http://collector:4318/v1/traces"
    );
    assert_eq!(
        normalize_trace_export_endpoint("http://collector:4318/v1/traces"),
        "http://collector:4318/v1/traces"
    );
}

#[cfg(feature = "otel-tracing")]
#[test]
fn json_formatter_emits_common_fields_and_trace_id() {
    let writer = SharedWriter::default();
    let subscriber = json_test_subscriber(writer.clone());
    subscriber::with_default(subscriber, || {
        let span = activated_span(tracing::info_span!("ws.handshake", room_id = "room-a"));
        let _entered = span.enter();
        tracing::info!(
            event = schema::event::WS_JOIN_SUCCEEDED,
            user_id = "user-1",
            message = "joined user"
        );
    });

    let buffer = clone_buffer(&writer);
    assert!(buffer.is_some());
    let Some(buffer) = buffer else {
        return;
    };
    let payload = String::from_utf8(buffer);
    assert!(payload.is_ok());
    let Some(payload) = payload.ok() else {
        return;
    };
    let line = payload.lines().find(|line| !line.trim().is_empty());
    assert!(line.is_some());
    let Some(line) = line else {
        return;
    };
    let value = serde_json::from_str::<Value>(line);
    assert!(value.is_ok());
    let Some(value) = value.ok() else {
        return;
    };

    assert_json_string(&value, "event", schema::event::WS_JOIN_SUCCEEDED);
    assert_json_string(&value, "message", "joined user");
    assert_json_string(&value, "service.name", "o-sfu-test");
    assert_json_string(&value, "service.version", env!("CARGO_PKG_VERSION"));
    assert_json_string(&value, "service.instance.id", "test-instance");
    assert_json_string(&value, "deployment.environment", "test");
    assert_json_string(&value, "user_id", "user-1");
    assert_json_string(&value, "target", "o_sfu_telemetry::setup::tests");
    assert!(json_is_string(&value, "timestamp"));
    assert!(json_is_string(&value, "trace_id"));
    assert_ne!(
        json_string(&value, "trace_id"),
        Some("00000000000000000000000000000000")
    );
}

#[cfg(not(feature = "otel-tracing"))]
#[test]
fn json_formatter_emits_common_fields_without_trace_id() {
    let writer = SharedWriter::default();
    let subscriber = json_test_subscriber(writer.clone());
    subscriber::with_default(subscriber, || {
        let span = activated_span(tracing::info_span!("ws.handshake", room_id = "room-a"));
        let _entered = span.enter();
        tracing::info!(
            event = schema::event::WS_JOIN_SUCCEEDED,
            user_id = "user-1",
            message = "joined user"
        );
    });

    let buffer = clone_buffer(&writer);
    assert!(buffer.is_some());
    let Some(buffer) = buffer else {
        return;
    };
    let payload = String::from_utf8(buffer);
    assert!(payload.is_ok());
    let Some(payload) = payload.ok() else {
        return;
    };
    let line = payload.lines().find(|line| !line.trim().is_empty());
    assert!(line.is_some());
    let Some(line) = line else {
        return;
    };
    let value = serde_json::from_str::<Value>(line);
    assert!(value.is_ok());
    let Some(value) = value.ok() else {
        return;
    };

    assert_json_string(&value, "event", schema::event::WS_JOIN_SUCCEEDED);
    assert_json_string(&value, "message", "joined user");
    assert_json_string(&value, "service.name", "o-sfu-test");
    assert_json_string(&value, "service.version", env!("CARGO_PKG_VERSION"));
    assert_json_string(&value, "service.instance.id", "test-instance");
    assert_json_string(&value, "deployment.environment", "test");
    assert_json_string(&value, "user_id", "user-1");
    assert_json_string(&value, "target", "o_sfu_telemetry::setup::tests");
    assert!(json_is_string(&value, "timestamp"));
    assert!(json_field(&value, "trace_id").is_none());
}

fn json_test_config() -> TelemetryConfig {
    TelemetryConfig {
        log_format: TelemetryLogFormat::Json,
        resource: TelemetryResource {
            service_name: "o-sfu-test".to_owned(),
            deployment_environment: "test".to_owned(),
            service_instance_id: Some("test-instance".to_owned()),
        },
        trace_export: TraceExportConfig::default(),
        media_quality_interval: None,
    }
}

#[cfg(feature = "otel-tracing")]
fn json_test_subscriber(writer: SharedWriter) -> impl Subscriber + Send + Sync {
    let resource = telemetry_resource_fields(&json_test_config(), 7);
    let tracer_provider = SdkTracerProvider::builder().build();
    let tracer = tracer_provider.tracer(TRACE_EXPORTER_NAME);
    Registry::default()
        .with(EnvFilter::new(DEFAULT_ENV_FILTER))
        .with(
            fmt_layer()
                .fmt_fields(JsonFields::new())
                .event_format(RuntimeJsonFormatter::new(resource))
                .with_ansi(false)
                .with_writer(writer),
        )
        .with(Some(OpenTelemetryLayer::new(tracer)))
}

#[cfg(not(feature = "otel-tracing"))]
fn json_test_subscriber(writer: SharedWriter) -> impl Subscriber + Send + Sync {
    let resource = telemetry_resource_fields(&json_test_config(), 7);
    Registry::default()
        .with(EnvFilter::new(DEFAULT_ENV_FILTER))
        .with(
            fmt_layer()
                .fmt_fields(JsonFields::new())
                .event_format(RuntimeJsonFormatter::new(resource))
                .with_ansi(false)
                .with_writer(writer),
        )
}
