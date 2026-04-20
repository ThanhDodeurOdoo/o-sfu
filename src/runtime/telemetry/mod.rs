use anyhow::{Result, anyhow};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::{TelemetryConfig, TelemetryLogFormat};

pub(crate) mod schema;

const DEFAULT_ENV_FILTER: &str = "o_sfu=info,o_sfu_router=info";

pub(crate) fn init_tracing(config: &TelemetryConfig, process_id: u32) -> Result<()> {
    match config.log_format {
        TelemetryLogFormat::Compact => tracing_subscriber::fmt()
            .with_env_filter(default_env_filter())
            .with_target(false)
            .compact()
            .try_init()
            .map_err(|error| anyhow!(error.to_string()))?,
        TelemetryLogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(default_env_filter())
            .with_target(false)
            .json()
            .try_init()
            .map_err(|error| anyhow!(error.to_string()))?,
    }
    info!(
        event = schema::event::RUNTIME_TELEMETRY_INITIALIZED,
        service_name = config.resource.service_name.as_str(),
        deployment_environment = config.resource.deployment_environment.as_str(),
        service_instance_id = %config.resource.resolved_instance_id(process_id),
        log_format = config.log_format.as_str(),
        common_fields = ?schema::COMMON_FIELD_NAMES,
        correlation_fields = ?schema::CORRELATION_FIELD_NAMES,
        trace_export_otlp_endpoint = config
            .trace_export
            .otlp_endpoint
            .as_deref()
            .unwrap_or("disabled"),
        "initialized runtime telemetry"
    );
    Ok(())
}

fn default_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_error| EnvFilter::new(DEFAULT_ENV_FILTER))
}
