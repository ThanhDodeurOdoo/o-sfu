use std::time::Duration;

use anyhow::{Result, anyhow};
pub use o_sfu_telemetry::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT,
    DEFAULT_TELEMETRY_SERVICE_NAME, TelemetryConfig, TelemetryLogFormat, TelemetryResource,
    TraceExportConfig,
};

use super::env::{EnvParse, env_block, non_empty};

const OTEL_TRACING_FEATURE_NAME: &str = "otel-tracing";
const MEDIA_QUALITY_INTERVAL_ENV: &str = "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS";

impl EnvParse for TelemetryLogFormat {
    fn parse(key: &'static str, value: String) -> Result<Self> {
        match value.as_str() {
            "compact" => Ok(Self::Compact),
            "json" => Ok(Self::Json),
            _ => Err(anyhow!("{key} must be either `compact` or `json`")),
        }
    }
}

env_block! {
    struct TelemetryExportEnv {
        log_format: TelemetryLogFormat = default(
            "TELEMETRY_LOG_FORMAT",
            TelemetryLogFormat::default()
        );
        otlp_endpoint: Option<String> = optional("TELEMETRY_OTLP_ENDPOINT").check(non_empty);
    }
}

env_block! {
    struct TelemetryRuntimeEnv {
        media_quality_interval_ms: u64 = default(
            MEDIA_QUALITY_INTERVAL_ENV,
            u64::try_from(DEFAULT_MEDIA_QUALITY_INTERVAL.as_millis()).unwrap_or(5_000)
        );
        service_name: Option<String> = optional("TELEMETRY_SERVICE_NAME").check(non_empty);
        deployment_environment: Option<String> =
            optional("TELEMETRY_DEPLOYMENT_ENVIRONMENT").check(non_empty);
        service_instance_id: Option<String> =
            optional("TELEMETRY_SERVICE_INSTANCE_ID").check(non_empty);
    }
}

pub(super) fn load_telemetry_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<TelemetryConfig> {
    let export = TelemetryExportEnv::load(&mut get_var)?;
    if !cfg!(feature = "otel-tracing") && export.otlp_endpoint.is_some() {
        return Err(anyhow!(
            "TELEMETRY_OTLP_ENDPOINT requires the `{OTEL_TRACING_FEATURE_NAME}` cargo feature"
        ));
    }
    let runtime = TelemetryRuntimeEnv::load(get_var)?;
    Ok(TelemetryConfig {
        log_format: export.log_format,
        resource: TelemetryResource {
            service_name: runtime
                .service_name
                .unwrap_or_else(|| DEFAULT_TELEMETRY_SERVICE_NAME.to_owned()),
            deployment_environment: runtime
                .deployment_environment
                .unwrap_or_else(|| DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT.to_owned()),
            service_instance_id: runtime.service_instance_id,
        },
        trace_export: TraceExportConfig {
            otlp_endpoint: export.otlp_endpoint,
        },
        media_quality_interval: (runtime.media_quality_interval_ms > 0)
            .then(|| Duration::from_millis(runtime.media_quality_interval_ms)),
    })
}

#[cfg(test)]
#[path = "TESTS/telemetry.rs"]
mod tests;
