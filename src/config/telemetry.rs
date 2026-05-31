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
mod tests {
    #[cfg(feature = "otel-tracing")]
    use std::time::Duration;

    #[cfg(feature = "otel-tracing")]
    use super::{TelemetryConfig, TelemetryLogFormat, TraceExportConfig};
    use super::{TelemetryResource, load_telemetry_config};

    #[test]
    fn telemetry_resource_resolves_process_fallback_instance_id() {
        let resource = TelemetryResource::default();
        assert_eq!(resource.resolved_instance_id(17), "pid-17");
    }

    #[cfg(feature = "otel-tracing")]
    #[test]
    fn load_telemetry_config_accepts_explicit_settings() {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_LOG_FORMAT" => Some("json".to_owned()),
            "TELEMETRY_SERVICE_NAME" => Some("custom-o-sfu".to_owned()),
            "TELEMETRY_DEPLOYMENT_ENVIRONMENT" => Some("staging".to_owned()),
            "TELEMETRY_SERVICE_INSTANCE_ID" => Some("node-a-1".to_owned()),
            "TELEMETRY_OTLP_ENDPOINT" => Some("http://collector:4317".to_owned()),
            _ => None,
        });
        assert_eq!(
            config.ok(),
            Some(TelemetryConfig {
                log_format: TelemetryLogFormat::Json,
                resource: TelemetryResource {
                    service_name: "custom-o-sfu".to_owned(),
                    deployment_environment: "staging".to_owned(),
                    service_instance_id: Some("node-a-1".to_owned()),
                },
                trace_export: TraceExportConfig {
                    otlp_endpoint: Some("http://collector:4317".to_owned()),
                },
                media_quality_interval: Some(Duration::from_secs(5)),
            })
        );
    }

    #[cfg(not(feature = "otel-tracing"))]
    #[test]
    fn load_telemetry_config_rejects_otlp_without_feature() {
        let error = load_telemetry_config(|key| match key {
            "TELEMETRY_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("TELEMETRY_OTLP_ENDPOINT requires the `otel-tracing` cargo feature")
        );
    }

    #[cfg(not(feature = "otel-tracing"))]
    #[test]
    fn load_telemetry_config_reports_otlp_feature_error_before_later_errors() {
        let error = load_telemetry_config(|key| match key {
            "TELEMETRY_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("abc".to_owned()),
            "TELEMETRY_SERVICE_NAME" => Some("   ".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("TELEMETRY_OTLP_ENDPOINT requires the `otel-tracing` cargo feature")
        );
    }

    #[cfg(feature = "otel-tracing")]
    #[test]
    fn load_telemetry_config_trims_otlp_endpoint() {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_OTLP_ENDPOINT" => Some("  http://collector:4317  ".to_owned()),
            _ => None,
        });

        assert_eq!(
            config
                .ok()
                .and_then(|config| config.trace_export.otlp_endpoint),
            Some("http://collector:4317".to_owned())
        );
    }

    #[test]
    fn load_telemetry_config_rejects_invalid_log_format() {
        let error = load_telemetry_config(|key| match key {
            "TELEMETRY_LOG_FORMAT" => Some("pretty".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("TELEMETRY_LOG_FORMAT must be either `compact` or `json`")
        );
    }

    #[test]
    fn load_telemetry_config_rejects_empty_service_name() {
        let error = load_telemetry_config(|key| match key {
            "TELEMETRY_SERVICE_NAME" => Some("   ".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("TELEMETRY_SERVICE_NAME must not be empty")
        );
    }

    #[test]
    fn load_telemetry_config_trims_resource_fields() -> anyhow::Result<()> {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_SERVICE_NAME" => Some("  o-sfu-custom  ".to_owned()),
            "TELEMETRY_DEPLOYMENT_ENVIRONMENT" => Some("  staging  ".to_owned()),
            "TELEMETRY_SERVICE_INSTANCE_ID" => Some("  node-a  ".to_owned()),
            _ => None,
        })?;
        assert_eq!(config.resource.service_name, "o-sfu-custom");
        assert_eq!(config.resource.deployment_environment, "staging");
        assert_eq!(
            config.resource.service_instance_id.as_deref(),
            Some("node-a")
        );
        Ok(())
    }

    #[test]
    fn load_telemetry_config_reports_interval_parse_error() {
        let error = load_telemetry_config(|key| match key {
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("abc".to_owned()),
            _ => None,
        })
        .err()
        .map(|error| error.to_string());

        assert_eq!(
            error.as_deref(),
            Some("TELEMETRY_MEDIA_QUALITY_INTERVAL_MS must be a valid u64")
        );
    }

    #[test]
    fn load_telemetry_config_allows_disabling_media_quality_sampling() {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS" => Some("0".to_owned()),
            _ => None,
        });

        assert_eq!(
            config.ok().and_then(|config| config.media_quality_interval),
            None
        );
    }
}
