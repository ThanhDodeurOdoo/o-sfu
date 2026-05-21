use std::time::Duration;

use anyhow::{Result, anyhow};
pub use o_sfu_telemetry::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT,
    DEFAULT_TELEMETRY_SERVICE_NAME, TelemetryConfig, TelemetryLogFormat, TelemetryResource,
    TraceExportConfig,
};

use super::{
    log_view::ConfigLogField,
    parsing::{parse_env_or_default, parse_optional_non_empty_env},
};

const OTEL_TRACING_FEATURE_NAME: &str = "otel-tracing";
const MEDIA_QUALITY_INTERVAL_ENV: &str = "TELEMETRY_MEDIA_QUALITY_INTERVAL_MS";

pub(super) fn load_telemetry_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<TelemetryConfig> {
    let log_format = match get_var("TELEMETRY_LOG_FORMAT") {
        Some(value) => match value.as_str() {
            "compact" => TelemetryLogFormat::Compact,
            "json" => TelemetryLogFormat::Json,
            _ => {
                return Err(anyhow!(
                    "TELEMETRY_LOG_FORMAT must be either `compact` or `json`"
                ));
            }
        },
        None => TelemetryLogFormat::default(),
    };
    let otlp_endpoint = parse_optional_non_empty_env(&mut get_var, "TELEMETRY_OTLP_ENDPOINT")?;
    if !cfg!(feature = "otel-tracing") && otlp_endpoint.is_some() {
        return Err(anyhow!(
            "TELEMETRY_OTLP_ENDPOINT requires the `{OTEL_TRACING_FEATURE_NAME}` cargo feature"
        ));
    }
    let media_quality_interval_ms = parse_env_or_default(
        &mut get_var,
        MEDIA_QUALITY_INTERVAL_ENV,
        u64::try_from(DEFAULT_MEDIA_QUALITY_INTERVAL.as_millis()).unwrap_or(5_000),
    )?;
    Ok(TelemetryConfig {
        log_format,
        resource: TelemetryResource {
            service_name: parse_optional_non_empty_env(&mut get_var, "TELEMETRY_SERVICE_NAME")?
                .unwrap_or_else(|| DEFAULT_TELEMETRY_SERVICE_NAME.to_owned()),
            deployment_environment: parse_optional_non_empty_env(
                &mut get_var,
                "TELEMETRY_DEPLOYMENT_ENVIRONMENT",
            )?
            .unwrap_or_else(|| DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT.to_owned()),
            service_instance_id: parse_optional_non_empty_env(
                &mut get_var,
                "TELEMETRY_SERVICE_INSTANCE_ID",
            )?,
        },
        trace_export: TraceExportConfig { otlp_endpoint },
        media_quality_interval: (media_quality_interval_ms > 0)
            .then(|| Duration::from_millis(media_quality_interval_ms)),
    })
}

#[must_use]
pub(super) fn telemetry_log_fields(
    config: &TelemetryConfig,
    process_id: u32,
) -> [ConfigLogField; 5] {
    [
        ConfigLogField::new("service_name", config.resource.service_name.as_str()),
        ConfigLogField::new(
            "deployment_environment",
            config.resource.deployment_environment.as_str(),
        ),
        ConfigLogField::new(
            "service_instance_id",
            config.resource.resolved_instance_id(process_id),
        ),
        ConfigLogField::new("log_format", config.log_format.as_str()),
        ConfigLogField::new(
            "trace_export_otlp_endpoint",
            config
                .trace_export
                .otlp_endpoint
                .as_deref()
                .unwrap_or("disabled"),
        ),
    ]
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
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_OTLP_ENDPOINT" => Some("http://collector:4318".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
        let Some(error) = config.err() else {
            return;
        };
        assert!(
            error
                .to_string()
                .contains("TELEMETRY_OTLP_ENDPOINT requires the `otel-tracing` cargo feature")
        );
    }

    #[test]
    fn load_telemetry_config_rejects_invalid_log_format() {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_LOG_FORMAT" => Some("pretty".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn load_telemetry_config_rejects_empty_service_name() {
        let config = load_telemetry_config(|key| match key {
            "TELEMETRY_SERVICE_NAME" => Some("   ".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
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
