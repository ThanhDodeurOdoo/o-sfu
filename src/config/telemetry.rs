use anyhow::{Result, anyhow};

use super::parsing::parse_optional_non_empty_env;

const DEFAULT_TELEMETRY_SERVICE_NAME: &str = "o-sfu";
const DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT: &str = "local";
const OTEL_TRACING_FEATURE_NAME: &str = "otel-tracing";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TelemetryConfig {
    pub log_format: TelemetryLogFormat,
    pub resource: TelemetryResource,
    pub trace_export: TraceExportConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TelemetryLogFormat {
    #[default]
    Compact,
    Json,
}

impl TelemetryLogFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compact => "compact",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryResource {
    pub service_name: String,
    pub deployment_environment: String,
    pub service_instance_id: Option<String>,
}

impl TelemetryResource {
    #[must_use]
    pub fn resolved_instance_id(&self, process_id: u32) -> String {
        self.service_instance_id
            .clone()
            .unwrap_or_else(|| format!("pid-{process_id}"))
    }
}

impl Default for TelemetryResource {
    fn default() -> Self {
        Self {
            service_name: DEFAULT_TELEMETRY_SERVICE_NAME.to_owned(),
            deployment_environment: DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT.to_owned(),
            service_instance_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TraceExportConfig {
    pub otlp_endpoint: Option<String>,
}

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
    })
}

#[cfg(test)]
mod tests {
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
}
