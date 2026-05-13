pub const DEFAULT_TELEMETRY_SERVICE_NAME: &str = "o-sfu";
pub const DEFAULT_TELEMETRY_DEPLOYMENT_ENVIRONMENT: &str = "local";

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
