use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, ensure};

use crate::signaling::DEFAULT_AUTHENTICATION_TIMEOUT_MS;

const DEFAULT_CHANNEL_SIZE: usize = 100;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_PING_INTERVAL_MS: u64 = 60_000;
const DEFAULT_RTC_MIN_PORT: u16 = 40_000;
const DEFAULT_RTC_MAX_PORT: u16 = 49_999;
const DEFAULT_RTC_MEDIA_WORKER_COUNT: usize = 1;
const DEFAULT_ENABLE_TRANSCRIPTION_FEATURE: bool = false;
const DEFAULT_ENABLE_AUDIO_RECORDING_FEATURE: bool = false;
const DEFAULT_ENABLE_VIDEO_RECORDING_FEATURE: bool = false;
const DEFAULT_TRUST_PROXY_HEADERS: bool = false;
const TRANSPORT_BACKEND_FAKE: &str = "fake";
const TRANSPORT_BACKEND_RTC: &str = "rtc";
const FAKE_PUBLIC_IP_DEFAULT: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeFeatureFlags {
    pub transcription: bool,
    pub audio_recording: bool,
    pub video_recording: bool,
}

impl Default for RuntimeFeatureFlags {
    fn default() -> Self {
        Self {
            transcription: DEFAULT_ENABLE_TRANSCRIPTION_FEATURE,
            audio_recording: DEFAULT_ENABLE_AUDIO_RECORDING_FEATURE,
            video_recording: DEFAULT_ENABLE_VIDEO_RECORDING_FEATURE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaCodecFlags {
    enabled: u16,
}

impl MediaCodecFlags {
    const OPUS: u16 = 1 << 0;
    const PCMU: u16 = 1 << 1;
    const PCMA: u16 = 1 << 2;
    const VP8: u16 = 1 << 3;
    const H264: u16 = 1 << 4;
    const H265: u16 = 1 << 5;
    const VP9: u16 = 1 << 6;
    const AV1: u16 = 1 << 7;

    #[must_use]
    const fn with_flag(mut self, flag: u16, enabled: bool) -> Self {
        if enabled {
            self.enabled |= flag;
        } else {
            self.enabled &= !flag;
        }
        self
    }

    #[must_use]
    const fn flag_enabled(self, flag: u16) -> bool {
        self.enabled & flag != 0
    }

    #[must_use]
    pub const fn opus_enabled(self) -> bool {
        self.flag_enabled(Self::OPUS)
    }

    #[must_use]
    pub const fn with_opus(self, enabled: bool) -> Self {
        self.with_flag(Self::OPUS, enabled)
    }

    #[must_use]
    pub const fn pcmu_enabled(self) -> bool {
        self.flag_enabled(Self::PCMU)
    }

    #[must_use]
    pub const fn with_pcmu(self, enabled: bool) -> Self {
        self.with_flag(Self::PCMU, enabled)
    }

    #[must_use]
    pub const fn pcma_enabled(self) -> bool {
        self.flag_enabled(Self::PCMA)
    }

    #[must_use]
    pub const fn with_pcma(self, enabled: bool) -> Self {
        self.with_flag(Self::PCMA, enabled)
    }

    #[must_use]
    pub const fn vp8_enabled(self) -> bool {
        self.flag_enabled(Self::VP8)
    }

    #[must_use]
    pub const fn with_vp8(self, enabled: bool) -> Self {
        self.with_flag(Self::VP8, enabled)
    }

    #[must_use]
    pub const fn h264_enabled(self) -> bool {
        self.flag_enabled(Self::H264)
    }

    #[must_use]
    pub const fn with_h264(self, enabled: bool) -> Self {
        self.with_flag(Self::H264, enabled)
    }

    #[must_use]
    pub const fn h265_enabled(self) -> bool {
        self.flag_enabled(Self::H265)
    }

    #[must_use]
    pub const fn with_h265(self, enabled: bool) -> Self {
        self.with_flag(Self::H265, enabled)
    }

    #[must_use]
    pub const fn vp9_enabled(self) -> bool {
        self.flag_enabled(Self::VP9)
    }

    #[must_use]
    pub const fn with_vp9(self, enabled: bool) -> Self {
        self.with_flag(Self::VP9, enabled)
    }

    #[must_use]
    pub const fn av1_enabled(self) -> bool {
        self.flag_enabled(Self::AV1)
    }

    #[must_use]
    pub const fn with_av1(self, enabled: bool) -> Self {
        self.with_flag(Self::AV1, enabled)
    }
}

impl Default for MediaCodecFlags {
    fn default() -> Self {
        Self { enabled: 0 }.with_opus(true).with_vp8(true)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtcPortRange {
    min: u16,
    max: u16,
}

impl RtcPortRange {
    #[must_use]
    pub const fn new(min: u16, max: u16) -> Self {
        Self { min, max }
    }

    #[must_use]
    pub const fn min(self) -> u16 {
        self.min
    }

    #[must_use]
    pub const fn max(self) -> u16 {
        self.max
    }

    #[must_use]
    pub const fn port_count(self) -> u16 {
        self.max - self.min + 1
    }

    pub fn ports(self) -> impl Iterator<Item = u16> {
        self.min..=self.max
    }

    #[must_use]
    pub fn split_for_workers(self, worker_count: usize) -> Option<Vec<Self>> {
        if worker_count == 0 || worker_count > usize::from(self.port_count()) {
            return None;
        }
        let total_ports = usize::from(self.port_count());
        let base_ports_per_worker = total_ports / worker_count;
        let extra_ports = total_ports % worker_count;
        let mut next_min = u32::from(self.min);
        let mut ranges = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let worker_port_count = base_ports_per_worker + usize::from(worker_idx < extra_ports);
            let worker_port_count = u32::try_from(worker_port_count).ok()?;
            let max_inclusive = next_min + worker_port_count - 1;
            ranges.push(Self::new(
                u16::try_from(next_min).ok()?,
                u16::try_from(max_inclusive).ok()?,
            ));
            next_min = max_inclusive + 1;
        }
        Some(ranges)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportBackend {
    Fake,
    Rtc,
}

impl FromStr for TransportBackend {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            TRANSPORT_BACKEND_FAKE => Ok(Self::Fake),
            TRANSPORT_BACKEND_RTC => Ok(Self::Rtc),
            _ => Err(anyhow!(
                "TRANSPORT_BACKEND must be either `{TRANSPORT_BACKEND_FAKE}` or `{TRANSPORT_BACKEND_RTC}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub auth_key: String,
    pub bind_address: SocketAddr,
    pub authentication_timeout_ms: u64,
    pub channel_size: usize,
    pub session_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub trust_proxy_headers: bool,
    pub feature_flags: RuntimeFeatureFlags,
    pub codec_flags: MediaCodecFlags,
    pub public_ip: IpAddr,
    pub rtc_port_range: RtcPortRange,
    pub rtc_media_worker_count: usize,
    pub transport_backend: TransportBackend,
}

impl Config {
    /// # Errors
    ///
    /// Returns an error when `AUTH_KEY` is missing, `BIND_ADDRESS` is invalid,
    /// `AUTHENTICATION_TIMEOUT_MS` is invalid, `CHANNEL_SIZE` is zero,
    /// `SESSION_TIMEOUT_MS` is invalid, `PING_INTERVAL_MS` is invalid, `PROXY`
    /// is invalid, `PUBLIC_IP` is invalid, `RTC_MIN_PORT`/`RTC_MAX_PORT` are
    /// invalid, or `TRANSPORT_BACKEND` is invalid.
    pub fn from_env() -> Result<Self> {
        Self::from_var_lookup(|key| env::var(key).ok())
    }

    fn from_var_lookup(mut get_var: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let bind_address = get_var("BIND_ADDRESS")
            .unwrap_or_else(|| "0.0.0.0:8080".to_owned())
            .parse()
            .context("BIND_ADDRESS must be a valid socket address")?;
        let auth_key = get_var("AUTH_KEY").context("AUTH_KEY env variable is required")?;
        let authentication_timeout_ms = parse_optional_env(
            &mut get_var,
            "AUTHENTICATION_TIMEOUT_MS",
            "AUTHENTICATION_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_AUTHENTICATION_TIMEOUT_MS);
        let channel_size = parse_optional_env(
            &mut get_var,
            "CHANNEL_SIZE",
            "CHANNEL_SIZE must be a valid usize",
        )?
        .unwrap_or(DEFAULT_CHANNEL_SIZE);
        let session_timeout_ms = parse_optional_env(
            &mut get_var,
            "SESSION_TIMEOUT_MS",
            "SESSION_TIMEOUT_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_SESSION_TIMEOUT_MS);
        let ping_interval_ms = parse_optional_env(
            &mut get_var,
            "PING_INTERVAL_MS",
            "PING_INTERVAL_MS must be a valid u64",
        )?
        .unwrap_or(DEFAULT_PING_INTERVAL_MS);
        let trust_proxy_headers = parse_optional_env(
            &mut get_var,
            "PROXY",
            "PROXY must be either `true` or `false`",
        )?
        .unwrap_or(DEFAULT_TRUST_PROXY_HEADERS);
        let feature_flags = load_runtime_feature_flags(&mut get_var)?;
        let codec_flags = load_media_codec_flags(&mut get_var)?;
        let (public_ip, rtc_port_range, rtc_media_worker_count, transport_backend) =
            load_transport_config(&mut get_var)?;
        ensure!(channel_size > 0, "CHANNEL_SIZE must be greater than zero");
        ensure!(
            session_timeout_ms > 0,
            "SESSION_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            ping_interval_ms > 0,
            "PING_INTERVAL_MS must be greater than zero"
        );
        Ok(Self {
            auth_key,
            bind_address,
            authentication_timeout_ms,
            channel_size,
            session_timeout_ms,
            ping_interval_ms,
            trust_proxy_headers,
            feature_flags,
            codec_flags,
            public_ip,
            rtc_port_range,
            rtc_media_worker_count,
            transport_backend,
        })
    }
}

fn load_runtime_feature_flags(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<RuntimeFeatureFlags> {
    Ok(RuntimeFeatureFlags {
        transcription: parse_optional_env(
            &mut get_var,
            "ENABLE_FEATURE_TRANSCRIPTION",
            "ENABLE_FEATURE_TRANSCRIPTION must be either `true` or `false`",
        )?
        .unwrap_or(DEFAULT_ENABLE_TRANSCRIPTION_FEATURE),
        audio_recording: parse_optional_env(
            &mut get_var,
            "ENABLE_FEATURE_AUDIO_RECORDING",
            "ENABLE_FEATURE_AUDIO_RECORDING must be either `true` or `false`",
        )?
        .unwrap_or(DEFAULT_ENABLE_AUDIO_RECORDING_FEATURE),
        video_recording: parse_optional_env(
            &mut get_var,
            "ENABLE_FEATURE_VIDEO_RECORDING",
            "ENABLE_FEATURE_VIDEO_RECORDING must be either `true` or `false`",
        )?
        .unwrap_or(DEFAULT_ENABLE_VIDEO_RECORDING_FEATURE),
    })
}

fn load_media_codec_flags(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<MediaCodecFlags> {
    let default_flags = MediaCodecFlags::default();
    let opus = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_OPUS",
        "ENABLE_CODEC_OPUS must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.opus_enabled());
    let g711_mu_law_enabled = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_PCMU",
        "ENABLE_CODEC_PCMU must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.pcmu_enabled());
    let g711_a_law_enabled = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_PCMA",
        "ENABLE_CODEC_PCMA must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.pcma_enabled());
    let vp8 = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_VP8",
        "ENABLE_CODEC_VP8 must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.vp8_enabled());
    let h264 = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_H264",
        "ENABLE_CODEC_H264 must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.h264_enabled());
    let h265 = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_H265",
        "ENABLE_CODEC_H265 must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.h265_enabled());
    let vp9 = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_VP9",
        "ENABLE_CODEC_VP9 must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.vp9_enabled());
    let av1 = parse_optional_env(
        &mut get_var,
        "ENABLE_CODEC_AV1",
        "ENABLE_CODEC_AV1 must be either `true` or `false`",
    )?
    .unwrap_or(default_flags.av1_enabled());
    Ok(MediaCodecFlags::default()
        .with_opus(opus)
        .with_pcmu(g711_mu_law_enabled)
        .with_pcma(g711_a_law_enabled)
        .with_vp8(vp8)
        .with_h264(h264)
        .with_h265(h265)
        .with_vp9(vp9)
        .with_av1(av1))
}

fn load_transport_config(
    mut get_var: impl FnMut(&str) -> Option<String>,
) -> Result<(IpAddr, RtcPortRange, usize, TransportBackend)> {
    let public_ip = parse_optional_env(
        &mut get_var,
        "PUBLIC_IP",
        "PUBLIC_IP must be a valid IP address",
    )?;
    let rtc_min_port = parse_optional_env(
        &mut get_var,
        "RTC_MIN_PORT",
        "RTC_MIN_PORT must be a valid u16",
    )?
    .unwrap_or(DEFAULT_RTC_MIN_PORT);
    let rtc_max_port = parse_optional_env(
        &mut get_var,
        "RTC_MAX_PORT",
        "RTC_MAX_PORT must be a valid u16",
    )?
    .unwrap_or(DEFAULT_RTC_MAX_PORT);
    let rtc_media_worker_count = parse_optional_env(
        &mut get_var,
        "RTC_MEDIA_WORKER_COUNT",
        "RTC_MEDIA_WORKER_COUNT must be a valid usize",
    )?
    .unwrap_or(DEFAULT_RTC_MEDIA_WORKER_COUNT);
    let transport_backend = parse_optional_env(
        &mut get_var,
        "TRANSPORT_BACKEND",
        "TRANSPORT_BACKEND must be either `fake` or `rtc`",
    )?
    .unwrap_or(TransportBackend::Fake);
    ensure!(
        rtc_min_port <= rtc_max_port,
        "RTC_MAX_PORT must be greater than or equal to RTC_MIN_PORT"
    );
    ensure!(
        rtc_media_worker_count > 0,
        "RTC_MEDIA_WORKER_COUNT must be greater than zero"
    );
    let rtc_port_range = RtcPortRange::new(rtc_min_port, rtc_max_port);
    ensure!(
        rtc_media_worker_count <= usize::from(rtc_port_range.port_count()),
        "RTC_MEDIA_WORKER_COUNT must be less than or equal to the available RTC port count"
    );
    let public_ip = match (transport_backend, public_ip) {
        (_, Some(public_ip)) => public_ip,
        (TransportBackend::Fake, None) => FAKE_PUBLIC_IP_DEFAULT,
        (TransportBackend::Rtc, None) => {
            return Err(anyhow!(
                "PUBLIC_IP env variable is required when TRANSPORT_BACKEND=rtc"
            ));
        }
    };
    ensure!(
        transport_backend != TransportBackend::Rtc || !public_ip.is_unspecified(),
        "PUBLIC_IP must be a concrete advertised address when TRANSPORT_BACKEND=rtc"
    );
    ensure!(
        transport_backend != TransportBackend::Rtc || !public_ip.is_multicast(),
        "PUBLIC_IP cannot be a multicast address when TRANSPORT_BACKEND=rtc"
    );
    Ok((
        public_ip,
        rtc_port_range,
        rtc_media_worker_count,
        transport_backend,
    ))
}

fn parse_optional_env<T>(
    mut get_var: impl FnMut(&str) -> Option<String>,
    key: &str,
    error_message: &str,
) -> Result<Option<T>>
where
    T: FromStr,
{
    get_var(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_error| anyhow!(error_message.to_owned()))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::{
        Config, FAKE_PUBLIC_IP_DEFAULT, MediaCodecFlags, RtcPortRange, RuntimeFeatureFlags,
        TransportBackend,
    };

    #[test]
    fn config_requires_auth_key() {
        let error = Config::from_var_lookup(|_| None).err();
        assert!(error.is_some());
        let Some(error) = error else {
            return;
        };
        assert!(error.to_string().contains("AUTH_KEY"));
    }

    #[test]
    fn config_uses_defaults_and_explicit_values() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.bind_address.to_string(), "0.0.0.0:8080");
        assert_eq!(config.auth_key, "dGVzdC1rZXk=");
        assert_eq!(config.authentication_timeout_ms, 10_000);
        assert_eq!(config.channel_size, 100);
        assert_eq!(config.session_timeout_ms, 10_000);
        assert_eq!(config.ping_interval_ms, 60_000);
        assert!(!config.trust_proxy_headers);
        assert_eq!(config.feature_flags, RuntimeFeatureFlags::default());
        assert_eq!(config.codec_flags, MediaCodecFlags::default());
        assert_eq!(config.public_ip, FAKE_PUBLIC_IP_DEFAULT);
        assert_eq!(config.rtc_port_range, RtcPortRange::new(40_000, 49_999));
        assert_eq!(config.rtc_media_worker_count, 1);
        assert_eq!(config.transport_backend, TransportBackend::Fake);
    }

    #[test]
    fn config_accepts_feature_flags() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "ENABLE_FEATURE_TRANSCRIPTION"
            | "ENABLE_FEATURE_AUDIO_RECORDING"
            | "ENABLE_FEATURE_VIDEO_RECORDING" => Some("true".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(
            config.feature_flags,
            RuntimeFeatureFlags {
                transcription: true,
                audio_recording: true,
                video_recording: true,
            }
        );
    }

    #[test]
    fn config_accepts_proxy_flag() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PROXY" => Some("true".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert!(config.trust_proxy_headers);
    }

    #[test]
    fn config_accepts_codec_flags() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "ENABLE_CODEC_OPUS" => Some("false".to_owned()),
            "ENABLE_CODEC_H264" | "ENABLE_CODEC_AV1" => Some("true".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(
            config.codec_flags,
            MediaCodecFlags::default()
                .with_opus(false)
                .with_h264(true)
                .with_av1(true)
        );
    }

    #[test]
    fn config_rejects_zero_channel_size() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "CHANNEL_SIZE" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_session_timeout() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "SESSION_TIMEOUT_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_ping_interval() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PING_INTERVAL_MS" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_accepts_rtc_transport_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("203.0.113.10".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_ok());
        let Some(config) = config.ok() else {
            return;
        };
        assert_eq!(config.transport_backend, TransportBackend::Rtc);
    }

    #[test]
    fn config_rejects_removed_stub_transport_backend_alias() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("stub".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_requires_public_ip_for_rtc_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_unspecified_public_ip_for_rtc_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("0.0.0.0".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_multicast_public_ip_for_rtc_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "PUBLIC_IP" => Some("239.1.1.1".to_owned()),
            "TRANSPORT_BACKEND" => Some("rtc".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_unknown_transport_backend() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "TRANSPORT_BACKEND" => Some("unknown".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_inverted_rtc_port_range() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MIN_PORT" => Some("5000".to_owned()),
            "RTC_MAX_PORT" => Some("4000".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_zero_rtc_media_worker_count() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("0".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_more_rtc_workers_than_ports() {
        let config = Config::from_var_lookup(|key| match key {
            "AUTH_KEY" => Some("dGVzdC1rZXk=".to_owned()),
            "RTC_MIN_PORT" => Some("4000".to_owned()),
            "RTC_MAX_PORT" => Some("4001".to_owned()),
            "RTC_MEDIA_WORKER_COUNT" => Some("3".to_owned()),
            _ => None,
        });
        assert!(config.is_err());
    }

    #[test]
    fn rtc_port_range_splits_ports_across_workers() {
        let ranges = RtcPortRange::new(40_000, 40_004).split_for_workers(3);
        assert_eq!(
            ranges,
            Some(vec![
                RtcPortRange::new(40_000, 40_001),
                RtcPortRange::new(40_002, 40_003),
                RtcPortRange::new(40_004, 40_004),
            ])
        );
    }
}
