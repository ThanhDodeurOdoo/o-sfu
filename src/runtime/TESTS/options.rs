use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::RuntimeOptions;
use crate::{
    config::{
        AuthConfig, Bitrate, CodecConfig, CodecPreferences, Config, DiagnosticsConfig, HttpConfig,
        MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy, RtcPortRange, RtcUdpIoBackend,
        RuntimeFeatureFlags, TelemetryConfig, TransportConfig, UserConfig, VideoBitrateLimits,
    },
    core::server::room::{
        DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
    },
};

fn test_config() -> Config {
    Config {
        auth: AuthConfig {
            key: "dGVzdC1rZXk=".to_owned(),
            authentication_timeout_ms: 1_500,
            max_pre_auth_websocket_sessions: 512,
            max_pre_auth_websocket_sessions_per_origin: 16,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8090)),
            trust_proxy_headers: true,
        },
        user: UserConfig {
            room_size: 42,
            timeout_ms: 7_000,
            ping_interval_ms: 11_000,
            outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        },
        transport: TransportConfig {
            announced_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            max_bitrate_in: Bitrate::from_kbps(1_234),
            max_bitrate_out: Bitrate::from_kbps(5_678),
            video_bitrate_limits: VideoBitrateLimits::new(Bitrate::from_kbps(4_321)),
            rtc_port_range: RtcPortRange::new(50_000, 50_099),
            rtc_udp_io_backend: RtcUdpIoBackend::IoUring,
            rtc_media_worker_count: 4,
            room_worker_policy: RoomWorkerPolicy::bounded_local_spillover(2),
            room_media_limits: RoomMediaLimits::default(),
        },
        codecs: CodecConfig {
            flags: MediaCodecFlags::default().with_h264(true),
            preferences: CodecPreferences::default(),
        },
        features: RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: false,
        },
        telemetry: TelemetryConfig::default(),
        diagnostics: DiagnosticsConfig {
            auth_token: Some("operator-secret".to_owned()),
        },
    }
}

#[test]
fn runtime_options_project_core_settings() {
    let config = test_config();

    let options = RuntimeOptions::from_config(&config);

    assert_eq!(
        options.core.media.announced_ip,
        config.transport.announced_ip
    );
    assert_eq!(
        options.core.media.rtc_port_range,
        config.transport.rtc_port_range
    );
    assert_eq!(
        options.core.media.rtc_udp_io_backend,
        config.transport.rtc_udp_io_backend
    );
    assert_eq!(
        options.core.media.bitrate_limits.max_bitrate_in(),
        config.transport.max_bitrate_in
    );
    assert_eq!(
        options.core.media.video_bitrate_limits,
        config.transport.video_bitrate_limits
    );
    assert_eq!(options.core.routing.media_worker_count, 4);
    assert_eq!(
        options.core.routing.room_worker_policy.max_local_routers(),
        2
    );
    assert_eq!(
        options.room_media_limits,
        config.transport.room_media_limits
    );
    assert_eq!(options.core.codecs.flags, config.codecs.flags);
    assert_eq!(options.core.codecs.preferences, config.codecs.preferences);
    assert_eq!(
        options.core.observability.media_quality_interval,
        config.telemetry.media_quality_interval
    );
}

#[test]
fn effective_feature_flags_disable_transcription_without_recording() {
    let mut config = test_config();
    config.features = RuntimeFeatureFlags {
        transcription: true,
        audio_recording: false,
        video_recording: false,
    };

    let options = RuntimeOptions::from_config(&config);

    assert_eq!(
        options.effective_feature_flags(),
        RuntimeFeatureFlags {
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );

    config.features.audio_recording = true;
    let options = RuntimeOptions::from_config(&config);

    assert_eq!(
        options.effective_feature_flags(),
        RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: false,
        }
    );
}
