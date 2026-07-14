use std::{
    future::pending,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Weak},
    time::Duration,
};

use tokio::{
    task::yield_now,
    time::{sleep, timeout},
};

use super::{Result, RoomManager, Runtime};
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

#[tokio::test]
async fn cancelling_serve_future_stops_runtime_background_tasks() {
    let runtime = Runtime::new(&test_config());
    assert!(runtime.is_ok());
    let Ok(runtime) = runtime else {
        return;
    };
    let rooms = Arc::downgrade(&runtime.room_manager);
    let server = tokio::spawn(runtime.serve(|_shutdown_token| pending::<Result<()>>()));

    let task_started = timeout(Duration::from_secs(1), wait_for_runtime_task_start(&rooms)).await;
    assert!(task_started.is_ok());

    server.abort();
    assert!(server.await.is_err());

    let room_manager_dropped =
        timeout(Duration::from_secs(1), wait_for_room_manager_drop(&rooms)).await;
    assert!(room_manager_dropped.is_ok());
}

async fn wait_for_runtime_task_start(rooms: &Weak<RoomManager>) {
    loop {
        if rooms.strong_count() > 1 {
            return;
        }
        yield_now().await;
    }
}

async fn wait_for_room_manager_drop(rooms: &Weak<RoomManager>) {
    loop {
        if rooms.upgrade().is_none() {
            return;
        }
        sleep(Duration::from_millis(1)).await;
    }
}

fn test_config() -> Config {
    Config {
        auth: AuthConfig {
            key: "dGVzdC1rZXk=".to_owned(),
            authentication_timeout_ms: 1_000,
            max_pre_auth_websocket_sessions: 512,
            max_pre_auth_websocket_sessions_per_origin: 16,
        },
        http: HttpConfig {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            trust_proxy_headers: false,
        },
        user: UserConfig {
            room_size: 10,
            timeout_ms: 1_000,
            ping_interval_ms: 60_000,
            outbound_queue_capacity: DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            outbound_queue_byte_capacity: DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        },
        transport: TransportConfig {
            announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            max_bitrate_in: Bitrate::from_mbps(8),
            max_bitrate_out: Bitrate::from_mbps(10),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(41_000, 41_009),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
            rtc_media_worker_count: 1,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            room_media_limits: RoomMediaLimits::default(),
        },
        codecs: CodecConfig {
            flags: MediaCodecFlags::default(),
            preferences: CodecPreferences::default(),
        },
        features: RuntimeFeatureFlags::default(),
        telemetry: TelemetryConfig::default(),
        diagnostics: DiagnosticsConfig::default(),
    }
}
