pub(super) use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};

pub(super) use o_sfu_router::{
    MediaKind,
    test_support::rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters,
};
pub(super) use str0m::{Candidate, Rtc, change::SdpOffer};

pub(super) use super::super::{api::NegotiatedPublish, fixtures::*};
pub(super) use crate::{
    Bitrate, MediaCodecFlags, RtcPortRange, SessionBitrateLimits,
    runtime::{
        diagnostics::{
            DiagnosticsSourceSelector, DiagnosticsStore, DiagnosticsVideoLayoutRole,
            DiagnosticsVideoRoutePriority,
        },
        media_transport::{
            MediaTransportDeps, RtcTransport, RtcTransportConfig, SessionOffer, TransportMediaId,
            TransportSessionKey,
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
        room::Room,
    },
};

pub(super) fn assert_track_binding_activity_update(
    message: &UserOutbound,
    user_id: &UserId,
    stream_type: TestSourceKind,
    active: Option<bool>,
) {
    match message {
        UserOutbound::TrackBindingUpdate(update) => {
            assert_eq!(&update.user_id, user_id);
            assert_eq!(update.stream_id, stream_id_for_source(stream_type));
            assert_eq!(update.active, active);
        }
        other => panic!("expected TrackBindingUpdate, got {other:?}"),
    }
}
pub(super) async fn assert_transport_media_mapping_is_missing(
    room: &Arc<Room>,
    transport_media_id: TransportMediaId,
) {
    assert!(
        room.test_api()
            .inspect()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
}

pub(super) async fn assert_transport_media_owner_mapping_is_missing(
    room: &Arc<Room>,
    transport_media_id: TransportMediaId,
) {
    assert!(
        room.test_api()
            .inspect()
            .producer_owner_user_id_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert!(
        room.test_api()
            .inspect()
            .producer_owner_connection_id_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
}

pub(super) async fn assert_user_has_no_producer_route_target(
    room: &Arc<Room>,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
) {
    assert!(
        !room
            .test_api()
            .inspect()
            .has_producer_route_target(user_id, connection_id, stream_type)
            .await
    );
}

pub(super) async fn assert_subscription_layout(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_role: DiagnosticsVideoLayoutRole,
    expected_priority: DiagnosticsVideoRoutePriority,
) {
    let diagnostics = room.diagnostics_user_views(adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.layout_role == Some(expected_role)
                && subscription.layout_priority == Some(expected_priority)
        }),
        "diagnostics should expose the subscription layout role and priority"
    );
}

pub(super) async fn assert_subscription_selected_rid(
    room: &Arc<Room>,
    adapter: &MediaTransport,
    consumer_user_id: &UserId,
    producer_user_id: &UserId,
    stream_type: TestSourceKind,
    expected_rid: &str,
) {
    let diagnostics = room.diagnostics_user_views(adapter).await;
    let Some(user) = diagnostics
        .iter()
        .find(|view| &view.user_id == consumer_user_id)
    else {
        panic!("diagnostics should include the consumer user");
    };
    assert!(
        user.subscriptions.iter().any(|subscription| {
            subscription.producer_user_id == *producer_user_id
                && subscription.stream_id == stream_id_for_source(stream_type).to_string()
                && subscription.selection.selector == DiagnosticsSourceSelector::Encoding
                && subscription.selection.selected_rid.as_deref() == Some(expected_rid)
                && subscription.selection.policy_allows_delivery
        }),
        "diagnostics should expose the selected RID for the subscription: {:?}",
        user.subscriptions
    );
}

pub(super) struct RealRtcRefreshScenario {
    pub(super) room: Arc<Room>,
    pub(super) media_transport: MediaTransport,
    pub(super) publisher_user_id: UserId,
    pub(super) subscriber_user_id: UserId,
    pub(super) publisher_initial_offer: SessionOffer,
    pub(super) subscriber_session_key: TransportSessionKey,
    pub(super) publisher_rx: UserOutboundReceiver,
    pub(super) subscriber_rx: UserOutboundReceiver,
    pub(super) subscriber_remote: Rtc,
}

pub(super) async fn setup_real_rtc_refresh_scenario() -> RealRtcRefreshScenario {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await;
    let (publisher_tx, publisher_rx) = test_sender();
    let (subscriber_tx, subscriber_rx) = test_sender();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let publisher_connection_id = room
        .test_api()
        .lifecycle()
        .join_user(
            publisher_user_id.clone(),
            None,
            UserPermissions::default(),
            publisher_tx,
        )
        .await
        .expect("publisher should join");
    let subscriber_connection_id = room
        .test_api()
        .lifecycle()
        .join_user(
            subscriber_user_id.clone(),
            None,
            UserPermissions::default(),
            subscriber_tx,
        )
        .await
        .expect("subscriber should join");
    let media_transport = build_real_rtc_media_transport();
    let publisher_session_key =
        room.transport_user_key(&publisher_user_id, publisher_connection_id);
    let subscriber_session_key =
        room.transport_user_key(&subscriber_user_id, subscriber_connection_id);

    let publisher_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &publisher_session_key).await;
    let subscriber_initial_offer =
        bootstrap_real_rtc_user(&media_transport, &subscriber_session_key).await;
    let mut subscriber_remote = build_remote_rtc(55_100);
    apply_offer_answer(
        &media_transport,
        &subscriber_session_key,
        &mut subscriber_remote,
        subscriber_initial_offer.into_sdp(),
    )
    .await;

    assert_eq!(
        room.apply_session_negotiated(
            &publisher_user_id,
            publisher_connection_id,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );
    assert_eq!(
        room.apply_session_negotiated(
            &subscriber_user_id,
            subscriber_connection_id,
            test_client_rtp_capabilities(),
            &media_transport,
        )
        .await,
        SessionNegotiationOutcome::Applied
    );

    RealRtcRefreshScenario {
        room,
        media_transport,
        publisher_user_id,
        subscriber_user_id,
        publisher_initial_offer,
        subscriber_session_key,
        publisher_rx,
        subscriber_rx,
        subscriber_remote,
    }
}

pub(super) async fn settle_refresh_offer(
    scenario: &mut RealRtcRefreshScenario,
    offer: SessionOffer,
) {
    apply_offer_answer(
        &scenario.media_transport,
        &scenario.subscriber_session_key,
        &mut scenario.subscriber_remote,
        offer.into_sdp(),
    )
    .await;

    scenario
        .room
        .apply_session_refreshed(
            &scenario.subscriber_user_id,
            user_connection_id(&scenario.room, &scenario.subscriber_user_id).await,
            &scenario.media_transport,
        )
        .await;
}
#[allow(
    clippy::panic,
    reason = "the RTC room test fixture uses a fixed valid configuration and should fail loudly if it stops being valid"
)]
pub(super) fn build_real_rtc_media_transport() -> MediaTransport {
    match RtcTransport::builder()
        .transport_config(RtcTransportConfig {
            public_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: crate::VideoBitrateLimits::default(),
            rtc_port_range: RtcPortRange::new(46_200, 46_299),
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: crate::CodecPreferences::default(),
        })
        .deps(MediaTransportDeps {
            diagnostics: Arc::new(DiagnosticsStore::default()),
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        })
        .worker_count(1)
        .build()
    {
        Ok(transport) => MediaTransport::from_rtc_transport(transport),
        Err(error) => panic!("constant RTC room test transport config should be valid: {error}"),
    }
}

pub(super) async fn bootstrap_real_rtc_user(
    media_transport: &MediaTransport,
    session_key: &TransportSessionKey,
) -> SessionOffer {
    media_transport
        .create_initial_session_offer(session_key)
        .await
        .expect("rtc user should produce an initial offer")
}

pub(super) fn assert_bootstrap_for_stream(messages: &[UserOutbound], stream_type: TestSourceKind) {
    assert!(
        messages.iter().any(|message| matches!(
            message,
            UserOutbound::Request(request)
                if matches!(
                    request.as_ref(),
                    RoomEventRequest::BootstrapRemoteTrack(payload)
                        if payload.stream_id() == &stream_id_for_source(stream_type)
                )
        )),
        "expected a bootstrap request for {stream_type:?}"
    );
}

pub(super) fn build_remote_rtc(port: u16) -> Rtc {
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    remote
}

pub(super) async fn apply_offer_answer(
    adapter: &MediaTransport,
    session_key: &TransportSessionKey,
    remote: &mut Rtc,
    offer_sdp: String,
) {
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&offer_sdp)
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build");
    assert!(
        adapter
            .apply_session_answer(session_key, &answer.to_sdp_string())
            .await
            .is_ok()
    );
}

pub(super) fn video_rtp_parameters_with_mid(mid: &str, ssrc: u32) -> MediaStream {
    router_sample_video_rtp_parameters(Some(mid), ssrc)
}
