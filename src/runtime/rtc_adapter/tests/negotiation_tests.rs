use std::time::Instant;

use o_sfu_router::MediaKind as RouterMediaKind;
use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::Frequency,
};

use super::fixtures::*;
use crate::runtime::rtc_adapter::client_rtp_capabilities_from_answer;
use crate::runtime::transport_adapter::TransportMediaId;

#[tokio::test]
async fn rtc_initial_session_offer_round_trips_through_str0m_answer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 34, SessionId::Integer(34));

    let offer = adapter.create_initial_session_offer(&session_key).await;
    assert!(offer.is_ok());
    let Some(offer) = offer.ok() else {
        return;
    };
    let offer_sdp = offer.into_sdp();
    assert!(offer_sdp.contains("m=audio"));
    assert!(offer_sdp.contains("m=video"));
    assert!(offer_sdp.contains("a=recvonly"));

    let mut remote = Rtc::new(Instant::now());
    assert!(
        remote
            .add_local_candidate(
                Candidate::host(SocketAddr::from(([127, 0, 0, 1], 55_000)), "udp")
                    .expect("test host candidate should build"),
            )
            .is_some()
    );
    let answer = remote.sdp_api().accept_offer(
        SdpOffer::from_sdp_string(&offer_sdp).expect("adapter should return parseable SDP offer"),
    );
    assert!(answer.is_ok());
    let Some(answer) = answer.ok() else {
        return;
    };

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &answer.to_sdp_string())
            .await,
        Ok(())
    );
    assert_eq!(
        adapter.create_initial_session_offer(&session_key).await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_initial_session_offer_projects_client_capabilities_from_answer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 38, SessionId::Integer(38));

    let offer_sdp = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed")
        .into_sdp();
    let mut remote = reduced_capability_probe_rtc();
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], 55_038)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&offer_sdp)
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build")
        .to_sdp_string();

    let projected = client_rtp_capabilities_from_answer(&answer)
        .expect("real RTC answer should expose client RTP capabilities");
    let codec_names = projected
        .codecs()
        .map(|codec| (codec.media_kind(), codec.codec_name().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        codec_names,
        vec![
            (RouterMediaKind::Audio, String::from("opus")),
            (RouterMediaKind::Video, String::from("VP8")),
        ]
    );
    assert!(
        projected.codecs().all(|codec| codec.codec_name() != "rtx"),
        "the projected client capability set must not invent RTX support"
    );
    assert!(
        projected.codecs().all(|codec| codec.codec_name() != "H264"),
        "the projected client capability set must reflect the answer instead of the router default"
    );
}

#[tokio::test]
async fn rtc_initial_session_offer_rejects_overlapping_pending_offer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 35, SessionId::Integer(35));

    assert!(
        adapter
            .create_initial_session_offer(&session_key)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter.create_initial_session_offer(&session_key).await,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_protocol_producer_additions() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 45, SessionId::Integer(45));

    let mut remote = build_remote_rtc(55_006);
    let initial_offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-mid", 89_000),
        )
        .await
        .expect("protocol producer media should stage a renegotiation offer");

    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged renegotiation offer should be available");
    let renegotiation_sdp = renegotiation_offer.into_sdp();
    assert!(renegotiation_sdp.contains("m=video"));

    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("transport media should resolve to the server-assigned mid");
    assert!(renegotiation_sdp.contains(&format!("a=mid:{negotiated_mid}")));

    apply_offer_answer(&adapter, &session_key, &mut remote, renegotiation_sdp).await;

    assert_eq!(
        adapter
            .debug_session_stream_rx_ssrc(&session_key, negotiated_mid)
            .await,
        Some(89_000),
        "renegotiated recv media should expect the published SSRC once the answer lands"
    );

    let negotiated_parameters = adapter
        .negotiated_producer_parameters(&session_key, transport_media_id)
        .await
        .expect("answered producer negotiation should project router RTP parameters");
    assert_eq!(
        negotiated_parameters.mid(),
        Some(negotiated_mid.to_string().as_str())
    );
    assert!(
        negotiated_parameters.formats().next().is_some(),
        "projected producer parameters should include negotiated media formats"
    );
    assert!(
        negotiated_parameters.bindings().next().is_some(),
        "projected producer parameters should include at least one negotiated binding"
    );
}

#[tokio::test]
async fn rtc_protocol_publish_projects_recv_expectation_from_answer_when_publish_intent_has_no_ssrc()
 {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 46, SessionId::Integer(46));

    let mut remote = build_remote_rtc(55_007);
    let initial_offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &RouterRtpParameters::new(vec![], vec![], vec![]),
        )
        .await
        .expect("protocol publish intent should stage a recv-only media line");
    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("protocol publish should stage a follow-up offer");
    let renegotiation_sdp = renegotiation_offer.into_sdp();
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("transport media should expose its negotiated mid");

    apply_offer_answer(&adapter, &session_key, &mut remote, renegotiation_sdp).await;

    assert!(
        adapter
            .debug_session_stream_rx_ssrc(&session_key, negotiated_mid)
            .await
            .is_some(),
        "answered protocol publish should recover the recv expectation from the negotiated SDP"
    );
    let negotiated_parameters = adapter
        .negotiated_producer_parameters(&session_key, transport_media_id)
        .await
        .expect("protocol publish should project negotiated RTP parameters");
    assert!(
        negotiated_parameters.bindings().next().is_some(),
        "projected protocol publish parameters should expose at least one binding"
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_protocol_consumer_additions() {
    let adapter = RtcTransportAdapter::default();
    let source_session_key = transport_key(1, 36, SessionId::Integer(36));
    let consumer_session_key = transport_key(1, 37, SessionId::Integer(37));

    assert!(
        adapter
            .transport_bootstrap_payload(&source_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    let source_media_id = adapter
        .add_recv_media(
            &source_session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up", 81_000),
        )
        .await
        .expect("source media should register");

    let mut remote = build_remote_rtc(55_002);
    let initial_offer = adapter
        .create_initial_session_offer(&consumer_session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &source_session_key,
            source_media_id,
            None,
            &sample_router_rtp_parameters("compat-mid", 82_000),
        )
        .await
        .expect("protocol consumer media should stage a renegotiation offer");

    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&consumer_session_key)
        .await
        .expect("staged renegotiation offer should be available");
    let renegotiation_sdp = renegotiation_offer.into_sdp();
    assert!(renegotiation_sdp.contains("m=video"));

    let renegotiated_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("transport media should resolve to the server-assigned mid");
    assert!(renegotiation_sdp.contains(&format!("a=mid:{renegotiated_mid}")));

    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        renegotiation_sdp,
    )
    .await;

    assert!(
        adapter
            .debug_session_stream_tx_ssrc(&consumer_session_key, renegotiated_mid)
            .await
            .is_some(),
        "renegotiated send media should exist after the answer is applied"
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_negotiated_consumer_removal() {
    let adapter = RtcTransportAdapter::default();
    let source_session_key = transport_key(1, 39, SessionId::Integer(39));
    let consumer_session_key = transport_key(1, 40, SessionId::Integer(40));

    assert!(
        adapter
            .transport_bootstrap_payload(&source_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    let source_media_id = adapter
        .add_recv_media(
            &source_session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-remove", 83_000),
        )
        .await
        .expect("source media should register");

    let mut remote = build_remote_rtc(55_004);
    let initial_offer = adapter
        .create_initial_session_offer(&consumer_session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &source_session_key,
            source_media_id,
            None,
            &sample_router_rtp_parameters("compat-mid-remove", 84_000),
        )
        .await
        .expect("protocol consumer media should stage a renegotiation offer");
    let consumer_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("consumer media should expose its staged mid");

    let addition_offer = adapter
        .create_session_renegotiation_offer(&consumer_session_key)
        .await
        .expect("staged addition offer should be available");
    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        addition_offer.into_sdp(),
    )
    .await;

    assert_eq!(
        adapter
            .remove_media(&consumer_session_key, consumer_media_id)
            .await,
        Ok(())
    );
    assert_eq!(
        adapter.debug_route_entry_by_media_id(source_media_id).await,
        None
    );

    let removal_offer = adapter
        .create_session_renegotiation_offer(&consumer_session_key)
        .await
        .expect("removal should stage a renegotiation offer");
    let removal_sdp = removal_offer.into_sdp();
    let removal_section = media_section_for_mid(&removal_sdp, &format!("{consumer_mid}"))
        .expect("removed consumer mid should remain in the renegotiation offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(&adapter, &consumer_session_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_negotiated_producer_removal() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 46, SessionId::Integer(46));

    let mut remote = build_remote_rtc(55_007);
    let initial_offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let (producer_media_id, producer_mid) = add_negotiated_producer_media(
        &adapter,
        &session_key,
        "compat-producer-mid-remove",
        90_000,
        &mut remote,
    )
    .await;

    assert_eq!(
        adapter.remove_media(&session_key, producer_media_id).await,
        Ok(())
    );

    let removal_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("removal should stage a renegotiation offer");
    let removal_sdp = removal_offer.into_sdp();
    let removal_section = media_section_for_mid(&removal_sdp, &format!("{producer_mid}"))
        .expect("removed producer mid should remain in the renegotiation offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(&adapter, &session_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .negotiated_producer_parameters(&session_key, producer_media_id)
            .await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_stages_follow_up_removal_for_cancelled_pending_producer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 47, SessionId::Integer(47));

    let mut remote = build_remote_rtc(55_008);
    let initial_offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let producer_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-mid-cancel", 91_000),
        )
        .await
        .expect("protocol producer media should stage an addition offer");
    let producer_mid = adapter
        .debug_resolve_mid(producer_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("addition offer should be available");
    let addition_sdp = addition_offer.into_sdp();

    assert_eq!(
        adapter.remove_media(&session_key, producer_media_id).await,
        Ok(())
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );

    apply_offer_answer(&adapter, &session_key, &mut remote, addition_sdp).await;

    let removal_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("cancelled pending producer should stage a follow-up removal offer");
    let removal_sdp = removal_offer.into_sdp();
    let removal_section = media_section_for_mid(&removal_sdp, &format!("{producer_mid}"))
        .expect("cancelled producer mid should remain in the follow-up offer");
    assert!(removal_section.contains("a=inactive"));
    assert_eq!(
        adapter
            .negotiated_producer_parameters(&session_key, producer_media_id)
            .await,
        Err(TransportAdapterError::TransportUnavailable)
    );

    apply_offer_answer(&adapter, &session_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_queues_consumer_removal_while_answer_is_pending() {
    let adapter = RtcTransportAdapter::default();
    let source_session_key = transport_key(1, 42, SessionId::Integer(42));
    let consumer_session_key = transport_key(1, 43, SessionId::Integer(43));

    let (first_source_media_id, second_source_media_id) =
        setup_queued_removal_sources(&adapter, &source_session_key).await;

    let mut remote = build_remote_rtc(55_005);
    let initial_offer = adapter
        .create_initial_session_offer(&consumer_session_key)
        .await
        .expect("initial offer should succeed");
    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        initial_offer.into_sdp(),
    )
    .await;

    let (first_consumer_media_id, first_consumer_mid) = add_negotiated_consumer_media(
        &adapter,
        &consumer_session_key,
        &source_session_key,
        first_source_media_id,
        "compat-mid-queued-remove-a",
        87_000,
        &mut remote,
    )
    .await;

    let _second_consumer_media_id = adapter
        .add_send_media(
            &consumer_session_key,
            Str0mMediaKind::Video,
            &source_session_key,
            second_source_media_id,
            None,
            &sample_router_rtp_parameters("compat-mid-queued-remove-b", 88_000),
        )
        .await
        .expect("second protocol consumer media should stage an addition offer");
    let second_addition_offer = adapter
        .create_session_renegotiation_offer(&consumer_session_key)
        .await
        .expect("second addition offer should be available");
    let second_addition_sdp = second_addition_offer.into_sdp();

    assert_eq!(
        adapter
            .remove_media(&consumer_session_key, first_consumer_media_id)
            .await,
        Ok(())
    );
    assert_eq!(
        adapter
            .debug_route_entry_by_media_id(first_source_media_id)
            .await,
        None
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );

    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        second_addition_sdp,
    )
    .await;

    let queued_removal_offer = adapter
        .create_session_renegotiation_offer(&consumer_session_key)
        .await
        .expect("queued removal should stage after the in-flight answer lands");
    let queued_removal_sdp = queued_removal_offer.into_sdp();
    let removal_section =
        media_section_for_mid(&queued_removal_sdp, &format!("{first_consumer_mid}"))
            .expect("queued removal mid should remain in the follow-up offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(
        &adapter,
        &consumer_session_key,
        &mut remote,
        queued_removal_sdp,
    )
    .await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stays_blocked_after_initial_answer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 41, SessionId::Integer(41));

    let offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    let mut remote = build_remote_rtc(55_003);
    apply_offer_answer(&adapter, &session_key, &mut remote, offer.into_sdp()).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

fn media_section_for_mid<'a>(sdp: &'a str, mid: &str) -> Option<&'a str> {
    let marker = format!("a=mid:{mid}");
    let marker_start = sdp.find(&marker)?;
    let section_start = sdp[..marker_start]
        .rfind("\r\nm=")
        .map_or(0, |index| index + 2);
    let section_end = sdp[marker_start..]
        .find("\r\nm=")
        .map_or(sdp.len(), |offset| marker_start + offset + 2);
    Some(&sdp[section_start..section_end])
}

fn build_remote_rtc(port: u16) -> Rtc {
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], port)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    remote
}

fn reduced_capability_probe_rtc() -> Rtc {
    let mut config = Rtc::builder().clear_codecs();
    config.codec_config().add_config(
        111.into(),
        None,
        Codec::Opus,
        Frequency::FORTY_EIGHT_KHZ,
        Some(2),
        FormatParams {
            use_inband_fec: Some(true),
            ..Default::default()
        },
    );
    config.codec_config().add_config(
        96.into(),
        None,
        Codec::Vp8,
        Frequency::NINETY_KHZ,
        None,
        FormatParams::default(),
    );
    config.build(Instant::now())
}

async fn apply_offer_answer(
    adapter: &RtcTransportAdapter,
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
    assert_eq!(
        adapter
            .apply_session_answer(session_key, &answer.to_sdp_string())
            .await,
        Ok(())
    );
}

async fn setup_queued_removal_sources(
    adapter: &RtcTransportAdapter,
    source_session_key: &TransportSessionKey,
) -> (TransportMediaId, TransportMediaId) {
    assert!(
        adapter
            .transport_bootstrap_payload(source_session_key, &empty_router_capabilities())
            .await
            .is_ok()
    );
    let first_source_media_id = adapter
        .add_recv_media(
            source_session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-queued-remove-a", 85_000),
        )
        .await
        .expect("first source media should register");
    let second_source_media_id = adapter
        .add_recv_media(
            source_session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-queued-remove-b", 86_000),
        )
        .await
        .expect("second source media should register");
    (first_source_media_id, second_source_media_id)
}

async fn add_negotiated_consumer_media(
    adapter: &RtcTransportAdapter,
    consumer_session_key: &TransportSessionKey,
    source_session_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    mid: &str,
    ssrc: u32,
    remote: &mut Rtc,
) -> (TransportMediaId, Mid) {
    let consumer_media_id = adapter
        .add_send_media(
            consumer_session_key,
            Str0mMediaKind::Video,
            source_session_key,
            source_media_id,
            None,
            &sample_router_rtp_parameters(mid, ssrc),
        )
        .await
        .expect("protocol consumer media should stage an addition offer");
    let consumer_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("consumer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(consumer_session_key)
        .await
        .expect("addition offer should be available");
    apply_offer_answer(
        adapter,
        consumer_session_key,
        remote,
        addition_offer.into_sdp(),
    )
    .await;
    (consumer_media_id, consumer_mid)
}

async fn add_negotiated_producer_media(
    adapter: &RtcTransportAdapter,
    session_key: &TransportSessionKey,
    mid: &str,
    ssrc: u32,
    remote: &mut Rtc,
) -> (TransportMediaId, Mid) {
    let producer_media_id = adapter
        .add_recv_media(
            session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters(mid, ssrc),
        )
        .await
        .expect("protocol producer media should stage an addition offer");
    let producer_mid = adapter
        .debug_resolve_mid(producer_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(session_key)
        .await
        .expect("addition offer should be available");
    apply_offer_answer(adapter, session_key, remote, addition_offer.into_sdp()).await;
    (producer_media_id, producer_mid)
}
