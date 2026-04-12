use std::time::Instant;

use str0m::{Candidate, Rtc, change::SdpOffer};

use super::fixtures::*;

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
    assert!(offer_sdp.contains("a=inactive"));

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
async fn rtc_session_renegotiation_offer_stages_native_consumer_additions() {
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
            &sample_router_rtp_parameters("compat-mid", 82_000),
        )
        .await
        .expect("native consumer media should stage a renegotiation offer");

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
async fn rtc_session_renegotiation_offer_stays_blocked_after_initial_answer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 38, SessionId::Integer(38));

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
