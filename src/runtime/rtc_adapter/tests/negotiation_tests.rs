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
async fn rtc_session_renegotiation_offer_stays_blocked_after_initial_answer() {
    let adapter = RtcTransportAdapter::default();
    let session_key = transport_key(1, 36, SessionId::Integer(36));

    let offer = adapter
        .create_initial_session_offer(&session_key)
        .await
        .expect("initial offer should succeed");
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(SocketAddr::from(([127, 0, 0, 1], 55_001)), "udp")
                .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&offer.into_sdp())
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build");

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &answer.to_sdp_string())
            .await,
        Ok(())
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}
