use super::fixtures::*;

#[test]
fn shared_payload_clones_for_non_final_destination() {
    let mut payload = SharedPayload::from_vec(vec![1, 2, 3, 4]);

    let write_payload = payload.take_write_payload(false);

    assert_eq!(write_payload, vec![1, 2, 3, 4]);
    assert_eq!(payload.as_slice(), [1, 2, 3, 4]);
}

#[test]
fn shared_payload_moves_for_final_destination() {
    let mut payload = SharedPayload::from_vec(vec![5, 6, 7, 8]);

    let write_payload = payload.take_write_payload(true);

    assert_eq!(write_payload, vec![5, 6, 7, 8]);
    assert!(payload.as_slice().is_empty());
}

#[test]
fn shared_payload_reports_length_through_the_helper() {
    let payload = SharedPayload::from_vec(vec![9, 10, 11]);

    assert_eq!(payload.len(), 3);
    assert_eq!(payload.as_slice(), [9, 10, 11]);
}

#[test]
fn validate_dtls_parameters_accepts_client_sha256_payload() {
    let result = validation::validate_dtls_parameters(&sample_sha256_dtls_parameters("client"));
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_sdp_offer_accepts_firefox_offer_fixture() {
    let result = validation::validate_sdp_offer(FIREFOX_OFFER_AUDIO_ONLY);
    assert_eq!(result, Ok(()));
}

#[test]
fn validate_sdp_offer_maps_safari_datachannel_fixture_to_unsupported_feature() {
    let result = validation::validate_sdp_offer(SAFARI_DATA_CHANNEL_OFFER);
    assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
}

#[test]
fn validate_dtls_parameters_maps_invalid_payload_to_invalid_input() {
    let result = validation::validate_dtls_parameters(&TransportConnectDtlsParameters {
        role: String::from("client"),
        fingerprints: vec![],
    });
    assert_eq!(result, Err(TransportAdapterError::InvalidInput));
}

#[test]
fn validate_dtls_parameters_maps_unsupported_payload_to_unsupported_feature() {
    let result = validation::validate_dtls_parameters(&TransportConnectDtlsParameters {
        role: String::from("client"),
        fingerprints: vec![TransportConnectDtlsFingerprint {
            algorithm: String::from("sha-1"),
            value: String::from("AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD"),
        }],
    });
    assert_eq!(result, Err(TransportAdapterError::UnsupportedFeature));
}

#[test]
fn validate_bootstrap_payload_accepts_supported_candidate_shape() {
    let payload = sample_bootstrap_payload(sample_candidate(40_000));
    let result = validation::validate_bootstrap_payload(&payload);
    assert_eq!(result, Ok(()));
}
