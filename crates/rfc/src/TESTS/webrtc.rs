use super::{RtpHeaderExtensionUri, RtpStreamDirection, rtp_header_extension_uri, sdp};

#[test]
fn rtp_stream_direction_uses_case_sensitive_rfc_tokens() {
    assert_eq!(
        RtpStreamDirection::parse(sdp::rid::DIRECTION_SEND),
        Some(RtpStreamDirection::Send)
    );
    assert_eq!(
        RtpStreamDirection::parse(sdp::rid::DIRECTION_RECV),
        Some(RtpStreamDirection::Recv)
    );
    assert_eq!(RtpStreamDirection::Send.as_str(), "send");
    assert_eq!(RtpStreamDirection::parse("SEND"), None);
}

#[test]
fn rid_id_validation_follows_rfc_8852_stream_id_rules() {
    let max_length_id = "a".repeat(sdp::rid::MAX_ID_OCTETS);
    let oversized_id = "a".repeat(sdp::rid::MAX_ID_OCTETS + 1);

    assert!(sdp::rid::is_id("low1"));
    assert!(sdp::rid::is_id("HI2"));
    assert!(sdp::rid::is_id(&max_length_id));
    assert!(!sdp::rid::is_id(""));
    assert!(!sdp::rid::is_id(&oversized_id));
    assert!(!sdp::rid::is_id("low-1"));
    assert!(!sdp::rid::is_id("hi_2"));
    assert!(!sdp::rid::is_id("hi.2"));
    assert!(!sdp::rid::is_id("hi:2"));
}

#[test]
fn simulcast_prefix_and_delimiters_follow_rfc_8853() {
    assert_eq!(sdp::simulcast::STREAM_SEPARATOR, ';');
    assert_eq!(sdp::simulcast::ALTERNATIVE_SEPARATOR, ',');
    assert_eq!(
        sdp::simulcast::strip_initial_pause_prefix("~hi"),
        Some("hi")
    );
    assert_eq!(sdp::simulcast::strip_initial_pause_prefix("hi"), None);
}

#[test]
fn header_extension_uri_maps_simulcast_and_svc_values() {
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::RTP_STREAM_ID),
        RtpHeaderExtensionUri::RtpStreamId
    );
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID),
        RtpHeaderExtensionUri::RepairedRtpStreamId
    );
    assert_eq!(
        RtpHeaderExtensionUri::from(rtp_header_extension_uri::FRAME_MARKING),
        RtpHeaderExtensionUri::FrameMarking
    );
}
