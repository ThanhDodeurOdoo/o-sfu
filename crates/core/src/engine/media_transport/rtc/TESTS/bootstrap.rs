use str0m::format::Codec;

use super::*;

#[test]
fn h264_bootstrap_omits_rtx_for_receiver_safe_consumer_streams() {
    let mut config = rtc_builder(
        MediaCodecFlags::default().with_vp8(false).with_h264(true),
        None,
    );
    let h264_payloads = config
        .codec_config()
        .params()
        .iter()
        .filter(|params| params.spec().codec == Codec::H264)
        .map(|params| {
            let spec = params.spec();
            (
                *params.pt(),
                params.resend().map(|payload_type| *payload_type),
                spec.format.packetization_mode,
                spec.format.profile_level_id,
            )
        })
        .collect::<Vec<_>>();
    let expected = H264_PAYLOAD_SPECS
        .iter()
        .map(|spec| {
            (
                spec.payload_type(),
                None,
                Some(spec.packetization_mode().fmtp_value()),
                Some(spec.profile_level_id()),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(h264_payloads, expected);
}
