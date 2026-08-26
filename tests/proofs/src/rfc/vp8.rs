use o_sfu_rfc::rtp::vp8 as production;

const DECISION_PREFIX_LEN: usize = 16;
const X_BIT: u8 = 0x80;
const S_BIT: u8 = 0x10;
const PARTITION_ID_MASK: u8 = 0x07;
const I_BIT: u8 = 0x80;
const L_BIT: u8 = 0x40;
const T_BIT: u8 = 0x20;
const K_BIT: u8 = 0x10;
const LONG_PICTURE_ID_BIT: u8 = 0x80;
const INTERFRAME_BIT: u8 = 0x01;
const VERSION_MASK: u8 = 0x0e;
const START_CODE: [u8; 3] = [0x9d, 0x01, 0x2a];
const DIMENSION_MASK: u16 = 0x3fff;

/// Proves that [`production::payload_starts_keyframe`] agrees with an
/// independent RFC 7741 descriptor model and RFC 6386 keyframe-prefix model
/// for every payload of length 0 through 16. Six descriptor bytes plus the
/// ten-byte keyframe prefix form the parser's maximum decision window.
/// Covers keep the accepted descriptor shapes and distinct rejection classes
/// reachable alongside the equivalence assertion.
#[kani::proof]
fn vp8_keyframe_prefix_matches_rfc_model() {
    let prefix = kani::any::<[u8; DECISION_PREFIX_LEN]>();
    let payload_len = usize::from(kani::any::<u8>() % 17);
    let payload = &prefix[..payload_len];

    let actual = production::payload_starts_keyframe(payload);
    let frame = model_frame_payload(payload);
    let expected = frame.is_some_and(is_keyframe_prefix);
    let descriptor = payload.first().copied().unwrap_or_default();
    let extension = payload.get(1).copied().unwrap_or_default();

    assert_eq!(actual, expected);

    kani::cover!(actual && descriptor & X_BIT == 0, "unextended keyframe");
    kani::cover!(
        actual
            && descriptor & X_BIT != 0
            && extension & I_BIT != 0
            && prefix[2] & LONG_PICTURE_ID_BIT == 0,
        "short PictureID keyframe"
    );
    kani::cover!(
        actual
            && descriptor & X_BIT != 0
            && extension & I_BIT != 0
            && prefix[2] & LONG_PICTURE_ID_BIT != 0,
        "long PictureID keyframe"
    );
    kani::cover!(
        actual
            && descriptor & X_BIT != 0
            && extension & (I_BIT | L_BIT | T_BIT | K_BIT) == I_BIT | L_BIT | T_BIT | K_BIT
            && prefix[2] & LONG_PICTURE_ID_BIT != 0,
        "maximum-length descriptor keyframe"
    );
    kani::cover!(
        actual && descriptor & X_BIT != 0 && extension & (I_BIT | L_BIT | T_BIT | K_BIT) == K_BIT,
        "KEYIDX-only keyframe"
    );
    kani::cover!(
        payload_len == 1
            && descriptor & (X_BIT | S_BIT) == X_BIT | S_BIT
            && descriptor & PARTITION_ID_MASK == 0
            && !actual,
        "truncated extended descriptor"
    );
    kani::cover!(
        payload_len == DECISION_PREFIX_LEN
            && descriptor & (X_BIT | S_BIT) == X_BIT | S_BIT
            && descriptor & PARTITION_ID_MASK == 0
            && extension & L_BIT != 0
            && extension & T_BIT == 0
            && !actual,
        "TL0PICIDX without TID"
    );
    kani::cover!(
        frame.is_some_and(|frame| { has_complete_prefix(frame) && frame[0] & INTERFRAME_BIT != 0 })
            && !actual,
        "interframe"
    );
    kani::cover!(
        frame.is_some_and(|frame| {
            has_complete_prefix(frame)
                && has_keyframe_type_and_defined_version(frame)
                && !has_start_code(frame)
                && has_nonzero_dimensions(frame)
        }) && !actual,
        "invalid keyframe start code"
    );
    kani::cover!(
        frame.is_some_and(|frame| {
            has_complete_prefix(frame)
                && has_keyframe_type_and_defined_version(frame)
                && has_start_code(frame)
                && !has_nonzero_dimensions(frame)
        }) && !actual,
        "zero keyframe dimension"
    );
    kani::cover!(
        frame.is_some_and(|frame| {
            has_complete_prefix(frame)
                && frame[0] & INTERFRAME_BIT == 0
                && (frame[0] & VERSION_MASK) >> 1 > 3
                && has_start_code(frame)
                && has_nonzero_dimensions(frame)
        }) && !actual,
        "undefined keyframe version"
    );
}

/// Models RFC 7741 field presence independently of the production helpers.
///
/// The offset follows only the advertised descriptor fields so shared parser
/// control flow cannot make the equivalence assertion pass by construction.
fn model_frame_payload(payload: &[u8]) -> Option<&[u8]> {
    let descriptor = *payload.first()?;
    if descriptor & S_BIT == 0 || descriptor & PARTITION_ID_MASK != 0 {
        return None;
    }

    let mut offset = 1;
    if descriptor & X_BIT != 0 {
        let extension = *payload.get(offset)?;
        offset += 1;
        if extension & L_BIT != 0 && extension & T_BIT == 0 {
            return None;
        }
        if extension & I_BIT != 0 {
            let picture_id = *payload.get(offset)?;
            offset += 1;
            if picture_id & LONG_PICTURE_ID_BIT != 0 {
                payload.get(offset)?;
                offset += 1;
            }
        }
        if extension & L_BIT != 0 {
            payload.get(offset)?;
            offset += 1;
        }
        if extension & (T_BIT | K_BIT) != 0 {
            payload.get(offset)?;
            offset += 1;
        }
    }

    payload.get(offset..).filter(|frame| !frame.is_empty())
}

fn is_keyframe_prefix(frame: &[u8]) -> bool {
    has_complete_prefix(frame)
        && has_keyframe_type_and_defined_version(frame)
        && has_start_code(frame)
        && has_nonzero_dimensions(frame)
}

fn has_complete_prefix(frame: &[u8]) -> bool {
    frame.len() >= 10
}

fn has_keyframe_type_and_defined_version(frame: &[u8]) -> bool {
    frame
        .first()
        .is_some_and(|tag| tag & INTERFRAME_BIT == 0 && (tag & VERSION_MASK) >> 1 <= 3)
}

fn has_start_code(frame: &[u8]) -> bool {
    frame.get(3).copied() == Some(START_CODE[0])
        && frame.get(4).copied() == Some(START_CODE[1])
        && frame.get(5).copied() == Some(START_CODE[2])
}

fn has_nonzero_dimensions(frame: &[u8]) -> bool {
    let Some(dimensions) = frame.get(6..10) else {
        return false;
    };
    let width = u16::from_le_bytes([dimensions[0], dimensions[1]]) & DIMENSION_MASK;
    let height = u16::from_le_bytes([dimensions[2], dimensions[3]]) & DIMENSION_MASK;
    width != 0 && height != 0
}
