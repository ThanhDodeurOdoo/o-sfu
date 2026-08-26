use std::fmt::Debug;

use super::{CounterProjection, Identity, Projection};

const PICTURE_ID_MODULUS: u32 = 1 << 15;
const PICTURE_ID_MASK: u16 = 0x7fff;
const TL0_PIC_IDX_MODULUS: u32 = 1 << 8;

/// `Anchored` omits `CounterProjection::Anchored::last` because every reachable
/// anchored state has `last == dst_anchor`. `snapshot` checks that invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelState<T> {
    Empty,
    LastOnly(T),
    Anchored { src_anchor: T, dst_anchor: T },
}

impl<T> ModelState<T> {
    fn map<U>(self, mut map: impl FnMut(T) -> U) -> ModelState<U> {
        match self {
            Self::Empty => ModelState::Empty,
            Self::LastOnly(last) => ModelState::LastOnly(map(last)),
            Self::Anchored {
                src_anchor,
                dst_anchor,
            } => ModelState::Anchored {
                src_anchor: map(src_anchor),
                dst_anchor: map(dst_anchor),
            },
        }
    }
}

struct ModelTransition {
    output: Option<u32>,
    state: ModelState<u32>,
}

/// Proves `Projection::project` and `Projection::reanchor` for every reachable
/// combination of PictureID and TL0PICIDX state.
///
/// `reach_state` constructs each stored state through `Projection` calls. The
/// proof applies one arbitrary call then compares its returned counters and
/// stored state with a widened modular model. `Projection::default` satisfies
/// the model and every call preserves it, so the property holds after any
/// finite sequence of direct calls.
///
/// The proof excludes descriptor parsing and patching, the caller's reanchor
/// decision and whether an outer RTP projection commits the result.
#[kani::proof]
fn vp8_identity_projection_matches_modular_model() {
    let default = Projection::default();
    assert_eq!(snapshot(default.picture_id), ModelState::Empty);
    assert_eq!(snapshot(default.tl0_pic_idx), ModelState::Empty);

    let picture_state = arbitrary_picture_state();
    let tl0_state = arbitrary_tl0_state();
    let mut projection = reach_state(picture_state, tl0_state);
    let reanchor = kani::any::<bool>();
    let identity = Identity {
        picture_id: kani::any::<bool>().then(arbitrary_picture_id),
        tl0_pic_idx: kani::any::<bool>().then(kani::any::<u8>),
    };

    let expected_picture = model_transition(
        picture_state.map(u32::from),
        identity.picture_id.map(u32::from),
        reanchor,
        PICTURE_ID_MODULUS,
    );
    let expected_tl0 = model_transition(
        tl0_state.map(u32::from),
        identity.tl0_pic_idx.map(u32::from),
        reanchor,
        TL0_PIC_IDX_MODULUS,
    );
    let projected = if reanchor {
        projection.reanchor(identity)
    } else {
        projection.project(identity)
    };

    assert_eq!(projected.picture_id.map(u32::from), expected_picture.output);
    assert_eq!(projected.tl0_pic_idx.map(u32::from), expected_tl0.output);
    assert_eq!(
        snapshot(projection.picture_id).map(u32::from),
        expected_picture.state
    );
    assert_eq!(
        snapshot(projection.tl0_pic_idx).map(u32::from),
        expected_tl0.state
    );

    let both_present = identity.picture_id.is_some() && identity.tl0_pic_idx.is_some();
    let both_absent = identity.picture_id.is_none() && identity.tl0_pic_idx.is_none();
    let both_empty =
        matches!(picture_state, ModelState::Empty) && matches!(tl0_state, ModelState::Empty);
    let both_anchored = matches!(picture_state, ModelState::Anchored { .. })
        && matches!(tl0_state, ModelState::Anchored { .. });
    let both_last_only = matches!(picture_state, ModelState::LastOnly(_))
        && matches!(tl0_state, ModelState::LastOnly(_));

    kani::cover!(!reanchor && both_empty && both_present, "first observation");
    kani::cover!(
        !reanchor && both_anchored && both_present,
        "anchored continuation"
    );
    kani::cover!(
        reanchor && both_anchored && both_present,
        "anchored source switch"
    );
    kani::cover!(
        reanchor && both_anchored && both_absent,
        "reanchor with absent counters"
    );
    kani::cover!(
        !reanchor && both_last_only && both_present,
        "resume after absent counters"
    );
    kani::cover!(
        !reanchor
            && both_anchored
            && identity.picture_id.is_none()
            && identity.tl0_pic_idx.is_some(),
        "PictureID absent with TL0PICIDX present"
    );
    kani::cover!(
        !reanchor
            && both_anchored
            && identity.picture_id.is_some()
            && identity.tl0_pic_idx.is_none(),
        "TL0PICIDX absent with PictureID present"
    );
    kani::cover!(
        matches!(picture_state, ModelState::LastOnly(PICTURE_ID_MASK))
            && identity.picture_id.is_some()
            && projected.picture_id == Some(0),
        "PictureID destination rollover"
    );
    kani::cover!(
        matches!(tl0_state, ModelState::LastOnly(u8::MAX))
            && identity.tl0_pic_idx.is_some()
            && projected.tl0_pic_idx == Some(0),
        "TL0PICIDX destination rollover"
    );
    kani::cover!(
        !reanchor
            && matches!(
                picture_state,
                ModelState::Anchored {
                    src_anchor: PICTURE_ID_MASK,
                    ..
                }
            )
            && identity.picture_id == Some(0),
        "PictureID source rollover"
    );
}

/// Reaches every `ModelState` pair through `Projection` calls.
///
/// The proof never assigns `CounterProjection` directly. Empty fields remain
/// absent. Last-only fields observe a value then lose their source anchor.
/// Anchored fields resume from the predecessor of their target destination so
/// the final projection lands on the requested anchor.
fn reach_state(picture_state: ModelState<u16>, tl0_state: ModelState<u8>) -> Projection {
    let mut projection = Projection::default();
    let _ = projection.project(Identity {
        picture_id: first_input(picture_state, picture_predecessor),
        tl0_pic_idx: first_input(tl0_state, tl0_predecessor),
    });
    let _ = projection.reanchor(Identity::default());
    let _ = projection.project(Identity {
        picture_id: final_input(picture_state),
        tl0_pic_idx: final_input(tl0_state),
    });

    assert_eq!(snapshot(projection.picture_id), picture_state);
    assert_eq!(snapshot(projection.tl0_pic_idx), tl0_state);
    projection
}

fn arbitrary_picture_state() -> ModelState<u16> {
    match kani::any::<u8>() % 3 {
        0 => ModelState::Empty,
        1 => ModelState::LastOnly(arbitrary_picture_id()),
        _ => ModelState::Anchored {
            src_anchor: arbitrary_picture_id(),
            dst_anchor: arbitrary_picture_id(),
        },
    }
}

fn arbitrary_tl0_state() -> ModelState<u8> {
    match kani::any::<u8>() % 3 {
        0 => ModelState::Empty,
        1 => ModelState::LastOnly(kani::any()),
        _ => ModelState::Anchored {
            src_anchor: kani::any(),
            dst_anchor: kani::any(),
        },
    }
}

fn arbitrary_picture_id() -> u16 {
    kani::any::<u16>() & PICTURE_ID_MASK
}

fn first_input<T>(state: ModelState<T>, predecessor: impl FnOnce(T) -> T) -> Option<T> {
    match state {
        ModelState::Empty => None,
        ModelState::LastOnly(last) => Some(last),
        ModelState::Anchored { dst_anchor, .. } => Some(predecessor(dst_anchor)),
    }
}

fn final_input<T>(state: ModelState<T>) -> Option<T> {
    match state {
        ModelState::Anchored { src_anchor, .. } => Some(src_anchor),
        ModelState::Empty | ModelState::LastOnly(_) => None,
    }
}

fn picture_predecessor(value: u16) -> u16 {
    if value == 0 {
        PICTURE_ID_MASK
    } else {
        value - 1
    }
}

fn tl0_predecessor(value: u8) -> u8 {
    value.wrapping_sub(1)
}

fn snapshot<T: Debug + PartialEq>(state: CounterProjection<T>) -> ModelState<T> {
    match state {
        CounterProjection::Empty => ModelState::Empty,
        CounterProjection::LastOnly(last) => ModelState::LastOnly(last),
        CounterProjection::Anchored {
            src_anchor,
            dst_anchor,
            last,
        } => {
            assert_eq!(last, dst_anchor);
            ModelState::Anchored {
                src_anchor,
                dst_anchor,
            }
        }
    }
}

fn model_transition(
    state: ModelState<u32>,
    source: Option<u32>,
    reanchor: bool,
    modulus: u32,
) -> ModelTransition {
    let Some(source) = source else {
        let state = match (reanchor, state) {
            (true, ModelState::Anchored { dst_anchor, .. }) => ModelState::LastOnly(dst_anchor),
            (_, state) => state,
        };
        return ModelTransition {
            output: None,
            state,
        };
    };

    let destination = match state {
        ModelState::Anchored { dst_anchor, .. } if reanchor => successor(dst_anchor, modulus),
        ModelState::Anchored {
            src_anchor,
            dst_anchor,
        } => translated(source, src_anchor, dst_anchor, modulus),
        ModelState::LastOnly(last) => successor(last, modulus),
        ModelState::Empty => source,
    };
    ModelTransition {
        output: Some(destination),
        state: ModelState::Anchored {
            src_anchor: source,
            dst_anchor: destination,
        },
    }
}

fn successor(value: u32, modulus: u32) -> u32 {
    (value + 1) % modulus
}

fn translated(source: u32, src_anchor: u32, dst_anchor: u32, modulus: u32) -> u32 {
    (dst_anchor + modulus + source - src_anchor) % modulus
}
