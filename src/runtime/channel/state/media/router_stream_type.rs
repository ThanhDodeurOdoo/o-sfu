use o_sfu_router::StreamType as RouterStreamType;

use crate::signaling::shared::StreamType;

pub(super) const fn to_router_stream_type(stream_type: StreamType) -> RouterStreamType {
    match stream_type {
        StreamType::Audio => RouterStreamType::Audio,
        StreamType::Camera => RouterStreamType::Camera,
        StreamType::Screen => RouterStreamType::Screen,
    }
}
