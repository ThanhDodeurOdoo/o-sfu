use std::sync::Arc;

use crate::runtime::{
    RoomInstanceId,
    metrics::RtpForwardDestinationKind,
    packet_sink_registry::{PacketSink as MediaPacketSink, RoomPacketSinkRegistry},
};

pub(crate) trait MediaSource: Send + Sync {
    fn activate_room(&self, room_instance_id: RoomInstanceId, sink: Arc<dyn MediaPacketSink>);
    fn deactivate_room(&self, room_instance_id: RoomInstanceId);
}

impl MediaSource for RoomPacketSinkRegistry {
    fn activate_room(&self, room_instance_id: RoomInstanceId, sink: Arc<dyn MediaPacketSink>) {
        self.register_room(room_instance_id, sink, RtpForwardDestinationKind::Recording);
    }

    fn deactivate_room(&self, room_instance_id: RoomInstanceId) {
        self.unregister_room(room_instance_id);
    }
}

pub(crate) fn into_media_source<T>(source: Arc<T>) -> Arc<dyn MediaSource>
where
    T: MediaSource + 'static,
{
    source
}
