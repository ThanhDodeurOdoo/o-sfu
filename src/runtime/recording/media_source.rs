use std::sync::Arc;

use crate::runtime::{
    ChannelInstanceId,
    metrics::RtpForwardDestinationKind,
    packet_sink_registry::{ChannelPacketSinkRegistry, PacketSink as MediaPacketSink},
};

pub(crate) trait MediaSource: Send + Sync {
    fn activate_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
        sink: Arc<dyn MediaPacketSink>,
    );
    fn deactivate_channel(&self, channel_instance_id: ChannelInstanceId);
}

impl MediaSource for ChannelPacketSinkRegistry {
    fn activate_channel(
        &self,
        channel_instance_id: ChannelInstanceId,
        sink: Arc<dyn MediaPacketSink>,
    ) {
        self.register_channel(
            channel_instance_id,
            sink,
            RtpForwardDestinationKind::Recording,
        );
    }

    fn deactivate_channel(&self, channel_instance_id: ChannelInstanceId) {
        self.unregister_channel(channel_instance_id);
    }
}

pub(crate) fn into_media_source<T>(source: Arc<T>) -> Arc<dyn MediaSource>
where
    T: MediaSource + 'static,
{
    source
}
