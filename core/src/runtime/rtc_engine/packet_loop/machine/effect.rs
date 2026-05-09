//! Typed packet-loop effects.
//!
//! The packet loop emits these values when a turn needs host-owned services.
//! Execution remains outside the deterministic planning path.

use std::net::SocketAddr;

use str0m::media::{KeyframeRequestKind, Rid};

use super::super::super::commands::RemoteSourceControl;
use crate::runtime::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{
        RtcDatagramDropReason, RtcDatagramRoutePath, RtcRouteControlOutcome,
        RtpForwardDestinationKind, RtpRelayDropKind, TransportIceState,
    },
    rtc_engine::{TransportSessionHealth, packet_loop::time::PacketLoopTime},
};

#[derive(Debug, Clone)]
pub(in crate::runtime::rtc_engine) enum PacketLoopEffect {
    RecordIncomingBitrate {
        packet_idx: usize,
        transport_media_id: TransportMediaId,
        payload_bytes: usize,
    },
    RecordHotRtpMetric(HotRtpMetricEffect),
    RecordMetric(PacketLoopMetricEffect),
    MarkSourcePolicyDirty(RoomInstanceId),
    RememberSnapshotRemoteAddr {
        source_addr: SocketAddr,
        session_key: TransportSessionKey,
    },
    ForgetSnapshotRemoteAddr(SocketAddr),
    SetReceiverBandwidth {
        session_key: TransportSessionKey,
        estimate_bps: u64,
    },
    SetTransportHealth {
        session_key: TransportSessionKey,
        health: TransportSessionHealth,
    },
    RequestLocalKeyframe {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        now: PacketLoopTime,
    },
    RequestRemoteKeyframe {
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
        source_control: RemoteSourceControl,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) enum HotRtpMetricEffect {
    Ingress {
        payload_bytes: usize,
    },
    Egress {
        payload_bytes: usize,
    },
    Forwarded {
        destination: RtpForwardDestinationKind,
        payload_bytes: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime::rtc_engine) enum PacketLoopMetricEffect {
    RtcDatagramRoute(RtcDatagramRoutePath),
    RtcDatagramDrop(RtcDatagramDropReason),
    RtcDatagramFallbackScan(usize),
    RtcRouteControl(RtcRouteControlOutcome),
    RtpRelayOverloadDrop(RtpRelayDropKind),
    TransportIceStateChange(TransportIceState),
    TransportDtlsConnected,
}

#[derive(Debug, Default)]
pub struct PacketLoopEffects {
    effects: Vec<PacketLoopEffect>,
}

impl PacketLoopEffects {
    pub fn clear(&mut self) {
        self.effects.clear();
    }

    pub(in crate::runtime::rtc_engine) fn push(&mut self, effect: PacketLoopEffect) {
        self.effects.push(effect);
    }

    pub(in crate::runtime::rtc_engine) fn record_hot_rtp(&mut self, effect: HotRtpMetricEffect) {
        self.push(PacketLoopEffect::RecordHotRtpMetric(effect));
    }

    pub(in crate::runtime::rtc_engine) fn record_metric(&mut self, effect: PacketLoopMetricEffect) {
        self.push(PacketLoopEffect::RecordMetric(effect));
    }

    pub(in crate::runtime::rtc_engine) fn iter(&self) -> impl Iterator<Item = &PacketLoopEffect> {
        self.effects.iter()
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn incoming_bitrate_effect_count(&self) -> usize {
        self.effects
            .iter()
            .filter(|effect| matches!(effect, PacketLoopEffect::RecordIncomingBitrate { .. }))
            .count()
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn effect_count(&self) -> usize {
        self.effects.len()
    }

    #[cfg(any(test, feature = "packet-loop-verification"))]
    #[must_use]
    pub fn invalid_reference_count(&self, scratch: &super::scratch::PacketLoopScratch) -> usize {
        self.effects
            .iter()
            .filter(|effect| match effect {
                PacketLoopEffect::RecordIncomingBitrate { packet_idx, .. } => {
                    *packet_idx >= scratch.pending_packet_count()
                }
                PacketLoopEffect::RecordHotRtpMetric(_)
                | PacketLoopEffect::RecordMetric(_)
                | PacketLoopEffect::MarkSourcePolicyDirty(_)
                | PacketLoopEffect::RememberSnapshotRemoteAddr { .. }
                | PacketLoopEffect::ForgetSnapshotRemoteAddr(_)
                | PacketLoopEffect::SetReceiverBandwidth { .. }
                | PacketLoopEffect::SetTransportHealth { .. }
                | PacketLoopEffect::RequestLocalKeyframe { .. }
                | PacketLoopEffect::RequestRemoteKeyframe { .. } => false,
            })
            .count()
    }
}
