//! Small request and ownership transfer objects shared by worker media modules.

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};

use super::super::super::{commands::RemoteSourceControl, relay_registry::RelayTargetId};
use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteSourceKind {
    Local,
    Remote,
}

pub struct AddSendMediaRequest<'a> {
    pub consumer_session_key: &'a TransportSessionKey,
    pub media_kind: MediaKind,
    pub source_session_key: &'a TransportSessionKey,
    pub source_transport_media_id: TransportMediaId,
    pub remote_source_control: Option<RemoteSourceControl>,
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
}

pub struct RemoteKeyframeRequest<'a> {
    pub source_session_key: &'a TransportSessionKey,
    pub source_transport_media_id: TransportMediaId,
    pub target_id: RelayTargetId,
    pub rid: Option<Rid>,
    pub kind: KeyframeRequestKind,
}

impl RouteSourceKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}
