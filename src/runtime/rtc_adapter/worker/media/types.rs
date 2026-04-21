//! Small request and ownership transfer objects shared by worker media modules.

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::{KeyframeRequestKind, MediaKind, Rid};

use crate::runtime::transport_adapter::{TransportMediaId, TransportSessionKey};

use super::super::super::{commands::RemoteSourceControl, relay_registry::RelayTargetId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteSourceKind {
    Local,
    Remote,
}

pub(crate) struct AddSendMediaRequest<'a> {
    pub(crate) consumer_session_key: &'a TransportSessionKey,
    pub(crate) media_kind: MediaKind,
    pub(crate) source_session_key: &'a TransportSessionKey,
    pub(crate) source_transport_media_id: TransportMediaId,
    pub(crate) remote_source_control: Option<RemoteSourceControl>,
    pub(crate) consumer_rtp_parameters: &'a RouterRtpParameters,
}

pub(crate) struct RemoteKeyframeRequest<'a> {
    pub(crate) source_session_key: &'a TransportSessionKey,
    pub(crate) source_transport_media_id: TransportMediaId,
    pub(crate) target_id: RelayTargetId,
    pub(crate) rid: Option<Rid>,
    pub(crate) kind: KeyframeRequestKind,
}

impl RouteSourceKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}
