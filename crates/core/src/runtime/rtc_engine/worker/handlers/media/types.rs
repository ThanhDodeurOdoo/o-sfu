//! Small request and ownership transfer objects shared by worker media modules.

use o_sfu_router::MediaStream as RouterRtpParameters;
use str0m::media::MediaKind;

use super::super::super::super::commands::RemoteSourceControl;
use crate::runtime::media_transport::{TransportSessionKey, TransportSourceKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteSourceKind {
    Local,
    Remote,
}

pub struct AddSendMediaRequest<'a> {
    pub consumer_session_key: &'a TransportSessionKey,
    pub media_kind: MediaKind,
    pub source: &'a TransportSourceKey,
    pub remote_source_control: Option<RemoteSourceControl>,
    pub consumer_rtp_parameters: &'a RouterRtpParameters,
    pub active: bool,
}

impl RouteSourceKind {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}
