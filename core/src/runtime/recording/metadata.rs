use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::runtime::StreamType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordingMetadata {
    pub(crate) version: u16,
    pub(crate) room_name: String,
    pub(crate) room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) routing_address: Option<String>,
    pub(crate) audio: bool,
    pub(crate) video: bool,
    pub(crate) transcription: bool,
    pub(crate) started_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stopped_at: Option<u64>,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) files: Vec<RecordingFileMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordingFileMetadata {
    pub(crate) filename: String,
    pub(crate) user_id: String,
    pub(crate) stream_type: StreamType,
    pub(crate) codec: String,
    pub(crate) clock_rate: u32,
    pub(crate) segments: Vec<RecordingSegment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordingSegment {
    pub(crate) active_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inactive_at: Option<u64>,
}
