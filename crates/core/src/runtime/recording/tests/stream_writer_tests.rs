use serde_json::json;

use crate::runtime::{
    recording::{
        OrtpFileHeader,
        test_support::{
            OrtpCodec, OrtpFrameHeader, RecordingFileMetadata, RecordingMetadata, RecordingSegment,
            StreamWriter,
        },
    },
    source_model::UserStreamId,
};

#[test]
fn stream_writer_serializes_ortp_header_and_frames() {
    let header = OrtpFileHeader {
        codec: OrtpCodec::Opus,
        clock_rate: 48_000,
        channel_count: 2,
        payload_type: 111,
    };
    let writer = StreamWriter::new(Vec::new(), header);
    assert!(writer.is_ok());
    let Some(mut writer) = writer.ok() else {
        return;
    };

    let frame_payload = [0x80, 0x6f, 0x12, 0x34];
    assert!(writer.write_frame(123_456, &frame_payload).is_ok());
    let bytes = writer.into_inner();

    let expected_file_header = header.to_bytes();
    let expected_frame_header = OrtpFrameHeader {
        reception_timestamp_us: 123_456,
        rtp_packet_len: 4,
    }
    .to_bytes();
    let frame_start = expected_file_header.len();
    let payload_start = frame_start + expected_frame_header.len();

    assert_eq!(
        bytes.get(..frame_start),
        Some(expected_file_header.as_slice())
    );
    assert_eq!(
        bytes.get(frame_start..payload_start),
        Some(expected_frame_header.as_slice())
    );
    assert_eq!(bytes.get(payload_start..), Some(frame_payload.as_slice()));
}

#[test]
fn recording_metadata_round_trips_through_json() {
    let metadata = RecordingMetadata {
        version: 1,
        room_name: "demo".to_owned(),
        room_id: "room-uuid".to_owned(),
        routing_address: Some("https://record.example.test".to_owned()),
        audio: true,
        video: true,
        transcription: false,
        started_at: 1_000,
        stopped_at: Some(2_000),
        labels: [("42".to_owned(), "Alice".to_owned())]
            .into_iter()
            .collect(),
        files: vec![RecordingFileMetadata {
            filename: "audio/1000-42-audio.ortp".to_owned(),
            user_id: "42".to_owned(),
            stream_id: UserStreamId::new("main-audio"),
            codec: "opus".to_owned(),
            clock_rate: 48_000,
            segments: vec![RecordingSegment {
                active_at: 1_000,
                inactive_at: Some(2_000),
            }],
        }],
    };

    let value = serde_json::to_value(&metadata);
    assert!(value.is_ok());
    assert_eq!(
        value.ok(),
        Some(json!({
            "version": 1,
            "roomName": "demo",
            "roomId": "room-uuid",
            "routingAddress": "https://record.example.test",
            "audio": true,
            "video": true,
            "transcription": false,
            "startedAt": 1000,
            "stoppedAt": 2000,
            "labels": { "42": "Alice" },
            "files": [{
                "filename": "audio/1000-42-audio.ortp",
                "userId": "42",
                "streamId": "main-audio",
                "codec": "opus",
                "clockRate": 48000,
                "segments": [{
                    "activeAt": 1000,
                    "inactiveAt": 2000
                }]
            }]
        }))
    );
}
