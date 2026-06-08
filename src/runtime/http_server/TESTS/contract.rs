use serde_json::json;

use super::{
    CreateRoomQuery, IncomingBitRateStatsResponse, NoopResponse, RoomResponse, RoomStatsResponse,
    StatsResponse, UsersStatsResponse,
};

#[test]
fn route_types_round_trip() -> serde_json::Result<()> {
    let query = CreateRoomQuery {
        web_rtc: Some(false),
        recording_address: Some("https://record.example.com".to_owned()),
    };
    let expected_query = json!({
        "webRTC": false,
        "recordingAddress": "https://record.example.com"
    });
    assert_eq!(serde_json::to_value(&query)?, expected_query);
    assert_eq!(
        serde_json::from_value::<CreateRoomQuery>(expected_query)?,
        query
    );
    assert!(!query.web_rtc_enabled());

    let noop = NoopResponse::ok();
    let expected_noop = json!({ "result": "ok" });
    assert_eq!(serde_json::to_value(&noop)?, expected_noop);
    assert_eq!(serde_json::from_value::<NoopResponse>(expected_noop)?, noop);

    let room = RoomResponse {
        uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
        url: "https://sfu.example.com".to_owned(),
    };
    let expected_room = json!({
        "uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
        "url": "https://sfu.example.com"
    });
    assert_eq!(serde_json::to_value(&room)?, expected_room);
    assert_eq!(serde_json::from_value::<RoomResponse>(expected_room)?, room);

    let stats: StatsResponse = vec![RoomStatsResponse {
        create_date: "2026-04-02T01:02:03.000Z".to_owned(),
        uuid: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
        remote_address: "203.0.113.10".to_owned(),
        users_stats: UsersStatsResponse {
            incoming_bit_rate: IncomingBitRateStatsResponse {
                total: 1200,
                screen: 400,
                audio: 300,
                camera: 500,
            },
            count: 2,
            camera_count: 1,
            screen_count: 1,
        },
        web_rtc_enabled: true,
    }];
    let expected_stats = json!([{
        "createDate": "2026-04-02T01:02:03.000Z",
        "uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
        "remoteAddress": "203.0.113.10",
        "sessionsStats": {
            "incomingBitRate": {
                "total": 1200,
                "screen": 400,
                "audio": 300,
                "camera": 500
            },
            "count": 2,
            "cameraCount": 1,
            "screenCount": 1
        },
        "webRtcEnabled": true
    }]);
    assert_eq!(serde_json::to_value(&stats)?, expected_stats);
    assert_eq!(
        serde_json::from_value::<StatsResponse>(expected_stats)?,
        stats
    );
    Ok(())
}
