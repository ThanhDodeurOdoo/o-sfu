use o_sfu_model::UserId;
use serde_json::{Map, Value};

use super::{
    DiagnosticsEventData, DiagnosticsRoomInstanceId, DiagnosticsStore, GLOBAL_RECENT_EVENT_LIMIT,
};

#[test]
fn global_events_keep_the_newest_entries_in_reverse_chronological_order() {
    let store = DiagnosticsStore::default();

    for index in 0..(GLOBAL_RECENT_EVENT_LIMIT + 3) {
        store.record(
            DiagnosticsEventData::for_room("room-a", "event")
                .insert_field("index", Value::from(index.to_string())),
        );
    }

    let events = store.global_recent_events();
    assert_eq!(events.len(), GLOBAL_RECENT_EVENT_LIMIT);
    assert_eq!(
        events.first().and_then(|event| event.fields.get("index")),
        Some(&Value::from("66"))
    );
    assert_eq!(
        events.last().and_then(|event| event.fields.get("index")),
        Some(&Value::from("3"))
    );
}

#[test]
fn forgetting_a_room_clears_room_and_user_scoped_history() {
    let store = DiagnosticsStore::default();
    let user_id = UserId::Integer(7);
    store.register_user("room-a", &user_id);
    store.record(DiagnosticsEventData::for_room("room-a", "room.event"));
    store.record(DiagnosticsEventData::for_user(
        "room-a",
        &user_id,
        "user.event",
    ));
    store.record(DiagnosticsEventData::for_room("room-b", "other.event"));

    store.forget_room("room-a");

    assert!(store.room_recent_events("room-a").is_empty());
    assert!(store.user_recent_events("room-a", &user_id).is_empty());
    assert!(store.user_lookup_room_ids("7").is_empty());
    assert_eq!(store.room_recent_events("room-b").len(), 1);
    assert_eq!(store.global_recent_events().len(), 3);
}

#[test]
fn user_lookup_tracks_integer_and_string_user_ids_by_room() {
    let store = DiagnosticsStore::default();
    let integer_user = UserId::Integer(7);
    let string_user = UserId::String(String::from("guest-7"));
    store.register_user("room-a", &integer_user);
    store.register_user("room-b", &integer_user);
    store.register_user("room-c", &string_user);

    assert_eq!(
        store.user_lookup_room_ids("7"),
        vec![String::from("room-a"), String::from("room-b")]
    );
    assert_eq!(
        store.user_lookup_room_ids("guest-7"),
        vec![String::from("room-c")]
    );

    store.forget_user("room-a", &integer_user);

    assert_eq!(
        store.user_lookup_room_ids("7"),
        vec![String::from("room-b")]
    );

    store.forget_room("room-c");

    assert!(store.user_lookup_room_ids("guest-7").is_empty());
}

#[test]
fn transport_user_events_use_registered_runtime_to_find_room_scope() {
    let store = DiagnosticsStore::default();
    let user_id = UserId::Integer(9);
    let mut fields = Map::new();
    fields.insert(String::from("state"), Value::from("connected"));

    store.record_transport_user_event(
        DiagnosticsRoomInstanceId::from_raw(12),
        &user_id,
        "transport.health_changed",
        2,
        fields,
    );
    assert!(store.global_recent_events().is_empty());

    store.register_room_instance(DiagnosticsRoomInstanceId::from_raw(12), "room-a");
    let mut fields = Map::new();
    fields.insert(String::from("state"), Value::from("connected"));
    store.record_transport_user_event(
        DiagnosticsRoomInstanceId::from_raw(12),
        &user_id,
        "transport.health_changed",
        2,
        fields,
    );

    let events = store.user_recent_events("room-a", &user_id);
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.first().map(|event| event.room_id.as_str()),
        Some("room-a")
    );
    assert_eq!(
        events.first().and_then(|event| event.media_worker_id),
        Some(2)
    );
    assert_eq!(
        events.first().and_then(|event| event.fields.get("state")),
        Some(&Value::from("connected"))
    );
}
