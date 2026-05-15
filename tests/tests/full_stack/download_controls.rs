use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-e").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_audio_media_flow_peers(&server, &room).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_download_mute_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
}

#[tokio::test]
async fn fake_rtc_peers_stop_forwarding_after_explicit_upload_unpublish() {
    let _guard = full_stack_test_guard().await;
    let room_server = spawn_room_server("issuer-f").await;
    assert!(room_server.is_some());
    let Some(room_server) = room_server else {
        return;
    };
    let (server, room) = room_server.into_parts();

    let setup = connect_audio_media_flow_peers(&server, &room).await;
    assert!(setup.is_some());
    let Some((mut publisher, mut subscriber)) = setup else {
        return;
    };

    assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
}
