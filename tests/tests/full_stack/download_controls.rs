use super::support::*;

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers() -> TestResult
{
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers("issuer-e", UserId::Integer(70), UserId::Integer(71)).await?;

    assert_audio_media_arrives_and_download_mute_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn fake_rtc_peers_stop_forwarding_after_explicit_upload_unpublish() -> TestResult {
    let _guard = full_stack_test_guard().await;
    let ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = ready_room_fake_peers("issuer-f", UserId::Integer(70), UserId::Integer(71)).await?;

    assert_audio_media_arrives_and_explicit_unpublish_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
    Ok(())
}
