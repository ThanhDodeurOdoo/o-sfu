#[allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]
mod channel_tests {
    use std::{sync::Arc, time::Duration};

    use o_sfu_router::SessionPermissions as RouterSessionPermissions;
    use tokio::sync::mpsc;
    use tokio::{task::yield_now, time::timeout};

    use super::super::{
        ChannelJoinError, ChannelManager, ChannelManagerJoinError, SessionOutbound,
    };
    use crate::runtime::stub_bus::{StubWebRtcAdapter, StubWebRtcEvent};
    use crate::runtime::transport_adapter::{RuntimeTransportAdapter, TransportConnectDirection};
    use crate::signaling::{
        current_protocol::{CurrentServerMessage, CurrentWebSocketCloseCode},
        http::CreateChannelQuery,
        shared::{DownloadStates, SessionId, SessionInfo, SessionPermissions, StreamType},
        webrtc::{MediaKind, RtpCapabilities, RtpParameters},
    };
    use serde_json::json;

    /// Realistic client RTP capabilities (default codecs)
    fn test_client_rtp_capabilities() -> RtpCapabilities {
        RtpCapabilities(json!({
            "codecs": [
                {
                    "mimeType": "audio/opus",
                    "kind": "audio",
                    "preferredPayloadType": 111,
                    "clockRate": 48000,
                    "channels": 2,
                    "parameters": { "useinbandfec": "1" },
                    "rtcpFeedback": [{ "type": "transport-cc" }]
                },
                {
                    "mimeType": "video/VP8",
                    "kind": "video",
                    "preferredPayloadType": 96,
                    "clockRate": 90000,
                    "parameters": {},
                    "rtcpFeedback": [
                        { "type": "nack" },
                        { "type": "nack", "parameter": "pli" },
                        { "type": "ccm", "parameter": "fir" },
                        { "type": "goog-remb" },
                        { "type": "transport-cc" }
                    ]
                },
                {
                    "mimeType": "video/rtx",
                    "kind": "video",
                    "preferredPayloadType": 97,
                    "clockRate": 90000,
                    "parameters": { "apt": "96" },
                    "rtcpFeedback": []
                }
            ],
            "headerExtensions": [
                {
                    "uri": "urn:ietf:params:rtp-hdrext:sdes:mid",
                    "preferredId": 1,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
                    "preferredId": 4,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
                    "preferredId": 5,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                },
                {
                    "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
                    "preferredId": 10,
                    "preferredEncrypt": false,
                    "kind": "audio",
                    "direction": "sendrecv"
                }
            ]
        }))
    }

    fn test_audio_rtp_parameters() -> RtpParameters {
        RtpParameters(json!({
            "codecs": [{
                "mimeType": "audio/opus",
                "payloadType": 111,
                "clockRate": 48000,
                "channels": 2,
                "parameters": { "useinbandfec": "1" },
                "rtcpFeedback": [{ "type": "transport-cc" }]
            }],
            "headerExtensions": [
                { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
                { "uri": "urn:ietf:params:rtp-hdrext:ssrc-audio-level", "id": 10, "encrypt": false }
            ],
            "encodings": [{ "ssrc": 11111 }]
        }))
    }

    fn test_video_rtp_parameters() -> RtpParameters {
        RtpParameters(json!({
            "codecs": [
                {
                    "mimeType": "video/VP8",
                    "payloadType": 96,
                    "clockRate": 90000,
                    "parameters": {},
                    "rtcpFeedback": [
                        { "type": "nack" },
                        { "type": "nack", "parameter": "pli" },
                        { "type": "ccm", "parameter": "fir" },
                        { "type": "goog-remb" },
                        { "type": "transport-cc" }
                    ]
                },
                {
                    "mimeType": "video/rtx",
                    "payloadType": 97,
                    "clockRate": 90000,
                    "parameters": { "apt": "96" },
                    "rtcpFeedback": []
                }
            ],
            "headerExtensions": [
                { "uri": "urn:ietf:params:rtp-hdrext:sdes:mid", "id": 1, "encrypt": false },
                { "uri": "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time", "id": 4, "encrypt": false },
                { "uri": "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01", "id": 5, "encrypt": false }
            ],
            "encodings": [{ "ssrc": 22222 }]
        }))
    }

    fn test_sender() -> (
        mpsc::UnboundedSender<SessionOutbound>,
        mpsc::UnboundedReceiver<SessionOutbound>,
    ) {
        mpsc::unbounded_channel()
    }

    #[tokio::test]
    async fn channel_manager_is_idempotent_by_issuer() {
        let manager = ChannelManager::new();
        let query = CreateChannelQuery::default();
        let first = manager.create_or_get("issuer-a", None, &query).await;
        let second = manager
            .create_or_get("issuer-a", Some("ignored"), &query)
            .await;
        let third = manager.create_or_get("issuer-b", None, &query).await;
        assert_eq!(first.uuid(), second.uuid());
        assert_ne!(first.uuid(), third.uuid());
    }

    #[tokio::test]
    async fn channel_manager_lookup_by_uuid() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let fetched = manager.get_by_uuid(channel.uuid()).await;
        assert!(fetched.is_some());
        assert_eq!(
            fetched.map(|channel| channel.uuid().to_owned()),
            Some(channel.uuid().to_owned())
        );
        assert!(manager.get_by_uuid("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn channel_manager_join_session_reports_missing_channel() {
        let manager = ChannelManager::new();
        let (tx, _rx) = test_sender();
        let result = manager
            .join_session(
                "missing-channel",
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx,
                1,
            )
            .await;
        assert!(matches!(
            result,
            Err(ChannelManagerJoinError::MissingChannel)
        ));
    }

    #[tokio::test]
    async fn join_session_enforces_capacity() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, _rx1) = test_sender();
        let result = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                1,
            )
            .await;
        assert!(result.is_ok());

        let (tx2, _rx2) = test_sender();
        let result = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                1,
            )
            .await;
        assert_eq!(result, Err(ChannelJoinError::ChannelFull));
    }

    #[tokio::test]
    async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let first_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                1,
            )
            .await;
        assert!(first_connection.is_ok());
        assert_eq!(channel.router_session_count().await, 1);

        let (tx2, mut rx2) = test_sender();
        let second_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx2,
                1,
            )
            .await;
        assert!(second_connection.is_ok());
        assert_eq!(channel.router_session_count().await, 1);
        assert!(matches!(
            rx1.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        let Some(first_connection) = first_connection.ok() else {
            return;
        };
        let Some(second_connection) = second_connection.ok() else {
            return;
        };

        channel
            .leave_session(&SessionId::Integer(1), first_connection)
            .await;
        assert_eq!(channel.session_count().await, 1);
        assert_eq!(channel.router_session_count().await, 1);

        channel
            .broadcast(&SessionId::Integer(99), serde_json::json!("hello"))
            .await;
        let msg = rx2.try_recv();
        assert!(msg.is_ok(), "new sender should receive broadcast");

        channel
            .leave_session(&SessionId::Integer(1), second_connection)
            .await;
        assert_eq!(channel.session_count().await, 0);
        assert_eq!(channel.router_session_count().await, 0);
    }

    #[tokio::test]
    async fn leave_session_sends_departure_to_remaining_peers() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, _rx2) = test_sender();
        let alice_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let bob_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        assert!(alice_connection.is_ok());
        assert!(bob_connection.is_ok());
        let Some(bob_connection) = bob_connection.ok() else {
            return;
        };

        channel
            .leave_session(&SessionId::Integer(2), bob_connection)
            .await;

        let msg = rx1.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
            assert_eq!(payload.session_id, SessionId::Integer(2));
        } else {
            panic!("expected SessionDeparted, got {msg:?}");
        }
        assert_eq!(channel.session_count().await, 1);
    }

    #[tokio::test]
    async fn replacing_a_session_notifies_remaining_peers() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut alice_rx) = test_sender();
        let (tx2, mut bob_old_rx) = test_sender();
        let (tx3, _bob_new_rx) = test_sender();
        let _alice_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _bob_old_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let _bob_new_connection = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;
        assert!(matches!(
            bob_old_rx.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        let msg = alice_rx.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionDeparted(payload))) = msg {
            assert_eq!(payload.session_id, SessionId::Integer(2));
        } else {
            panic!("expected SessionDeparted, got {msg:?}");
        }
        assert_eq!(channel.session_count().await, 2);
    }

    #[tokio::test]
    async fn broadcast_reaches_all_except_sender() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let (tx3, mut rx3) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(3),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;

        channel
            .broadcast(&SessionId::Integer(2), serde_json::json!({"text": "hi"}))
            .await;

        assert!(rx1.try_recv().is_ok(), "session 1 should receive broadcast");
        assert!(
            rx2.try_recv().is_err(),
            "sender (session 2) should NOT receive own broadcast"
        );
        assert!(rx3.try_recv().is_ok(), "session 3 should receive broadcast");
    }

    #[tokio::test]
    async fn update_session_info_broadcasts_to_all() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let info = SessionInfo {
            is_talking: Some(true),
            ..SessionInfo::default()
        };
        channel
            .update_session_info(&SessionId::Integer(1), info, false)
            .await;

        let msg1 = rx1.try_recv();
        let msg2 = rx2.try_recv();
        assert!(msg1.is_ok());
        assert!(msg2.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) =
            msg1
        {
            assert!(snapshot.contains_key("1"));
            assert_eq!(
                snapshot.get("1").and_then(|info| info.is_talking),
                Some(true)
            );
        } else {
            panic!("expected SessionInfoChanged");
        }
    }

    #[tokio::test]
    async fn update_session_info_with_refresh_sends_full_snapshot() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, _rx2) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;

        let info = SessionInfo {
            is_camera_on: Some(true),
            ..SessionInfo::default()
        };
        channel
            .update_session_info(&SessionId::Integer(1), info, true)
            .await;

        let msg = rx1.try_recv();
        assert!(msg.is_ok());
        if let Ok(SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot))) =
            msg
        {
            assert_eq!(
                snapshot.len(),
                2,
                "full refresh should include all sessions"
            );
            assert!(snapshot.contains_key("1"));
            assert!(snapshot.contains_key("2"));
        } else {
            panic!("expected SessionInfoChanged with full snapshot");
        }
    }

    #[tokio::test]
    async fn disconnect_sessions_kicks_targets_and_notifies_remaining() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let (tx3, mut rx3) = test_sender();
        let _ = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        let _ = channel
            .join_session(
                SessionId::Integer(3),
                None,
                SessionPermissions::default(),
                tx3,
                10,
            )
            .await;

        channel
            .disconnect_sessions(&[SessionId::Integer(1), SessionId::Integer(2)])
            .await;

        let msg1 = rx1.try_recv();
        assert!(msg1.is_ok());
        assert!(matches!(
            msg1.ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        let msg2 = rx2.try_recv();
        assert!(msg2.is_ok());
        assert!(matches!(
            msg2.ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        let departure1 = rx3.try_recv();
        let departure2 = rx3.try_recv();
        assert!(departure1.is_ok());
        assert!(departure2.is_ok());

        assert_eq!(channel.session_count().await, 1);
    }

    #[tokio::test]
    async fn disconnect_sessions_target_only_the_active_replaced_session() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, mut rx1) = test_sender();
        let (tx2, mut rx2) = test_sender();
        let first_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await;
        let second_connection = channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await;
        assert!(first_connection.is_ok());
        assert!(second_connection.is_ok());
        assert!(matches!(
            rx1.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));

        channel.disconnect_sessions(&[SessionId::Integer(1)]).await;

        assert!(matches!(
            rx2.try_recv().ok(),
            Some(SessionOutbound::Close(CurrentWebSocketCloseCode::Kicked))
        ));
        assert!(rx1.try_recv().is_err());
        assert_eq!(channel.session_count().await, 0);
        assert_eq!(channel.router_session_count().await, 0);
    }

    #[tokio::test]
    async fn channel_maps_string_session_ids_into_router_sessions() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx, _rx) = test_sender();
        let joined = channel
            .join_session(
                SessionId::String("guest-1".to_owned()),
                None,
                SessionPermissions::default(),
                tx,
                10,
            )
            .await;
        assert!(joined.is_ok());
        assert_eq!(channel.session_count().await, 1);
        assert_eq!(channel.router_session_count().await, 1);

        let Some(connection_id) = joined.ok() else {
            return;
        };

        channel
            .leave_session(&SessionId::String("guest-1".to_owned()), connection_id)
            .await;
        assert_eq!(channel.session_count().await, 0);
        assert_eq!(channel.router_session_count().await, 0);
    }

    #[tokio::test]
    async fn channel_keeps_router_session_permissions_in_sync() {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let permissions = SessionPermissions {
            transcription: Some(true),
            audio_recording: Some(false),
            video_recording: Some(true),
        };
        let (first_tx, _first_rx) = test_sender();
        let joined = channel
            .join_session(
                SessionId::Integer(1),
                None,
                permissions.clone(),
                first_tx,
                10,
            )
            .await;
        assert!(joined.is_ok());
        assert_eq!(
            channel
                .router_session_permissions(&SessionId::Integer(1))
                .await,
            Some(RouterSessionPermissions::new(true, false, true))
        );

        let replacement_permissions = SessionPermissions {
            transcription: Some(false),
            audio_recording: Some(true),
            video_recording: Some(false),
        };
        let (second_tx, _second_rx) = test_sender();
        let replaced = channel
            .join_session(
                SessionId::Integer(1),
                None,
                replacement_permissions,
                second_tx,
                10,
            )
            .await;
        assert!(replaced.is_ok());
        assert_eq!(
            channel
                .router_session_permissions(&SessionId::Integer(1))
                .await,
            Some(RouterSessionPermissions::new(false, true, false))
        );
    }

    #[tokio::test]
    async fn manager_leave_session_removes_empty_channel() {
        let manager = ChannelManager::new();
        let first_channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let channel_uuid = first_channel.uuid().to_owned();
        let (tx, _rx) = test_sender();
        let joined = manager
            .join_session(
                &channel_uuid,
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx,
                1,
            )
            .await;
        assert!(joined.is_ok());
        let Some((_channel, connection_id)) = joined.ok() else {
            return;
        };

        manager
            .leave_session(&channel_uuid, &SessionId::Integer(1), connection_id)
            .await;

        assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
        let replacement = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        assert_ne!(replacement.uuid(), channel_uuid);
    }

    #[tokio::test]
    async fn manager_disconnect_sessions_removes_empty_channel() {
        let manager = ChannelManager::new();
        let first_channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let channel_uuid = first_channel.uuid().to_owned();
        let (tx, _rx) = test_sender();
        let joined = manager
            .join_session(
                &channel_uuid,
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx,
                1,
            )
            .await;
        assert!(joined.is_ok());

        manager
            .disconnect_sessions(&channel_uuid, &[SessionId::Integer(1)])
            .await;

        assert!(manager.get_by_uuid(&channel_uuid).await.is_none());
        let replacement = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        assert_ne!(replacement.uuid(), channel_uuid);
    }

    fn stub_adapter() -> (RuntimeTransportAdapter, Arc<StubWebRtcAdapter>) {
        let adapter = Arc::new(StubWebRtcAdapter::default());
        (
            RuntimeTransportAdapter::from_stub_adapter(Arc::clone(&adapter)),
            adapter,
        )
    }

    /// Set up a channel with two joined sessions that both have upload and download
    /// transports connected plus client RTP capabilities, ready for publish/consume tests.
    async fn setup_two_ready_sessions() -> (
        Arc<super::super::Channel>,
        RuntimeTransportAdapter,
        mpsc::UnboundedReceiver<SessionOutbound>,
        mpsc::UnboundedReceiver<SessionOutbound>,
    ) {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (tx1, rx1) = test_sender();
        let (tx2, rx2) = test_sender();
        channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                tx1,
                10,
            )
            .await
            .unwrap();
        channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                tx2,
                10,
            )
            .await
            .unwrap();
        let (adapter, _stub) = stub_adapter();
        for session_id in &[SessionId::Integer(1), SessionId::Integer(2)] {
            channel
                .set_transport_connected(session_id, TransportConnectDirection::Upload)
                .await;
            channel
                .set_transport_connected(session_id, TransportConnectDirection::Download)
                .await;
            channel
                .set_client_rtp_capabilities(session_id, test_client_rtp_capabilities())
                .await;
        }
        (channel, adapter, rx1, rx2)
    }

    async fn setup_late_join_bootstrap_scenario() -> (
        Arc<super::super::Channel>,
        RuntimeTransportAdapter,
        Arc<StubWebRtcAdapter>,
        mpsc::UnboundedReceiver<SessionOutbound>,
        mpsc::UnboundedReceiver<SessionOutbound>,
    ) {
        let manager = ChannelManager::new();
        let channel = manager
            .create_or_get("issuer-a", None, &CreateChannelQuery::default())
            .await;
        let (publisher_tx, publisher_rx) = test_sender();
        let (subscriber_tx, subscriber_rx) = test_sender();
        channel
            .join_session(
                SessionId::Integer(1),
                None,
                SessionPermissions::default(),
                publisher_tx,
                10,
            )
            .await
            .unwrap();
        channel
            .join_session(
                SessionId::Integer(2),
                None,
                SessionPermissions::default(),
                subscriber_tx,
                10,
            )
            .await
            .unwrap();

        let (transport_adapter, stub) = stub_adapter();
        channel
            .set_transport_connected(&SessionId::Integer(1), TransportConnectDirection::Upload)
            .await;
        channel
            .set_transport_connected(&SessionId::Integer(1), TransportConnectDirection::Download)
            .await;
        channel
            .set_client_rtp_capabilities(&SessionId::Integer(1), test_client_rtp_capabilities())
            .await;
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &transport_adapter,
            )
            .await;

        (
            channel,
            transport_adapter,
            stub,
            publisher_rx,
            subscriber_rx,
        )
    }

    fn drain_outbound(rx: &mut mpsc::UnboundedReceiver<SessionOutbound>) -> Vec<SessionOutbound> {
        let mut msgs = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            msgs.push(msg);
        }
        msgs
    }

    async fn wait_for_stub_event(
        adapter: &StubWebRtcAdapter,
        predicate: impl Fn(&StubWebRtcEvent) -> bool,
    ) {
        let wait_result = timeout(Duration::from_secs(1), async {
            loop {
                if adapter.snapshot_events().iter().any(&predicate) {
                    break;
                }
                yield_now().await;
            }
        })
        .await;
        assert!(
            wait_result.is_ok(),
            "timed out waiting for stub transport event"
        );
    }

    #[tokio::test]
    async fn production_change_pauses_producer_and_broadcasts_info() {
        let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        // Session 1 publishes a camera track.
        let producer_id = channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;
        assert!(producer_id.is_some());

        // Drain the INIT_CONSUMER bootstrap that went to session 2.
        let bootstrap_msgs = drain_outbound(&mut rx2);
        assert!(
            bootstrap_msgs
                .iter()
                .any(|m| matches!(m, SessionOutbound::Request(..))),
            "session 2 should have received a bootstrap remote track request"
        );
        // Session 1 shouldn't get its own consumer.
        assert!(drain_outbound(&mut rx1).is_empty());

        // Now session 1 sends PRODUCTION_CHANGE: camera off (pause).
        channel
            .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false)
            .await;

        // Both sessions should receive a session info broadcast with isCameraOn = false.
        let msgs1 = drain_outbound(&mut rx1);
        let msgs2 = drain_outbound(&mut rx2);
        assert_eq!(msgs1.len(), 1, "session 1 should get info broadcast");
        assert_eq!(msgs2.len(), 1, "session 2 should get info broadcast");

        // Verify the broadcast contains isCameraOn = false.
        let info_msg = &msgs1[0];
        if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) =
            info_msg
        {
            let info = snapshot
                .values()
                .next()
                .expect("snapshot should have one entry");
            assert_eq!(info.is_camera_on, Some(false));
        } else {
            panic!("expected SessionInfoChanged, got {info_msg:?}");
        }

        // Resume: session 1 sends PRODUCTION_CHANGE: camera on.
        channel
            .update_upload_state(&SessionId::Integer(1), StreamType::Camera, true)
            .await;

        let msgs1 = drain_outbound(&mut rx1);
        if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) =
            &msgs1[0]
        {
            let info = snapshot.values().next().unwrap();
            assert_eq!(info.is_camera_on, Some(true));
        } else {
            panic!("expected SessionInfoChanged after resume");
        }
    }

    #[tokio::test]
    async fn publish_track_releases_channel_lock_while_waiting_on_transport_adapter() {
        let (channel, _adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;
        let (stub_adapter, _) = stub_adapter();
        let RuntimeTransportAdapter::Stub(stub) = &stub_adapter else {
            panic!("expected stub transport adapter");
        };
        stub.set_publish_media_delay(Some(Duration::from_millis(200)));

        let publish_task = tokio::spawn({
            let channel = Arc::clone(&channel);
            let adapter = stub_adapter.clone();
            async move {
                channel
                    .publish_track(
                        &SessionId::Integer(1),
                        StreamType::Camera,
                        MediaKind::Video,
                        test_video_rtp_parameters(),
                        &adapter,
                    )
                    .await
            }
        });

        wait_for_stub_event(stub, |event| {
            matches!(
                event,
                StubWebRtcEvent::PublishMediaRequested {
                    session_id: SessionId::Integer(1),
                    media_kind: MediaKind::Video,
                }
            )
        })
        .await;

        let update_result = timeout(Duration::from_millis(50), async {
            channel
                .update_session_info(
                    &SessionId::Integer(2),
                    SessionInfo {
                        is_talking: Some(true),
                        ..SessionInfo::default()
                    },
                    false,
                )
                .await;
        })
        .await;
        assert!(
            update_result.is_ok(),
            "session info update should not wait for publish transport declaration"
        );

        assert!(publish_task.await.unwrap().is_some());
        assert!(
            drain_outbound(&mut rx1).iter().any(|msg| matches!(
                msg,
                SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
            )),
            "publisher should still receive the concurrent info broadcast"
        );
        assert!(
            drain_outbound(&mut rx2).iter().any(|msg| matches!(
                msg,
                SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
            )),
            "peer should still receive the concurrent info broadcast"
        );
    }

    #[tokio::test]
    async fn production_change_updates_screen_sharing_info() {
        let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Screen,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;

        // Drain bootstrap messages.
        drain_outbound(&mut rx1);
        drain_outbound(&mut rx2);

        // Pause screen sharing.
        channel
            .update_upload_state(&SessionId::Integer(1), StreamType::Screen, false)
            .await;

        let msgs = drain_outbound(&mut rx1);
        if let SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(snapshot)) =
            &msgs[0]
        {
            let info = snapshot.values().next().unwrap();
            assert_eq!(info.is_screen_sharing_on, Some(false));
        } else {
            panic!("expected SessionInfoChanged for screen sharing");
        }
    }

    #[tokio::test]
    async fn late_join_bootstrap_releases_channel_lock_while_waiting_on_transport_adapter() {
        let (channel, transport_adapter, stub, mut publisher_rx, mut subscriber_rx) =
            setup_late_join_bootstrap_scenario().await;
        drain_outbound(&mut publisher_rx);
        drain_outbound(&mut subscriber_rx);

        channel
            .set_transport_connected(&SessionId::Integer(2), TransportConnectDirection::Download)
            .await;
        channel
            .set_client_rtp_capabilities(&SessionId::Integer(2), test_client_rtp_capabilities())
            .await;
        stub.set_consume_media_delay(Some(Duration::from_millis(200)));

        let bootstrap_task = tokio::spawn({
            let channel = Arc::clone(&channel);
            let adapter = transport_adapter.clone();
            async move {
                channel
                    .bootstrap_late_join_consumers(&SessionId::Integer(2), &adapter)
                    .await;
            }
        });

        wait_for_stub_event(&stub, |event| {
            matches!(
                event,
                StubWebRtcEvent::ConsumeMediaRequested {
                    consumer_session_id: SessionId::Integer(2),
                    source_session_id: SessionId::Integer(1),
                    media_kind: MediaKind::Video,
                }
            )
        })
        .await;

        let update_result = timeout(Duration::from_millis(50), async {
            channel
                .update_session_info(
                    &SessionId::Integer(1),
                    SessionInfo {
                        is_talking: Some(true),
                        ..SessionInfo::default()
                    },
                    false,
                )
                .await;
        })
        .await;
        assert!(
            update_result.is_ok(),
            "session info update should not wait for late-join consumer declaration"
        );

        bootstrap_task.await.unwrap();
        assert!(
            drain_outbound(&mut publisher_rx).iter().any(|msg| matches!(
                msg,
                SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
            )),
            "publisher should still receive the concurrent info broadcast"
        );
        assert!(
            drain_outbound(&mut subscriber_rx)
                .iter()
                .any(|msg| matches!(
                    msg,
                    SessionOutbound::Message(CurrentServerMessage::SessionInfoChanged(_))
                        | SessionOutbound::Request(_)
                )),
            "late joiner should receive outbound traffic while bootstrap is running"
        );
    }

    #[tokio::test]
    async fn production_change_ignores_unknown_stream_type() {
        let (channel, _adapter, mut rx1, mut _rx2) = setup_two_ready_sessions().await;

        // No producer published for audio. PRODUCTION_CHANGE should be a no-op.
        channel
            .update_upload_state(&SessionId::Integer(1), StreamType::Audio, false)
            .await;

        assert!(
            drain_outbound(&mut rx1).is_empty(),
            "no broadcast expected when no producer exists for the stream type"
        );
    }

    #[tokio::test]
    async fn consumption_change_pauses_and_resumes_consumer() {
        let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        // Session 1 publishes a camera track.
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;

        // Drain bootstrap.
        drain_outbound(&mut rx1);
        drain_outbound(&mut rx2);

        // Session 2 sends CONSUMPTION_CHANGE: pause camera from session 1.
        channel
            .update_download_state(
                &SessionId::Integer(2),
                &SessionId::Integer(1),
                &DownloadStates {
                    camera: Some(false),
                    audio: None,
                    screen: None,
                },
            )
            .await;

        // No outbound messages expected — consumer pause is silent (matches Node SFU).
        assert!(drain_outbound(&mut rx1).is_empty());
        assert!(drain_outbound(&mut rx2).is_empty());

        // Session 2 sends CONSUMPTION_CHANGE: resume camera from session 1.
        channel
            .update_download_state(
                &SessionId::Integer(2),
                &SessionId::Integer(1),
                &DownloadStates {
                    camera: Some(true),
                    audio: None,
                    screen: None,
                },
            )
            .await;

        // Still no outbound — resume is also silent.
        assert!(drain_outbound(&mut rx1).is_empty());
        assert!(drain_outbound(&mut rx2).is_empty());
    }

    #[tokio::test]
    async fn consumption_change_ignores_nonexistent_consumer() {
        let (channel, _adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        // No tracks published. CONSUMPTION_CHANGE should be a no-op.
        channel
            .update_download_state(
                &SessionId::Integer(2),
                &SessionId::Integer(1),
                &DownloadStates {
                    camera: Some(false),
                    audio: Some(false),
                    screen: None,
                },
            )
            .await;

        assert!(drain_outbound(&mut rx1).is_empty());
        assert!(drain_outbound(&mut rx2).is_empty());
    }

    #[tokio::test]
    async fn consumption_change_handles_multiple_stream_types() {
        let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        // Session 1 publishes both camera and audio.
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Audio,
                MediaKind::Audio,
                test_audio_rtp_parameters(),
                &adapter,
            )
            .await;

        drain_outbound(&mut rx1);
        drain_outbound(&mut rx2);

        // Session 2 pauses both in one message.
        channel
            .update_download_state(
                &SessionId::Integer(2),
                &SessionId::Integer(1),
                &DownloadStates {
                    camera: Some(false),
                    audio: Some(false),
                    screen: None,
                },
            )
            .await;

        // No-op outbound (consumer pause is silent).
        assert!(drain_outbound(&mut rx2).is_empty());
    }

    #[tokio::test]
    async fn session_leave_purges_producer_and_consumer_indexes() {
        let (channel, adapter, mut rx1, mut rx2) = setup_two_ready_sessions().await;

        // Session 1 publishes camera, which creates a consumer for session 2.
        channel
            .publish_track(
                &SessionId::Integer(1),
                StreamType::Camera,
                MediaKind::Video,
                test_video_rtp_parameters(),
                &adapter,
            )
            .await;
        drain_outbound(&mut rx1);
        drain_outbound(&mut rx2);

        // Session 1 leaves.
        let connection_id = 0; // first join gets connection_id 0
        channel
            .leave_session(&SessionId::Integer(1), connection_id)
            .await;

        // After session 1 leaves, a consumption change targeting session 1's
        // producer should be a no-op (the consumer index entry was cleaned up).
        channel
            .update_download_state(
                &SessionId::Integer(2),
                &SessionId::Integer(1),
                &DownloadStates {
                    camera: Some(false),
                    audio: None,
                    screen: None,
                },
            )
            .await;

        // Similarly, a production change for session 1 should be a no-op.
        channel
            .update_upload_state(&SessionId::Integer(1), StreamType::Camera, false)
            .await;

        // No crashes, no stale state — both operations are silent no-ops.
        drain_outbound(&mut rx2);
    }
}
