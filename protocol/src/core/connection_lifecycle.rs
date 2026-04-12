use crate::{
    bundle_api::BundleConnectionState, shared::RecordingState, signaling::WebSocketCloseCode,
};

use super::{
    Command, Commands, ConnectContext, INITIAL_RECOVERY_DELAY_MS, ProtocolCore, RECOVERY_TIMER_ID,
    close_cause, empty_features, next_recovery_delay,
};

pub(super) fn connect(
    core: &mut ProtocolCore,
    url: String,
    jwt: String,
    channel: Option<String>,
) -> Commands {
    if !matches!(
        core.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return Vec::new();
    }
    core.connect_context = Some(ConnectContext {
        url: url.clone(),
        jwt,
        channel,
    });
    core.features = empty_features();
    core.recording_state = RecordingState::default();
    core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    core.clear_sticky_state();
    core.clear_runtime_state();
    core.state = BundleConnectionState::Connecting;
    vec![
        Command::EmitStateChange {
            state: core.state,
            cause: None,
        },
        Command::Connect { url },
    ]
}

pub(super) fn on_transport_ready(core: &mut ProtocolCore) -> Commands {
    if core.state != BundleConnectionState::Authenticated {
        return Vec::new();
    }
    core.state = BundleConnectionState::Connected;
    vec![Command::EmitStateChange {
        state: core.state,
        cause: None,
    }]
}

pub(super) fn disconnect(core: &mut ProtocolCore) -> Commands {
    if matches!(
        core.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return Vec::new();
    }
    core.state = BundleConnectionState::Disconnected;
    core.connect_context = None;
    core.features = empty_features();
    core.recording_state = RecordingState::default();
    core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    core.clear_sticky_state();

    let mut commands = vec![Command::CancelTimer {
        id: RECOVERY_TIMER_ID,
    }];
    commands.extend(core.clear_runtime_state_with_commands());
    commands.extend([
        Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::Clean),
        },
        Command::ClosePeerConnection,
        Command::EmitStateChange {
            state: core.state,
            cause: None,
        },
    ]);
    commands
}

pub(super) fn on_ws_close(core: &mut ProtocolCore, close_code: u16) -> Commands {
    if matches!(
        core.state,
        BundleConnectionState::Disconnected | BundleConnectionState::Closed
    ) {
        return Vec::new();
    }

    let mut commands = Vec::new();
    commands.extend(core.clear_runtime_state_with_commands());

    if let Some(
        WebSocketCloseCode::AuthFailed
        | WebSocketCloseCode::Kicked
        | WebSocketCloseCode::ChannelFull,
    ) = WebSocketCloseCode::from_u16(close_code)
    {
        core.state = BundleConnectionState::Closed;
        core.connect_context = None;
        core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        commands.extend([
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: core.state,
                cause: close_cause(close_code).map(str::to_owned),
            },
        ]);
        return commands;
    }

    let Some(connect_context) = core.connect_context.as_ref() else {
        core.state = BundleConnectionState::Disconnected;
        commands.push(Command::EmitStateChange {
            state: core.state,
            cause: None,
        });
        return commands;
    };
    let _ = connect_context;
    let delay_ms = core.recovery_delay_ms;
    core.recovery_delay_ms = next_recovery_delay(delay_ms);
    core.state = BundleConnectionState::Recovering;
    commands.extend([
        Command::ClosePeerConnection,
        Command::EmitStateChange {
            state: core.state,
            cause: None,
        },
        Command::ScheduleTimer {
            id: RECOVERY_TIMER_ID,
            ms: delay_ms,
        },
    ]);
    commands
}

pub(super) fn handle_recovery_timer(core: &mut ProtocolCore) -> Commands {
    if core.state != BundleConnectionState::Recovering {
        return Vec::new();
    }
    let Some(connect_context) = core.connect_context.as_ref() else {
        return Vec::new();
    };
    core.state = BundleConnectionState::Connecting;
    vec![
        Command::EmitStateChange {
            state: core.state,
            cause: None,
        },
        Command::Connect {
            url: connect_context.url.clone(),
        },
    ]
}
