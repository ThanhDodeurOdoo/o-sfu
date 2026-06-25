//! Verification projection for the protocol connection lifecycle.
//!
//! The proof surface drives real `ProtocolCore` transitions, then projects only
//! the state and command counts needed by the Kani recovery harnesses.

use std::mem::ManuallyDrop;

use super::{
    Command, Commands, ConnectionState, ProtocolCore, RECOVERY_TIMER_ID, connection_lifecycle,
    empty_features,
};
use crate::{
    shared::{RecordingState, StreamType},
    signaling::WelcomePayload,
};

const VERIFICATION_URL: &str = "wss://sfu.example.test/socket";
const VERIFICATION_JWT: &str = "signed-token";
const PROJECTED_COMMANDS: usize = 8;

#[derive(Default)]
pub struct VerificationLifecycleEffects {
    has_commands: bool,
    connect_count: usize,
    recovery_timer_ms: Option<u32>,
    recovery_timer_count: usize,
    close_peer_connection_count: usize,
}

impl VerificationLifecycleEffects {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.has_commands
    }

    #[must_use]
    pub fn connect_count(&self) -> usize {
        self.connect_count
    }

    #[must_use]
    pub fn recovery_timer_count(&self) -> usize {
        self.recovery_timer_count
    }

    #[must_use]
    pub fn recovery_timer_delay(&self) -> Option<u32> {
        self.recovery_timer_ms
    }

    #[must_use]
    pub fn close_peer_connection_count(&self) -> usize {
        self.close_peer_connection_count
    }
}

#[derive(Default)]
pub struct VerificationConnectionLifecycle {
    core: ProtocolCore,
}

impl VerificationConnectionLifecycle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&mut self) -> VerificationLifecycleEffects {
        project_effects(connection_lifecycle::connect(
            &mut self.core,
            VERIFICATION_URL.to_owned(),
            VERIFICATION_JWT.to_owned(),
            None,
        ))
    }

    pub fn on_transport_ready(&mut self) -> VerificationLifecycleEffects {
        project_effects(connection_lifecycle::on_transport_ready(&mut self.core))
    }

    pub fn on_welcome(&mut self) -> VerificationLifecycleEffects {
        project_effects(self.core.accept_welcome(empty_welcome()))
    }

    pub fn disconnect(&mut self) -> VerificationLifecycleEffects {
        project_effects(connection_lifecycle::disconnect(&mut self.core))
    }

    pub fn on_ws_close(&mut self, close_code: u16) -> VerificationLifecycleEffects {
        project_effects(connection_lifecycle::on_ws_close(
            &mut self.core,
            close_code,
        ))
    }

    pub fn on_timer(&mut self, timer_id: u32) -> VerificationLifecycleEffects {
        if timer_id == RECOVERY_TIMER_ID {
            return project_effects(connection_lifecycle::handle_recovery_timer(&mut self.core));
        }
        VerificationLifecycleEffects::default()
    }

    pub fn seed_sticky_replay(&mut self) {
        self.core
            .sticky_replay
            .set_publish_active(StreamType::Camera, true);
    }

    pub fn seed_source_snapshot(&mut self) {
        self.core.has_source_descriptors = true;
    }

    #[must_use]
    pub fn state(&self) -> ConnectionState {
        self.core.state()
    }

    #[must_use]
    pub fn has_connect_context(&self) -> bool {
        self.core.connect_context.is_some()
    }

    #[must_use]
    pub fn has_sticky_replay(&self) -> bool {
        !self.core.sticky_replay.is_empty()
    }

    #[must_use]
    pub fn has_source_snapshot(&self) -> bool {
        self.core.has_source_descriptors
    }
}

fn project_effects(commands: Commands) -> VerificationLifecycleEffects {
    let commands = ManuallyDrop::new(commands);
    assert!(commands.len() <= PROJECTED_COMMANDS);
    let mut effects = VerificationLifecycleEffects {
        has_commands: !commands.is_empty(),
        ..VerificationLifecycleEffects::default()
    };
    for command in commands.iter() {
        project_command(command, &mut effects);
    }
    effects
}

fn project_command(command: &Command, effects: &mut VerificationLifecycleEffects) {
    match command {
        Command::Connect { .. } => effects.connect_count += 1,
        Command::ScheduleTimer { id, ms } if *id == RECOVERY_TIMER_ID => {
            effects.recovery_timer_ms = Some(*ms);
            effects.recovery_timer_count += 1;
        }
        Command::ClosePeerConnection => effects.close_peer_connection_count += 1,
        _ => {}
    }
}

fn empty_welcome() -> WelcomePayload {
    WelcomePayload {
        features: empty_features(),
        recording: RecordingState::default(),
        peers: Vec::new(),
    }
}
