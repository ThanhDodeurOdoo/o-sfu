#![no_main]

//! fuzzes short packet-loop turn traces through the production verification seam
//!
//! the target mixes scratch warmup, relay packet input and idle turns because
//! these actions share packet-loop scratch and effect indexes, so random
//! ordering is good at finding stale references after otherwise harmless
//! refactors

use libfuzzer_sys::{
    arbitrary::{Arbitrary, Error, Unstructured},
    fuzz_target,
};
use o_sfu_core::{
    ConnectionId, RoomInstanceId,
    server::{
        session::UserId,
        transport::packet_loop_verification::{
            PacketLoopEffects, PacketLoopRouteSnapshot, PacketLoopScratch, PacketLoopState,
            PacketLoopTime, PacketLoopTurn, PacketLoopTurnInput, sample_forwarded_packet,
        },
    },
    transport::TransportSessionKey,
};

const MAX_STEPS: usize = 32;
const MAX_PAYLOAD_LEN: usize = 96;

#[derive(Debug)]
struct Scenario {
    steps: Vec<Step>,
}

impl<'a> Arbitrary<'a> for Scenario {
    fn arbitrary(input: &mut Unstructured<'a>) -> Result<Self, Error> {
        let step_count = input.int_in_range(0..=MAX_STEPS)?;
        let mut steps = Vec::with_capacity(step_count);
        for _ in 0..step_count {
            steps.push(input.arbitrary()?);
        }
        Ok(Self { steps })
    }
}

#[derive(Debug)]
enum Step {
    Warm {
        items: u8,
        payload_len: u8,
    },
    ClearScratch,
    Relay {
        connection_id: u8,
        payload: Vec<u8>,
    },
    Step {
        now_ms: u16,
    },
}

impl<'a> Arbitrary<'a> for Step {
    fn arbitrary(input: &mut Unstructured<'a>) -> Result<Self, Error> {
        match input.int_in_range(0..=3)? {
            0 => Ok(Self::Warm {
                items: input.arbitrary()?,
                payload_len: input.arbitrary()?,
            }),
            1 => Ok(Self::ClearScratch),
            2 => {
                let payload_len = input.int_in_range(0..=MAX_PAYLOAD_LEN)?;
                let mut payload = vec![0; payload_len];
                input.fill_buffer(&mut payload)?;
                Ok(Self::Relay {
                    connection_id: input.arbitrary()?,
                    payload,
                })
            }
            3 => Ok(Self::Step {
                now_ms: input.arbitrary()?,
            }),
            _ => unreachable!("bounded step selector"),
        }
    }
}

fuzz_target!(|scenario: Scenario| {
    let mut state = PacketLoopState::default();
    let mut scratch = PacketLoopScratch::new();
    let mut effects = PacketLoopEffects::default();
    let mut session_outputs = Vec::new();
    let mut relay_packets = Vec::new();
    let routes = PacketLoopRouteSnapshot::default();

    for step in scenario.steps {
        match step {
            Step::Warm { items, payload_len } => {
                let payload = vec![0xA5; usize::from(payload_len)];
                for idx in 0..items {
                    let port = 40_000_u16.saturating_add(u16::from(idx));
                    scratch.push_pending_transmit(
                        std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                        &payload,
                    );
                    scratch.mark_source_policy_dirty(RoomInstanceId::from_raw(u64::from(idx)));
                }
            }
            Step::ClearScratch => {
                scratch.clear();
                assert!(scratch.is_turn_empty());
            }
            Step::Relay {
                connection_id,
                payload,
            } => {
                relay_packets.push(sample_forwarded_packet(
                    session_key(1, u64::from(connection_id)),
                    "v0",
                    &payload,
                ));
            }
            Step::Step { now_ms } => {
                PacketLoopTurn::step(
                    &mut state,
                    &mut scratch,
                    &mut effects,
                    PacketLoopTurnInput::new(
                        PacketLoopTime::from_millis(u64::from(now_ms)),
                        &mut session_outputs,
                        &mut relay_packets,
                        &routes,
                    ),
                );
                assert_eq!(effects.invalid_reference_count(&scratch), 0);
            }
        }
    }
});

fn session_key(room_instance_id: u64, connection_id: u64) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        0,
        ConnectionId::from_raw(connection_id),
        UserId::Integer(i64::try_from(connection_id).unwrap_or(i64::MAX)),
    )
}
