#![no_main]

//! fuzzes the packet-loop cache for recently missed routes
//!
//! the target mutates miss recording, topology clearing, fallback success and
//! source rate-limit checks around one packet identity because a wrong cached
//! miss can waste CPU on repeated fallback scans or hide a valid route after
//! network or topology churn

use std::net::SocketAddr;

use libfuzzer_sys::{
    arbitrary,
    arbitrary::{Arbitrary, Error, Unstructured},
    fuzz_target,
};
use o_sfu_core::server::transport::packet_loop_verification::{
    PacketLoopRoutingMissKey, PacketLoopRoutingState, PacketLoopTime,
};

const MAX_PACKET_LEN: usize = 96;
const MAX_STEPS: usize = 24;

#[derive(Debug)]
struct Scenario {
    source_port: u16,
    candidate_port: u16,
    packet: Vec<u8>,
    steps: Vec<Step>,
}

impl<'a> Arbitrary<'a> for Scenario {
    fn arbitrary(input: &mut Unstructured<'a>) -> Result<Self, Error> {
        let packet_len = input.int_in_range(0..=MAX_PACKET_LEN)?;
        let mut packet = vec![0; packet_len];
        input.fill_buffer(&mut packet)?;
        let step_count = input.int_in_range(0..=MAX_STEPS)?;
        let mut steps = Vec::with_capacity(step_count);
        for _ in 0..step_count {
            steps.push(input.arbitrary()?);
        }
        Ok(Self {
            source_port: input.arbitrary()?,
            candidate_port: input.arbitrary()?,
            packet,
            steps,
        })
    }
}

#[derive(Debug, Arbitrary)]
enum Step {
    RecordMiss { now_ms: u16 },
    RouteSuccess,
    ClearTopology,
    CheckExact,
    CheckMutated,
    RateLimit { now_ms: u16 },
}

fuzz_target!(|scenario: Scenario| {
    let source = SocketAddr::from(([127, 0, 0, 1], scenario.source_port));
    let candidate = SocketAddr::from(([127, 0, 0, 1], scenario.candidate_port));
    let mut demux = PacketLoopRoutingState::new();

    for step in scenario.steps {
        let miss_key = PacketLoopRoutingMissKey::new(source, candidate, &scenario.packet);
        match step {
            Step::RecordMiss { now_ms } => {
                demux.record_miss(
                    miss_key,
                    &scenario.packet,
                    source,
                    PacketLoopTime::from_millis(u64::from(now_ms)),
                );
            }
            Step::RouteSuccess => {
                demux.record_fallback_route_success(miss_key, &scenario.packet, source);
                assert!(!demux.should_skip_scan(miss_key, &scenario.packet));
            }
            Step::ClearTopology => {
                demux.clear_on_topology_change();
                assert!(!demux.should_skip_scan(miss_key, &scenario.packet));
            }
            Step::CheckExact => {
                let _ = demux.should_skip_scan(miss_key, &scenario.packet);
            }
            Step::CheckMutated => {
                let mut mutated = scenario.packet.clone();
                if let Some(first) = mutated.first_mut() {
                    *first = first.wrapping_add(1);
                } else {
                    mutated.push(1);
                }
                let mutated_key = PacketLoopRoutingMissKey::new(source, candidate, &mutated);
                assert!(!demux.should_skip_scan(mutated_key, &mutated));
            }
            Step::RateLimit { now_ms } => {
                let _ = demux.should_rate_limit_source(
                    source,
                    PacketLoopTime::from_millis(u64::from(now_ms)),
                );
            }
        }
    }
});
