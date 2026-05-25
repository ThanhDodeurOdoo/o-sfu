#![no_main]

//! fuzz target for packet-loop UDP ingress demux
//!
//! the target drives malformed packets, cached source pins, stale pins and
//! repeated recent misses through the same packet-loop routing helper used by
//! the runtime worker

use libfuzzer_sys::{
    arbitrary::{Arbitrary, Unstructured},
    fuzz_target,
};
use o_sfu_core::server::transport::fuzz_support::route_packet_loop_ingress_demux;

const MAX_PACKET_LEN: usize = 1500;

#[derive(Debug)]
struct IngressDemuxInput<'a> {
    mode: u8,
    source_port: u16,
    candidate_port: u16,
    repeats: u8,
    packet: &'a [u8],
}

impl<'a> Arbitrary<'a> for IngressDemuxInput<'a> {
    fn arbitrary(input: &mut Unstructured<'a>) -> libfuzzer_sys::arbitrary::Result<Self> {
        let mode = input.arbitrary()?;
        let source_port = input.arbitrary()?;
        let candidate_port = input.arbitrary()?;
        let repeats = input.arbitrary()?;
        let packet_len = input.int_in_range(0..=MAX_PACKET_LEN)?;
        let packet = input.bytes(packet_len)?;
        Ok(Self {
            mode,
            source_port,
            candidate_port,
            repeats,
            packet,
        })
    }
}

fuzz_target!(|input: IngressDemuxInput<'_>| {
    route_packet_loop_ingress_demux(
        input.mode,
        input.source_port,
        input.candidate_port,
        input.packet,
        input.repeats,
    );
});
