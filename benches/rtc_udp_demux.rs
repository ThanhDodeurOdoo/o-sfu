use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use o_sfu::runtime::benchmark_support::{
    RtcUdpDemuxBenchmarkFixture, RtcUnknownSourceRecoveryBenchmarkFixture,
};

fn bench_rtc_udp_demux(c: &mut Criterion) {
    let mut group = c.benchmark_group("rtc_udp_remote_addr_demux");
    for session_count in [32_usize, 256, 1024, 4096] {
        let Some(fixture) = RtcUdpDemuxBenchmarkFixture::new(session_count) else {
            continue;
        };
        group.throughput(Throughput::Elements(fixture.lookup_count_u64()));
        group.bench_with_input(
            BenchmarkId::new("cached_hash_lookup", session_count),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| fixture.cached_lookup_cycle());
            },
        );
        group.bench_with_input(
            BenchmarkId::new("linear_reverse_scan", session_count),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| fixture.linear_scan_cycle());
            },
        );
    }
    group.finish();
}

fn bench_unknown_source_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("rtc_udp_unknown_source_recovery");
    for session_count in [32_usize, 256, 1024, 4096] {
        let Some(fixture) = RtcUnknownSourceRecoveryBenchmarkFixture::new(session_count) else {
            continue;
        };
        group.throughput(Throughput::Elements(fixture.lookup_count_u64()));
        group.bench_with_input(
            BenchmarkId::new("indexed_candidate_lookup", session_count),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| fixture.indexed_lookup_cycle());
            },
        );
        group.bench_with_input(
            BenchmarkId::new("linear_candidate_scan", session_count),
            &fixture,
            |bencher, fixture| {
                bencher.iter(|| fixture.linear_scan_cycle());
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_rtc_udp_demux, bench_unknown_source_recovery);
criterion_main!(benches);
