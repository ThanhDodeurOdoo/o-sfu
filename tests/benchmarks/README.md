Just for experimenting, no need to commit benches,
I kept the demux benchmark file as an example/template

## Running Benchmarks

Most benchmarks require the `internal-benchmarks` feature since it may require code that
shouldn't be present in release.

Don't forget to add the `#[cfg(feature = "internal-benchmarks")]` for code that is only
used here.

Run all:

```bash
cargo bench --features internal-benchmarks
```

Run one specific:

```bash
cargo bench --bench rtc_udp_demux --features internal-benchmarks
```
