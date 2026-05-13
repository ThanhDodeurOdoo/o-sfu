#[test]
fn telemetry_macros_cover_supported_and_rejected_inputs() {
    let tests = trybuild::TestCases::new();
    tests.pass("../crates/telemetry/tests/ui/pass/*.rs");
    tests.compile_fail("../crates/telemetry/tests/ui/fail/*.rs");
}
