#[test]
fn telemetry_macros_cover_supported_and_rejected_inputs() {
    let tests = trybuild::TestCases::new();
    tests.pass("../telemetry/tests/ui/pass/*.rs");
    tests.compile_fail("../telemetry/tests/ui/fail/*.rs");
}
