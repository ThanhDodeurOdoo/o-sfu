use super::{non_empty, positive};

fn local_check(key: &'static str, value: String) -> anyhow::Result<String> {
    anyhow::ensure!(value == "local", "{key} must pass the local check");
    Ok(value)
}

env_block! {
    struct TestEnv {
        required: String = required("REQUIRED_ENV");
        flag: bool = default("FLAG_ENV", false);
        count: usize = default("COUNT_ENV", 1).check(positive);
        token: Option<String> = optional("TOKEN_ENV").check(non_empty);
    }
}

env_block! {
    struct LocalCheckEnv {
        value: String = required("LOCAL_CHECK_ENV").check(local_check);
    }
}

#[test]
fn env_block_loads_required_default_and_optional_values() {
    let env = TestEnv::load(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "FLAG_ENV" => Some("true".to_owned()),
        "COUNT_ENV" => Some("4".to_owned()),
        "TOKEN_ENV" => Some("  token  ".to_owned()),
        _ => None,
    });

    assert_eq!(
        env.ok(),
        Some(TestEnv {
            required: "value".to_owned(),
            flag: true,
            count: 4,
            token: Some("token".to_owned()),
        })
    );
}

#[test]
fn env_block_reports_missing_required_values() {
    let error = TestEnv::load(|_| None).err().map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("REQUIRED_ENV env variable is required")
    );
}

#[test]
fn env_block_reports_invalid_bools() {
    let error = TestEnv::load(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "FLAG_ENV" => Some("yes".to_owned()),
        _ => None,
    })
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("FLAG_ENV must be either `true` or `false`")
    );
}

#[test]
fn env_block_applies_positive_validation() {
    let error = TestEnv::load(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "COUNT_ENV" => Some("0".to_owned()),
        _ => None,
    })
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("COUNT_ENV must be greater than zero")
    );
}

#[test]
fn env_block_rejects_empty_non_empty_options() {
    let error = TestEnv::load(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "TOKEN_ENV" => Some("   ".to_owned()),
        _ => None,
    })
    .err()
    .map(|error| error.to_string());

    assert_eq!(error.as_deref(), Some("TOKEN_ENV must not be empty"));
}

#[test]
fn env_block_accepts_call_site_validators() {
    let env = LocalCheckEnv::load(|key| match key {
        "LOCAL_CHECK_ENV" => Some("local".to_owned()),
        _ => None,
    });

    assert_eq!(
        env.ok(),
        Some(LocalCheckEnv {
            value: "local".to_owned()
        })
    );
}
