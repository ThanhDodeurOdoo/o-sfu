use super::{Env, EnvKey, non_empty, positive};

fn error<T>(result: anyhow::Result<T>) -> Option<String> {
    result.err().map(|error| error.to_string())
}

#[test]
fn env_loads_required_default_optional_check_and_trimmed_values() {
    let env = Env::new(|key| match key {
        "REQUIRED_ENV" => Some("value".to_owned()),
        "COUNT_ENV" => Some("4".to_owned()),
        "TOKEN_ENV" => Some("  token  ".to_owned()),
        _ => None,
    });

    assert_eq!(
        env.var("REQUIRED_ENV").required().ok(),
        Some("value".to_owned())
    );
    assert_eq!(env.var("FLAG_ENV").default(false).ok(), Some(false));
    assert_eq!(
        env.var("COUNT_ENV").check(positive).default(1usize).ok(),
        Some(4)
    );
    assert_eq!(
        env.var("TOKEN_ENV").check(non_empty).optional().ok(),
        Some(Some("token".to_owned()))
    );
    assert_eq!(env.var::<String>("MISSING_ENV").optional().ok(), Some(None));
}

#[test]
fn env_reports_parse_and_validation_errors() {
    let env = Env::new(|key| match key {
        "FLAG_ENV" => Some("yes".to_owned()),
        "COUNT_ENV" => Some("abc".to_owned()),
        "ZERO_ENV" => Some("0".to_owned()),
        "TOKEN_ENV" => Some("   ".to_owned()),
        _ => None,
    });

    assert_eq!(
        error(env.var::<String>("REQUIRED_ENV").required()).as_deref(),
        Some("REQUIRED_ENV env variable is required")
    );
    assert_eq!(
        error(env.var("FLAG_ENV").default(false)).as_deref(),
        Some("FLAG_ENV must be either `true` or `false`")
    );
    assert_eq!(
        error(env.var("COUNT_ENV").default(1usize)).as_deref(),
        Some("COUNT_ENV must be a valid usize")
    );
    assert_eq!(
        error(env.var("ZERO_ENV").check(positive).default(1usize)).as_deref(),
        Some("ZERO_ENV must be greater than zero")
    );
    assert_eq!(
        error(env.var("TOKEN_ENV").check(non_empty).optional()).as_deref(),
        Some("TOKEN_ENV must not be empty")
    );
}

#[test]
fn env_validates_default_values() {
    let env = Env::new(|_| None);

    assert_eq!(
        error(env.var("MISSING_COUNT").check(positive).default(0usize)).as_deref(),
        Some("MISSING_COUNT must be greater than zero")
    );
}

#[test]
fn env_runs_chained_checks_in_order() {
    fn less_than_ten(key: EnvKey, value: usize) -> anyhow::Result<usize> {
        anyhow::ensure!(value < 10, "{key} must be less than ten");
        Ok(value)
    }

    fn even(key: EnvKey, value: usize) -> anyhow::Result<usize> {
        anyhow::ensure!(value.is_multiple_of(2), "{key} must be even");
        Ok(value)
    }

    let env = Env::new(|key| match key {
        "COUNT_ENV" => Some("11".to_owned()),
        _ => None,
    });

    assert_eq!(
        error(
            env.var("COUNT_ENV")
                .check(less_than_ten)
                .check(even)
                .default(2usize),
        )
        .as_deref(),
        Some("COUNT_ENV must be less than ten")
    );
}

#[test]
fn env_alias() {
    let env = Env::new(|key| match key {
        "PRIMARY_ENV" => Some("primary".to_owned()),
        "SECOND_ALIAS_ENV" => Some("second alias".to_owned()),
        _ => None,
    });

    assert_eq!(
        env.var("MISSING_ENV")
            .alias("FIRST_ALIAS_ENV")
            .alias("SECOND_ALIAS_ENV")
            .required()
            .ok(),
        Some("second alias".to_owned())
    );
    assert_eq!(
        env.var("PRIMARY_ENV")
            .alias("SECOND_ALIAS_ENV")
            .required()
            .ok(),
        Some("primary".to_owned())
    );
}
