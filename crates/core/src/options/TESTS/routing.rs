use super::{Bitrate, LocalSpilloverPolicy, LocalSpilloverPolicyError, LocalSpilloverPolicyParts};

#[test]
fn local_spillover_policy_rejects_invalid_required_fields() {
    let valid_parts = LocalSpilloverPolicyParts::conservative();
    let cases = [
        (
            LocalSpilloverPolicyParts {
                min_receiver_count: 0,
                ..valid_parts
            },
            LocalSpilloverPolicyError::MinReceiverCountZero,
        ),
        (
            LocalSpilloverPolicyParts {
                max_active_consumers_per_router: 0,
                ..valid_parts
            },
            LocalSpilloverPolicyError::MaxActiveConsumersPerRouterZero,
        ),
        (
            LocalSpilloverPolicyParts {
                max_fanout_per_source: 0,
                ..valid_parts
            },
            LocalSpilloverPolicyError::MaxFanoutPerSourceZero,
        ),
        (
            LocalSpilloverPolicyParts {
                activation_window: 0,
                ..valid_parts
            },
            LocalSpilloverPolicyError::ActivationWindowZero,
        ),
    ];

    for (parts, error) in cases {
        assert_eq!(LocalSpilloverPolicy::try_new(parts).err(), Some(error));
    }
}

#[test]
fn local_spillover_policy_rejects_worker_pressure_above_score_range() {
    let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
        worker_pressure_threshold: 101,
        ..LocalSpilloverPolicyParts::conservative()
    });

    assert_eq!(
        policy.err(),
        Some(LocalSpilloverPolicyError::WorkerPressureThresholdTooHigh)
    );
}

#[test]
fn local_spillover_policy_accepts_disabled_transport_pressure_signals() {
    let policy = LocalSpilloverPolicy::try_new(LocalSpilloverPolicyParts {
        egress_bitrate_threshold: Bitrate::zero(),
        packet_loop_lag_threshold_ms: 0,
        command_backlog_threshold: 0,
        relay_mailbox_depth_threshold: 0,
        worker_pressure_threshold: 0,
        ..LocalSpilloverPolicyParts::conservative()
    });

    assert!(policy.is_ok());
    let Ok(policy) = policy else {
        return;
    };
    let parts = policy.parts();
    assert_eq!(parts.egress_bitrate_threshold, Bitrate::zero());
    assert_eq!(parts.packet_loop_lag_threshold_ms, 0);
    assert_eq!(parts.command_backlog_threshold, 0);
    assert_eq!(parts.relay_mailbox_depth_threshold, 0);
    assert_eq!(parts.worker_pressure_threshold, 0);
}
