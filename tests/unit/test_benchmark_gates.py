from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.synthetic.benchmark_gates import (
    BenchmarkGateComparator,
    BenchmarkGateObservationV1,
    BenchmarkGateRequirementV1,
    BenchmarkGateScope,
    BenchmarkGateSeverity,
    BenchmarkGateStatus,
    BenchmarkPromotionDecisionV1,
    BenchmarkPromotionGatePolicyV1,
    evaluate_benchmark_promotion_gates,
    load_default_benchmark_promotion_gate_policy,
)


def _requirement(
    requirement_id: str,
    scope: BenchmarkGateScope,
    severity: BenchmarkGateSeverity,
    metric_name: str,
    comparator: BenchmarkGateComparator,
    threshold: int | float | bool,
) -> BenchmarkGateRequirementV1:
    return BenchmarkGateRequirementV1(
        requirement_id=requirement_id,
        scope=scope,
        severity=severity,
        metric_name=metric_name,
        comparator=comparator,
        threshold=threshold,
        description=f"Predeclared requirement for {metric_name}.",
    )


def _policy() -> BenchmarkPromotionGatePolicyV1:
    return BenchmarkPromotionGatePolicyV1(
        policy_name="fixture-policy",
        policy_version="v1",
        issue_number=463,
        frozen_before_candidate_results=True,
        requirements=(
            _requirement(
                "campaign-hard",
                BenchmarkGateScope.CAMPAIGN,
                BenchmarkGateSeverity.HARD,
                "campaign_failures",
                BenchmarkGateComparator.ZERO,
                0,
            ),
            _requirement(
                "campaign-advisory",
                BenchmarkGateScope.CAMPAIGN,
                BenchmarkGateSeverity.ADVISORY,
                "campaign_windows",
                BenchmarkGateComparator.GREATER_OR_EQUAL,
                2,
            ),
            _requirement(
                "candidate-hard",
                BenchmarkGateScope.CANDIDATE,
                BenchmarkGateSeverity.HARD,
                "anchor_violations",
                BenchmarkGateComparator.ZERO,
                0,
            ),
            _requirement(
                "candidate-advisory",
                BenchmarkGateScope.CANDIDATE,
                BenchmarkGateSeverity.ADVISORY,
                "uncertainty_reported",
                BenchmarkGateComparator.TRUE,
                True,
            ),
        ),
    )


def _observation(
    scope: BenchmarkGateScope,
    subject_id: str,
    metric_name: str,
    value: int | float | bool,
) -> BenchmarkGateObservationV1:
    return BenchmarkGateObservationV1(
        scope=scope,
        subject_id=subject_id,
        metric_name=metric_name,
        value=value,
        evidence_ids=("artifact:fixture",),
    )


def test_packaged_policy_is_predeclared_complete_and_content_addressed() -> (
    None
):
    policy = load_default_benchmark_promotion_gate_policy()

    assert policy.issue_number == 463
    assert policy.frozen_before_candidate_results is True
    assert policy.policy_id.startswith("benchmark-promotion-gates:sha256:")
    assert len(policy.requirements) == 23
    assert {(item.scope, item.severity) for item in policy.requirements} == {
        (BenchmarkGateScope.CAMPAIGN, BenchmarkGateSeverity.HARD),
        (BenchmarkGateScope.CAMPAIGN, BenchmarkGateSeverity.ADVISORY),
        (BenchmarkGateScope.CANDIDATE, BenchmarkGateSeverity.HARD),
        (BenchmarkGateScope.CANDIDATE, BenchmarkGateSeverity.ADVISORY),
    }
    assert BenchmarkPromotionGatePolicyV1.from_json(policy.to_json()) == policy


def test_policy_identity_is_order_stable_and_tamper_evident() -> None:
    policy = _policy()
    reordered = replace(
        policy,
        requirements=tuple(reversed(policy.requirements)),
        policy_id="",
    )

    assert reordered.policy_id == policy.policy_id
    with pytest.raises(ValueError, match="policy_id differs"):
        replace(policy, policy_version="v2")
    with pytest.raises(ValueError, match="frozen before results"):
        replace(policy, frozen_before_candidate_results=False, policy_id="")


def test_missing_hard_evidence_fails_closed_and_advisory_does_not_block() -> (
    None
):
    policy = _policy()
    decision = evaluate_benchmark_promotion_gates(
        policy,
        (),
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id="candidate:fixture",
    )

    assert decision.promotion_eligible is False
    assert decision.automatic_winner is False
    by_requirement = {item.requirement_id: item for item in decision.checks}
    hard = by_requirement["candidate-hard"]
    advisory = by_requirement["candidate-advisory"]
    assert hard.status is BenchmarkGateStatus.MISSING
    assert hard.blocking is True
    assert advisory.status is BenchmarkGateStatus.MISSING
    assert advisory.blocking is False


def test_advisory_failure_is_visible_without_becoming_a_hidden_hard_gate() -> (
    None
):
    policy = _policy()
    observations = (
        _observation(
            BenchmarkGateScope.CANDIDATE,
            "candidate:fixture",
            "anchor_violations",
            0,
        ),
        _observation(
            BenchmarkGateScope.CANDIDATE,
            "candidate:fixture",
            "uncertainty_reported",
            False,
        ),
    )
    decision = evaluate_benchmark_promotion_gates(
        policy,
        observations,
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id="candidate:fixture",
    )

    assert decision.promotion_eligible is True
    assert [item.status for item in decision.checks] == [
        BenchmarkGateStatus.FAILED,
        BenchmarkGateStatus.PASSED,
    ]
    assert not any(item.blocking for item in decision.checks)
    assert (
        BenchmarkPromotionDecisionV1.from_dict(decision.to_dict()) == decision
    )


def test_hard_failure_blocks_and_duplicate_metric_evidence_is_rejected() -> (
    None
):
    policy = _policy()
    failed = _observation(
        BenchmarkGateScope.CANDIDATE,
        "candidate:fixture",
        "anchor_violations",
        1,
    )
    advisory = _observation(
        BenchmarkGateScope.CANDIDATE,
        "candidate:fixture",
        "uncertainty_reported",
        True,
    )
    decision = evaluate_benchmark_promotion_gates(
        policy,
        (failed, advisory),
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id="candidate:fixture",
    )

    assert decision.promotion_eligible is False
    assert any(item.blocking for item in decision.checks)
    with pytest.raises(ValueError, match="duplicate.*metric"):
        evaluate_benchmark_promotion_gates(
            policy,
            (failed, failed),
            scope=BenchmarkGateScope.CANDIDATE,
            subject_id="candidate:fixture",
        )


def test_comparator_types_and_policy_shape_fail_closed() -> None:
    with pytest.raises(ValueError, match="boolean.*threshold"):
        _requirement(
            "bad-boolean",
            BenchmarkGateScope.CANDIDATE,
            BenchmarkGateSeverity.HARD,
            "bad_boolean",
            BenchmarkGateComparator.TRUE,
            1,
        )
    with pytest.raises(ValueError, match="hard and advisory"):
        BenchmarkPromotionGatePolicyV1(
            policy_name="incomplete",
            policy_version="v1",
            issue_number=463,
            frozen_before_candidate_results=True,
            requirements=(_policy().requirements[0],),
        )
