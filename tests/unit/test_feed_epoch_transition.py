"""Tests for qualified feed-epoch transition scenarios and decisions."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.synthetic.feed_epoch_transition import (
    FEED_EPOCH_TRANSITION_METRIC_NAMES,
    TRANSITION_SCENARIO_ORDER,
    FeedEpochTransitionDecision,
    FeedEpochTransitionDiagnosticStatus,
    FeedEpochTransitionDiagnosticV1,
    FeedEpochTransitionPolicyV1,
    FeedEpochTransitionSplit,
    build_feed_epoch_transition_scenario,
    evaluate_feed_epoch_transition,
    read_feed_epoch_transition_policy,
    read_feed_epoch_transition_report,
    transition_crossed_member_count,
    transition_scenario_kind_for_member,
    write_feed_epoch_transition_policy,
    write_feed_epoch_transition_report,
)

OBSERVATION_SCENARIO_IDS = (
    "observation-scenario:high",
    "observation-scenario:central",
    "observation-scenario:low",
)


def _scenarios(policy: FeedEpochTransitionPolicyV1):
    return tuple(
        build_feed_epoch_transition_scenario(
            policy,
            kind,
            observation_operator_id="observation-operator:test",
            feed_epoch_definition_id="feed-epoch-definition:test",
            feed_epoch_id="transition:test",
            transition_boundary_id="boundary:test",
            transition_start_ns=100,
            transition_end_ns=200,
            transition_left_epoch_id="epoch:left",
            transition_right_epoch_id="epoch:right",
            symbol_scope=("EURUSD", "GBPUSD", "EURGBP"),
            information_mode="ex_post_reconstruction",
            left_stratum_ids=("stratum:left",),
            right_stratum_ids=("stratum:right",),
            operator_evidence_ids=("evidence:left", "evidence:right"),
            linear_right_weight=0.25,
        )
        for kind in policy.scenario_order
    )


def _diagnostics(scenarios, *, endpoint_delta: float = 0.0):
    linear_id = scenarios[1].scenario_id
    return tuple(
        FeedEpochTransitionDiagnosticV1(
            split=split,
            scenario_id=scenario.scenario_id,
            observation_scenario_id=observation_scenario_id,
            path_seed=index,
            metric_values={
                metric: (
                    10.0
                    if scenario.scenario_id == linear_id
                    else 10.0 + endpoint_delta
                )
                for metric in FEED_EPOCH_TRANSITION_METRIC_NAMES
            },
        )
        for split in FeedEpochTransitionSplit
        for observation_scenario_id in OBSERVATION_SCENARIO_IDS
        for index, scenario in enumerate(scenarios, start=1)
    )


def test_transition_policy_scenarios_and_crossed_member_assignment() -> None:
    policy = FeedEpochTransitionPolicyV1()
    scenarios = _scenarios(policy)

    assert tuple(item.kind for item in scenarios) == TRANSITION_SCENARIO_ORDER
    assert tuple(
        (item.left_weight, item.right_weight) for item in scenarios
    ) == (
        (1.0, 0.0),
        (0.75, 0.25),
        (0.0, 1.0),
    )
    assert len({item.scenario_id for item in scenarios}) == 3
    assert (
        transition_crossed_member_count(policy, observation_scenario_count=3)
        == 9
    )
    assert tuple(
        transition_scenario_kind_for_member(
            policy,
            member_ordinal=ordinal,
            observation_scenario_count=3,
        )
        for ordinal in range(1, 10)
    ) == (
        policy.scenario_order[0],
        policy.scenario_order[0],
        policy.scenario_order[0],
        policy.scenario_order[1],
        policy.scenario_order[1],
        policy.scenario_order[1],
        policy.scenario_order[2],
        policy.scenario_order[2],
        policy.scenario_order[2],
    )
    with pytest.raises(ValueError, match="weights differ"):
        replace(scenarios[0], right_weight=0.5)


def test_transition_evaluation_retains_linear_or_requires_scenarios() -> None:
    policy = FeedEpochTransitionPolicyV1()
    scenarios = _scenarios(policy)

    robust = evaluate_feed_epoch_transition(
        policy, scenarios, _diagnostics(scenarios)
    )
    assert robust.decision is FeedEpochTransitionDecision.LINEAR_RETAINED
    assert robust.certification_state == (
        "qualified_linear_sensitivity_negligible"
    )
    assert not robust.material_metrics

    material = evaluate_feed_epoch_transition(
        policy, scenarios, _diagnostics(scenarios, endpoint_delta=2.0)
    )
    assert material.decision is (
        FeedEpochTransitionDecision.MULTIPLE_SCENARIOS_REQUIRED
    )
    assert set(material.material_metrics) == set(
        FEED_EPOCH_TRANSITION_METRIC_NAMES
    )
    assert material.certification_state == (
        "qualified_multiple_transition_scenarios_required"
    )


def test_transition_evaluation_propagates_incomplete_support() -> None:
    policy = FeedEpochTransitionPolicyV1()
    scenarios = _scenarios(policy)
    diagnostics = _diagnostics(scenarios)[:-1] + (
        FeedEpochTransitionDiagnosticV1(
            split=FeedEpochTransitionSplit.FINAL_HOLDOUT,
            scenario_id=scenarios[-1].scenario_id,
            observation_scenario_id=OBSERVATION_SCENARIO_IDS[-1],
            path_seed=3,
            metric_values={},
            status=FeedEpochTransitionDiagnosticStatus.REFUSED,
            limitation="absent adjacent stratum support",
        ),
    )

    report = evaluate_feed_epoch_transition(policy, scenarios, diagnostics)

    assert report.decision is FeedEpochTransitionDecision.LIMITED_OR_REFUSED
    assert report.certification_state == "transition_support_limited_or_refused"
    assert report.limitations == ("absent adjacent stratum support",)


def test_transition_policy_and_report_artifacts_are_content_addressed(
    tmp_path,
) -> None:
    policy = FeedEpochTransitionPolicyV1()
    scenarios = _scenarios(policy)
    report = evaluate_feed_epoch_transition(
        policy, scenarios, _diagnostics(scenarios)
    )

    policy_ref = write_feed_epoch_transition_policy(policy, tmp_path)
    report_ref = write_feed_epoch_transition_report(report, tmp_path)

    assert read_feed_epoch_transition_policy(policy_ref.path) == policy
    assert read_feed_epoch_transition_report(report_ref.path) == report
    renamed = tmp_path / "renamed.json"
    renamed.write_bytes(Path(policy_ref.path).read_bytes())
    with pytest.raises(ValueError, match="filename differs"):
        read_feed_epoch_transition_policy(renamed)
