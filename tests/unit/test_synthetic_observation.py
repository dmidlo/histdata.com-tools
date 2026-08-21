"""Tests for versioned historical feed-observation operators."""

from __future__ import annotations

import hashlib
import math
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.data_analytics import (
    DEFAULT_FEED_EPOCH_FEATURES,
    FeedEpochDefinitionV1,
    FeedEpochEvidenceV1,
    fit_feed_epochs,
)
from histdatacom.synthetic import (
    OBSERVATION_PARAMETER_NAMES,
    InformationMode,
    ObservationApplicationResultV1,
    ObservationCarryStateV1,
    ObservationContextV1,
    ObservationFitEvidenceV1,
    ObservationInputEventV1,
    ObservationOperatorFitConfigV1,
    ObservationOperatorV1,
    ObservationOutputEventV1,
    ReconstructionWindowV1,
    fit_observation_operator,
    read_observation_operator_artifact,
    write_observation_operator,
)
from histdatacom.synthetic.feed_epoch_transition import (
    FeedEpochTransitionPolicyV1,
    FeedEpochTransitionScenarioKind,
)
from histdatacom.synthetic.historical_conditioning import (
    historical_product_observation_conditioning,
    historical_product_retention_probability,
)
from histdatacom.synthetic.reconstruction_handlers import (
    _historical_product_observation_conditioning,
)
from tests.fixtures.reconstruction_transition import (
    reconstruction_transition_fixture,
)

SYMBOL = "EURUSD"
BASE_PARAMETERS = {
    "retention_probability": 1.0,
    "unchanged_retention_probability": 1.0,
    "timestamp_quantum_ns": 10.0,
    "price_precision_digits": 4.0,
    "quote_transition_threshold": 0.0,
    "batch_window_ns": 0.0,
    "duplicate_probability": 0.0,
    "rate_cap_per_second": 0.0,
    "burst_window_ns": 100.0,
    "quiet_gap_probability": 0.0,
    "outage_window_ns": 100.0,
    "reconnect_duplicate_probability": 0.0,
}


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _epoch_evidence(index: int) -> FeedEpochEvidenceV1:
    modern = index >= 6
    period = f"2020{index + 1:02d}"
    tick_rate = 1_000.0 if modern else 100.0
    interarrival = 100.0 if modern else 1_000.0
    precision = 6.0 if modern else 4.0
    spread = 0.0001 if modern else 0.0008
    features = {
        "log_tick_rate_per_hour": math.log1p(tick_rate),
        "log_median_interarrival_ms": math.log1p(interarrival),
        "log_p95_interarrival_ms": math.log1p(interarrival * 2.5),
        "minimum_observed_interval_ms": 10.0 if modern else 1_000.0,
        "price_precision_digits": precision,
        "spread_median": spread,
        "conditioned_spread_median": spread * 1.05,
        "absolute_spread_change_median": spread / 4.0,
        "stale_repeat_rate": 0.02 if modern else 0.45,
        "burst_rate": 0.8 if modern else 0.1,
        "duplicate_timestamp_rate": 0.0 if modern else 0.1,
        "suspicious_gap_rate": 0.0 if modern else 0.05,
        "source_quality_penalty": 0.0 if modern else 0.2,
    }
    assert set(features) == set(DEFAULT_FEED_EPOCH_FEATURES)
    start = index * 10_000
    return FeedEpochEvidenceV1(
        symbol=SYMBOL,
        period=period,
        start_timestamp_utc_ms=start,
        end_timestamp_utc_ms=start + 9_000,
        fingerprint_id=_sha256(f"fingerprint:{period}"),
        source_artifact_sha256=_sha256(f"source:{period}"),
        source_hash_basis="persisted_fingerprint_artifact_sha256",
        source_kind="cache",
        feature_values=features,
        feature_provenance={name: (f"fixture.{name}",) for name in features},
        conditioning={
            "calendar_status": "ok",
            "session_counts": {"london": 50, "new_york": 50},
        },
        quality={"sequence_status": "ok", "limitations": []},
        profile={
            "row_count": int(tick_rate),
            "tick_rate_per_hour": tick_rate,
            "median_interarrival_ms": interarrival,
            "p95_interarrival_ms": interarrival * 2.5,
            "max_interarrival_ms": interarrival * 5.0,
        },
    )


@pytest.fixture(scope="module")
def epoch_definition() -> FeedEpochDefinitionV1:
    definition = fit_feed_epochs(tuple(_epoch_evidence(i) for i in range(12)))
    assert definition.valid_for_observation_models
    assert len(definition.epochs) == 2
    return definition


def _fit_evidence(
    definition: FeedEpochDefinitionV1,
    *,
    parameters: dict[str, float] | None = None,
    support: int = 100,
    source_number: int = 1,
    state: str | None = "epoch",
    session: str | None = "london",
    event_tag: str | None = "none",
) -> ObservationFitEvidenceV1:
    selected = dict(BASE_PARAMETERS)
    selected.update(parameters or {})
    assert set(selected) == set(OBSERVATION_PARAMETER_NAMES)
    return ObservationFitEvidenceV1(
        context=ObservationContextV1(
            symbol=SYMBOL,
            epoch_id="epoch-001",
            state=state,
            session=session,
            event_tag=event_tag,
        ),
        period=f"2020{source_number:02d}",
        start_timestamp_ns=source_number * 1_000_000_000,
        end_timestamp_ns=source_number * 1_000_000_000 + 100_000_000,
        source_evidence_id=f"fixture-evidence-{source_number}",
        source_artifact_sha256=_sha256(f"fit-source:{source_number}"),
        source_hash_basis="controlled_fixture_sha256",
        evidence_kind="controlled_fixture",
        parameter_values=selected,
        parameter_lower_bounds=selected,
        parameter_upper_bounds=selected,
        parameter_support_counts={name: support for name in selected},
        parameter_basis={name: "controlled_fixture" for name in selected},
        parameter_provenance={
            name: (f"fixture.parameters.{name}",) for name in selected
        },
    )


def _operator(
    definition: FeedEpochDefinitionV1,
    *,
    parameters: dict[str, float] | None = None,
    config: ObservationOperatorFitConfigV1 | None = None,
) -> ObservationOperatorV1:
    return fit_observation_operator(
        (_fit_evidence(definition, parameters=parameters),),
        epoch_definition=definition,
        config=config
        or ObservationOperatorFitConfigV1(
            min_stratum_support=1,
            min_parameter_support=1,
            min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
        ),
    )


def _context(
    *,
    state: str = "epoch",
    session: str = "london",
    event_tag: str = "none",
) -> ObservationContextV1:
    return ObservationContextV1(
        symbol=SYMBOL,
        epoch_id="epoch-001",
        state=state,
        session=session,
        event_tag=event_tag,
    )


def _event(
    ordinal: int,
    *,
    timestamp: int,
    bid: float = 1.23456,
    ask: float = 1.23467,
    protected: bool = False,
    context: ObservationContextV1 | None = None,
) -> ObservationInputEventV1:
    return ObservationInputEventV1(
        source_event_id=f"market-event-{ordinal}",
        symbol=SYMBOL,
        event_time_ns=timestamp,
        event_sequence=ordinal,
        bid=bid,
        ask=ask,
        context=context or _context(),
        protected_anchor=protected,
    )


def _window(start: int = 0, end: int = 1_000) -> ReconstructionWindowV1:
    return ReconstructionWindowV1(
        run_id="run-observation-v1",
        ensemble_member_id="member-000",
        symbols=(SYMBOL,),
        core_start_ns=start,
        core_end_ns=end,
    )


def test_historical_product_cardinality_uses_supported_joint_epoch_evidence(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={"retention_probability": 0.25},
    )
    conditions = {
        "eurusd": SimpleNamespace(feed_epoch_id="epoch-001"),
    }

    first = _historical_product_observation_conditioning(
        operator,
        conditions=conditions,  # type: ignore[arg-type]
    )
    second = _historical_product_observation_conditioning(
        operator,
        conditions=conditions,  # type: ignore[arg-type]
    )

    assert first == second
    assert first["conditioning_id"].startswith(
        "historical-product-observation-conditioning:sha256:"
    )
    assert first["observation_operator_id"] == operator.operator_id
    assert first["resolution_basis"] == (
        "synchronized-epoch-aggregate-for-qualified-multivariate-cardinality-v1"
    )
    joint = first["joint_retention"]  # type: ignore[assignment]
    assert joint["stratum_level"] == "epoch"
    assert joint["retention_probability"] == 0.25
    symbol = first["symbols"]["EURUSD"]  # type: ignore[index]
    assert symbol["stratum_level"] == "symbol_epoch"
    assert symbol["retention_probability"] == 0.25
    assert symbol["support_count"] > 0
    assert symbol["context"]["state"] is None
    for endpoint in ("central", "lower", "upper"):
        assert (
            historical_product_retention_probability(
                operator,
                feed_epoch_label="epoch-001",
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                retention_endpoint=endpoint,
            )
            == 0.25
        )
    with pytest.raises(
        ValueError, match="unknown historical retention endpoint"
    ):
        historical_product_retention_probability(
            operator,
            feed_epoch_label="epoch-001",
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            retention_endpoint="invented",
        )


def test_historical_product_transition_uses_declared_ex_post_bridge() -> None:
    definition, operator = reconstruction_transition_fixture()
    used_at_ns = 1_242_345_600_000_000_000
    assignment = definition.assign(
        symbol="EURUSD", timestamp_utc_ms=used_at_ns // 1_000_000
    )
    assert assignment.assignment_kind == "transition"
    conditions = {
        symbol: SimpleNamespace(feed_epoch_id=assignment.label)
        for symbol in ("eurgbp", "eurusd", "gbpusd")
    }

    policy = FeedEpochTransitionPolicyV1()
    conditionings = {
        kind: historical_product_observation_conditioning(
            operator,
            feed_epoch_label=assignment.label,
            symbols=tuple(conditions),
            feed_epoch_definition=definition,
            used_at_ns=used_at_ns,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            transition_policy=policy,
            transition_scenario_kind=kind,
        )
        for kind in policy.scenario_order
    }
    conditioning = conditionings[FeedEpochTransitionScenarioKind.LINEAR_BRIDGE]

    assert conditioning["conditioning_mode"] == (
        "qualified-adjacent-epoch-transition-scenario-v1"
    )
    assert conditioning["feed_epoch_id"] == assignment.label
    assert conditioning["transition_left_epoch_id"] == "technology_epoch_02"
    assert conditioning["transition_right_epoch_id"] == "technology_epoch_03"
    assert conditioning["transition_left_weight"] + conditioning[
        "transition_right_weight"
    ] == pytest.approx(1.0)
    assert conditioning["feed_epoch_transition_policy_id"] == policy.policy_id
    assert (
        conditioning["transition_scenario_id"]
        == conditioning["transition_scenario"]["scenario_id"]
    )
    joint = conditioning["joint_retention"]  # type: ignore[assignment]
    assert joint["stratum_level"] == "transition_bridge"
    assert 0.24065855 < joint["retention_probability"] < 0.49569722
    left = conditionings[FeedEpochTransitionScenarioKind.LEFT_PERSISTENCE][
        "joint_retention"
    ]
    right = conditionings[FeedEpochTransitionScenarioKind.EARLY_RIGHT_ADOPTION][
        "joint_retention"
    ]
    assert left["retention_probability"] == pytest.approx(0.24065855)
    assert right["retention_probability"] == pytest.approx(0.49569722)
    assert (
        len(
            {
                value["transition_scenario_id"]
                for value in conditionings.values()
            }
        )
        == 3
    )

    with pytest.raises(ValueError, match="explicit transition policy"):
        _historical_product_observation_conditioning(
            operator,
            conditions=conditions,  # type: ignore[arg-type]
            feed_epoch_definition=definition,
            used_at_ns=used_at_ns,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        )

    with pytest.raises(ValueError, match="ex-ante forbidden"):
        historical_product_observation_conditioning(
            operator,
            feed_epoch_label=assignment.label,
            symbols=tuple(conditions),
            feed_epoch_definition=definition,
            used_at_ns=used_at_ns,
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            transition_policy=policy,
            transition_scenario_kind=(
                FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
            ),
        )

    prior_policy = FeedEpochTransitionPolicyV1(
        ex_ante_prior_artifact_id="point-in-time-transition-prior:test"
    )
    ex_ante = historical_product_observation_conditioning(
        operator,
        feed_epoch_label=assignment.label,
        symbols=tuple(conditions),
        feed_epoch_definition=definition,
        used_at_ns=used_at_ns,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        transition_policy=prior_policy,
        transition_scenario_kind=(
            FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
        ),
    )
    assert ex_ante["transition_future_evidence_use"] == (
        "point-in-time-valid-prior-bound"
    )
    assert ex_ante["transition_ex_ante_prior_artifact_id"] == (
        "point-in-time-transition-prior:test"
    )

    with pytest.raises(ValueError, match="outside uncertainty interval"):
        historical_product_observation_conditioning(
            operator,
            feed_epoch_label=assignment.label,
            symbols=tuple(conditions),
            feed_epoch_definition=definition,
            used_at_ns=int(conditioning["transition_start_ns"]) - 1,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            transition_policy=policy,
            transition_scenario_kind=(
                FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
            ),
        )

    mismatched_operator = SimpleNamespace(
        operator_id=operator.operator_id,
        feed_epoch_definition_id=("feed-epoch-definition:sha256:" + "0" * 64),
        resolve_stratum=operator.resolve_stratum,
    )
    with pytest.raises(ValueError, match="differs from observation operator"):
        historical_product_observation_conditioning(
            mismatched_operator,  # type: ignore[arg-type]
            feed_epoch_label=assignment.label,
            symbols=tuple(conditions),
            feed_epoch_definition=definition,
            used_at_ns=used_at_ns,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            transition_policy=policy,
            transition_scenario_kind=(
                FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
            ),
        )

    def resolve_without_right_epoch(context):
        if (
            context.epoch_id == "technology_epoch_03"
            and context.symbol == "GLOBAL"
        ):
            raise ValueError("historical product retention is unsupported")
        return operator.resolve_stratum(context)

    absent_right = SimpleNamespace(
        operator_id=operator.operator_id,
        feed_epoch_definition_id=operator.feed_epoch_definition_id,
        resolve_stratum=resolve_without_right_epoch,
    )
    with pytest.raises(ValueError, match="unsupported"):
        historical_product_observation_conditioning(
            absent_right,  # type: ignore[arg-type]
            feed_epoch_label=assignment.label,
            symbols=tuple(conditions),
            feed_epoch_definition=definition,
            used_at_ns=used_at_ns,
            information_mode=InformationMode.EX_POST_RECONSTRUCTION,
            transition_policy=policy,
            transition_scenario_kind=(
                FeedEpochTransitionScenarioKind.LINEAR_BRIDGE
            ),
        )


def test_canonical_epoch_projection_is_replayable_and_does_not_overclaim(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    """Canonical evidence should fit proxies while exposing unknown thinning."""
    source = tuple(_epoch_evidence(index) for index in range(12))
    projected = tuple(
        ObservationFitEvidenceV1.from_feed_epoch_evidence(
            item, epoch_definition
        )
        for item in source
    )

    first = fit_observation_operator(
        projected,
        epoch_definition=epoch_definition,
    )
    second = fit_observation_operator(
        tuple(reversed(projected)),
        epoch_definition=epoch_definition,
    )

    assert first.operator_id == second.operator_id
    assert first.to_dict() == second.to_dict()
    assert ObservationOperatorV1.from_json(first.to_json()) == first
    assert len(first.source_hashes) == len(source)
    assert "retention_probability" in (
        first.diagnostics.unsupported_parameter_names
    )
    global_parameter = next(
        stratum for stratum in first.strata if stratum.level == "global"
    ).parameter_map["retention_probability"]
    assert global_parameter.support_status == "unsupported"
    assert global_parameter.estimation_bases == (
        "identity_without_dense_denominator",
    )
    assert first.lineage["evidence_count"] == len(source)


def test_evidence_and_operator_readers_reject_identity_or_lineage_forgery(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    evidence = _fit_evidence(epoch_definition)
    evidence_payload = evidence.to_dict()
    evidence_payload["evidence_id"] = "observation-evidence:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="evidence_id"):
        ObservationFitEvidenceV1.from_dict(evidence_payload)

    operator = _operator(epoch_definition)
    payload = operator.to_dict()
    lineage = dict(payload["lineage"])
    lineage["evidence_count"] = 99
    payload["lineage"] = lineage
    payload["operator_id"] = ""
    with pytest.raises(ValueError, match="lineage evidence count"):
        ObservationOperatorV1.from_dict(payload)

    payload = operator.to_dict()
    lineage = dict(payload["lineage"])
    lineage["unversioned_extra"] = "not-allowed"
    payload["lineage"] = lineage
    payload["operator_id"] = ""
    with pytest.raises(ValueError, match="lineage fields"):
        ObservationOperatorV1.from_dict(payload)


def test_fit_requires_stability_passing_epoch_definition(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    unstable_stability = replace(
        epoch_definition.stability,
        status="fail",
        unstable_boundary_ids=(epoch_definition.boundaries[0].boundary_id,),
    )
    unstable_definition = replace(
        epoch_definition,
        stability=unstable_stability,
        definition_id="",
    )

    with pytest.raises(ValueError, match="not passed stability"):
        fit_observation_operator(
            (_fit_evidence(epoch_definition),),
            epoch_definition=unstable_definition,
        )


def test_evidence_periods_and_large_supported_source_sets_are_bounded(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    source = _fit_evidence(epoch_definition)
    with pytest.raises(ValueError, match="period must use"):
        replace(source, period="202013", evidence_id="")

    evidence = tuple(
        replace(
            source,
            source_evidence_id=f"large-fixture-{index}",
            source_artifact_sha256=_sha256(f"large-source-{index}"),
            evidence_id="",
        )
        for index in range(65)
    )
    operator = fit_observation_operator(
        evidence,
        epoch_definition=epoch_definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=1,
            min_parameter_support=1,
            min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
        ),
    )

    assert operator.diagnostics.evidence_count == 65
    assert len(operator.lineage["evidence_ids"]) == 65


def test_sparse_specific_strata_back_off_to_supported_parent(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    sparse = _fit_evidence(
        epoch_definition,
        support=1,
        source_number=1,
        session="asia",
        event_tag="release",
    )
    supported = _fit_evidence(
        epoch_definition,
        support=100,
        source_number=2,
        session="london",
        event_tag="none",
    )
    operator = fit_observation_operator(
        (sparse, supported),
        epoch_definition=epoch_definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=10,
            min_parameter_support=10,
            min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
        ),
    )

    stratum, attempted = operator.resolve_stratum(sparse.context)

    assert len(attempted) > 1
    assert stratum.level == "symbol_epoch_state"
    assert stratum.status == "ready"
    exact = next(
        item
        for item in operator.strata
        if item.key
        == sparse.context.key_for_level("symbol_epoch_state_session_event")
    )
    assert exact.status == "unsupported"
    assert stratum.key in exact.fallback_keys


def test_apply_preserves_anchor_and_quantizes_delivery_observation(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={"batch_window_ns": 20.0},
    )
    anchor = _event(1, timestamp=15, protected=True)
    candidate = _event(2, timestamp=37)

    result = operator.apply(
        (candidate, anchor),
        window=_window(),
        source_start=True,
    )

    assert result.input_count == 2
    assert result.output_count == 2
    protected = next(
        event for event in result.output_events if event.protected_anchor
    )
    transformed = next(
        event for event in result.output_events if not event.protected_anchor
    )
    assert (protected.observed_time_ns, protected.bid, protected.ask) == (
        anchor.event_time_ns,
        anchor.bid,
        anchor.ask,
    )
    assert protected.transformations == ()
    assert transformed.observed_time_ns == 20
    assert (transformed.bid, transformed.ask) == (1.2346, 1.2347)
    assert set(transformed.transformations) == {
        "batched",
        "timestamp_quantized",
        "price_quantized",
    }
    assert (
        ObservationOutputEventV1.from_dict(transformed.to_dict()) == transformed
    )
    assert ObservationCarryStateV1.from_dict(result.carry_state.to_dict()) == (
        result.carry_state
    )
    assert ObservationApplicationResultV1.from_dict(result.to_dict()) == result


def test_ex_ante_apply_refuses_anchors_but_benchmark_degrade_controls_them(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={"retention_probability": 0.0},
    )
    anchor = _event(1, timestamp=20, protected=True)

    with pytest.raises(ValueError, match="ex-ante.*anchors"):
        operator.apply(
            (anchor,),
            window=_window(),
            information_mode=InformationMode.EX_ANTE_SIMULATION,
            source_start=True,
        )

    unprotected = operator.degrade(
        (anchor,),
        window=_window(),
        source_start=True,
    )
    protected = operator.degrade(
        (anchor,),
        window=_window(),
        protected_event_ids=(anchor.source_event_id,),
        source_start=True,
    )
    assert unprotected.output_count == 0
    assert unprotected.reason_counts == {"thinning": 1}
    assert protected.output_count == 1
    assert protected.output_events[0].protected_anchor


@pytest.mark.parametrize(
    ("parameters", "events", "reason", "output_count", "transformation"),
    (
        (
            {"retention_probability": 0.0},
            (_event(1, timestamp=20),),
            "thinning",
            0,
            None,
        ),
        (
            {"duplicate_probability": 1.0},
            (_event(1, timestamp=20),),
            "retained",
            2,
            "duplicated",
        ),
        (
            {
                "quiet_gap_probability": 1.0,
                "outage_window_ns": 100.0,
            },
            (_event(1, timestamp=20),),
            "outage",
            0,
            None,
        ),
        (
            {"unchanged_retention_probability": 0.0},
            (
                _event(1, timestamp=20),
                _event(2, timestamp=30),
            ),
            "unchanged_quote_filter",
            1,
            None,
        ),
    ),
)
def test_controlled_fixtures_distinguish_observation_mechanisms(
    epoch_definition: FeedEpochDefinitionV1,
    parameters: dict[str, float],
    events: tuple[ObservationInputEventV1, ...],
    reason: str,
    output_count: int,
    transformation: str | None,
) -> None:
    """Thinning, duplication, outages, and unchanged filters stay distinct."""
    operator = _operator(epoch_definition, parameters=parameters)

    result = operator.degrade(
        events,
        window=_window(),
        source_start=True,
    )

    assert result.output_count == output_count
    assert result.reason_counts[reason] >= 1
    if transformation is not None:
        assert transformation in result.output_events[-1].transformations


def test_rate_cap_uses_the_declared_burst_window(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={"rate_cap_per_second": 10.0},
    )

    result = operator.degrade(
        (_event(1, timestamp=20), _event(2, timestamp=30)),
        window=_window(),
        source_start=True,
    )

    assert result.reason_counts == {"rate_cap": 1, "retained": 1}
    assert result.output_count == 1


def test_reconnect_behavior_survives_a_streaming_boundary(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={
            "quiet_gap_probability": 0.5,
            "reconnect_duplicate_probability": 1.0,
        },
    )
    events = tuple(
        _event(
            bucket,
            timestamp=bucket * 100 + 20,
            bid=1.2 + bucket / 100_000,
            ask=1.2001 + bucket / 100_000,
        )
        for bucket in range(100)
    )

    whole = operator.degrade(
        events,
        window=_window(0, 10_000),
        source_start=True,
    )
    reconnect = next(
        event
        for event in whole.output_events
        if "reconnect_duplicate" in event.transformations
    )
    split = reconnect.source_time_ns // 100 * 100
    first_events = tuple(
        event for event in events if event.event_time_ns < split
    )
    second_events = tuple(
        event for event in events if event.event_time_ns >= split
    )

    first = operator.degrade(
        first_events,
        window=_window(0, split),
        source_start=True,
    )
    assert first.carry_state.reconnect_pending
    second = operator.degrade(
        second_events,
        window=_window(split, 10_000),
        carry=first.carry_state,
    )

    assert tuple(event.to_dict() for event in whole.output_events) == tuple(
        event.to_dict()
        for event in (*first.output_events, *second.output_events)
    )
    assert whole.carry_state == second.carry_state


def test_window_carry_matches_single_window_and_is_required_when_declared(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    config = ObservationOperatorFitConfigV1(
        min_stratum_support=1,
        min_parameter_support=1,
        min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
        required_left_halo_ns=20,
    )
    operator = _operator(epoch_definition, config=config)
    first_event = _event(1, timestamp=20)
    second_event = _event(2, timestamp=120, bid=1.2347, ask=1.2348)

    whole = operator.apply(
        (first_event, second_event),
        window=_window(0, 200),
        source_start=True,
    )
    first = operator.apply(
        (first_event,),
        window=_window(0, 100),
        source_start=True,
    )
    with pytest.raises(ValueError, match="requires carry"):
        operator.apply((second_event,), window=_window(100, 200))
    second = operator.apply(
        (second_event,),
        window=_window(100, 200),
        carry=first.carry_state,
    )

    assert tuple(event.to_dict() for event in whole.output_events) == tuple(
        event.to_dict()
        for event in (*first.output_events, *second.output_events)
    )
    assert whole.carry_state == second.carry_state

    with pytest.raises(ValueError, match="watermark is stale or overlapping"):
        operator.apply(
            (first_event,),
            window=_window(0, 100),
            carry=first.carry_state,
        )


def test_operator_artifact_replays_by_hash_and_rejects_tampering(
    epoch_definition: FeedEpochDefinitionV1,
    tmp_path: Path,
) -> None:
    operator = _operator(epoch_definition)
    path = tmp_path / "operator.json"

    artifact = write_observation_operator(operator, path)

    assert read_observation_operator_artifact(artifact) == operator
    path.write_text(operator.to_json() + " \n", encoding="utf-8")
    with pytest.raises(ValueError, match="size differs|hash differs"):
        read_observation_operator_artifact(artifact)


def test_fit_and_application_resource_limits_fail_before_unbounded_work(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    evidence = _fit_evidence(epoch_definition)
    with pytest.raises(ValueError, match="evidence.*limit"):
        fit_observation_operator(
            (evidence, replace(evidence, evidence_id="")),
            epoch_definition=epoch_definition,
            config=ObservationOperatorFitConfigV1(max_evidence=1),
        )

    operator = _operator(
        epoch_definition,
        config=ObservationOperatorFitConfigV1(
            min_stratum_support=1,
            min_parameter_support=1,
            min_supported_parameters=len(OBSERVATION_PARAMETER_NAMES),
            max_input_events=1,
        ),
    )
    with pytest.raises(ValueError, match="input event limit"):
        operator.apply(
            (_event(1, timestamp=20), _event(2, timestamp=30)),
            window=_window(),
            source_start=True,
        )


def test_input_order_does_not_change_delivery_result(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(
        epoch_definition,
        parameters={"duplicate_probability": 1.0},
    )
    events = (
        _event(1, timestamp=20),
        _event(2, timestamp=30, bid=1.2347, ask=1.2348),
    )

    forward = operator.degrade(events, window=_window(), source_start=True)
    reverse = operator.degrade(
        tuple(reversed(events)), window=_window(), source_start=True
    )

    assert forward.to_dict() == reverse.to_dict()


def test_application_result_rejects_incomplete_reason_accounting(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    result = _operator(epoch_definition).apply(
        (_event(1, timestamp=20),),
        window=_window(),
        source_start=True,
    )
    payload = result.to_dict()
    payload["reason_counts"] = {"retained": 2}
    payload["result_id"] = ""

    with pytest.raises(ValueError, match="reason counts do not cover"):
        ObservationApplicationResultV1.from_dict(payload)


def test_window_boundaries_must_align_with_quantization_contract(
    epoch_definition: FeedEpochDefinitionV1,
) -> None:
    operator = _operator(epoch_definition)

    with pytest.raises(ValueError, match="not aligned"):
        operator.apply(
            (_event(1, timestamp=21),),
            window=_window(1, 101),
            source_start=True,
        )
