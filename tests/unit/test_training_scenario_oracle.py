"""Independent arithmetic and actual native consumers, not metadata authority."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from fractions import Fraction
import hashlib
from itertools import permutations
import json

import pytest

from histdatacom.data_quality.training_scenario_math import (
    sample_scenario_member,
    scenario_scalar_moments,
)

pytest_plugins = ("tests.fixtures.training_scenario_v1",)


def exact_population_summary(values, masses, probabilities):
    # Defer importing the plugin module until pytest has registered it.
    from tests.fixtures.training_scenario_v1 import (
        exact_population_summary as independent_summary,
    )

    return independent_summary(values, masses, probabilities)


@pytest.mark.parametrize(
    ("values", "masses"),
    (
        ((1, 3, 9), (1, 2, 1)),
        ((-9, -3, -1), (1, 2, 1)),
        ((7, 7, 7), (1, 1, 1)),
        ((-100, 2, 4, 100), (0, 1, 1, 0)),
        ((0, 1, 2), (10, 1, 1)),
        ((1, 1, 3, 9), (1, 1, 4, 2)),
    ),
)
def test_exact_weighted_moments_and_inverse_cdf_match_independent_oracle(
    values, masses
):
    xs = tuple(Fraction(value) for value in values)
    ws = tuple(Fraction(weight) for weight in masses)
    ps = tuple(Fraction(p, 4) for p in range(5))
    expected = exact_population_summary(xs, ws, ps)
    actual = scenario_scalar_moments(xs, ws, ps)
    assert (
        actual.mean,
        actual.population_variance,
        actual.quantile_values,
    ) == (expected)
    assert actual.normalized_weights == tuple(weight / sum(ws) for weight in ws)


def test_literal_weighted_boundary_is_not_interpolated_or_sample_variance():
    values = (Fraction(1), Fraction(3), Fraction(9))
    weights = (Fraction(1), Fraction(2), Fraction(1))
    result = scenario_scalar_moments(
        values, weights, tuple(Fraction(p, 4) for p in range(5))
    )
    assert result.mean == 4
    assert result.population_variance == 9
    assert result.quantile_values == (1, 1, 3, 3, 9)


def test_binary64_projection_is_exact_including_subnormal_dispersion():
    # The input law is the persisted binary64, not a rounded decimal surrogate.
    values = tuple(
        Fraction.from_float(value) for value in (0.0, 5e-324, 1e-323)
    )
    weights = (Fraction(1),) * 3
    ps = (Fraction(0), Fraction(1, 2), Fraction(1))
    expected = exact_population_summary(values, weights, ps)
    result = scenario_scalar_moments(values, weights, ps)
    assert (
        result.mean,
        result.population_variance,
        result.quantile_values,
    ) == (expected)
    assert result.mean == Fraction(1, 2**1074)
    assert result.population_variance == Fraction(2, 3 * 2**2148)


def test_scalar_inventory_permutations_preserve_values_with_attached_masses():
    values = (Fraction(-3, 7), Fraction(1, 9), Fraction(11, 13))
    weights = (Fraction(1, 5), Fraction(2, 5), Fraction(2, 5))
    probabilities = (Fraction(0), Fraction(1, 5), Fraction(3, 5), Fraction(1))
    expected = exact_population_summary(values, weights, probabilities)
    for order in permutations(range(3)):
        result = scenario_scalar_moments(
            tuple(values[i] for i in order),
            tuple(weights[i] for i in order),
            probabilities,
        )
        assert (
            result.mean,
            result.population_variance,
            result.quantile_values,
        ) == expected


def test_duplicate_values_with_split_mass_do_not_inflate_information():
    probabilities = tuple(Fraction(p, 4) for p in range(5))
    original = scenario_scalar_moments(
        (Fraction(1), Fraction(9)),
        (Fraction(1, 2), Fraction(1, 2)),
        probabilities,
    )
    duplicate = scenario_scalar_moments(
        (Fraction(1), Fraction(1), Fraction(9)),
        (Fraction(1, 4), Fraction(1, 4), Fraction(1, 2)),
        probabilities,
    )
    assert duplicate.mean == original.mean
    assert duplicate.population_variance == original.population_variance
    assert duplicate.quantile_values == original.quantile_values


def test_semantic_sampling_ignores_inventory_order_and_thread_schedule():
    # These are mathematical identities, not independently admitted products.
    roster = ("member-a", "member-b", "member-c")

    def select(arguments):
        order, unit, epoch = arguments
        return sample_scenario_member(
            order,
            evidence_unit_id=unit,
            epoch=epoch,
            seed="predeclared-scenario-oracle",
            group_key="preserved-observation-and-transition",
        )

    arguments = tuple(
        (order, unit, epoch)
        for order in permutations(roster)
        for unit in ("unit-a", "unit-b")
        for epoch in (0, 1, 23)
    )
    sequential = tuple(map(select, arguments))
    with ThreadPoolExecutor(max_workers=3) as pool:
        scheduled = tuple(pool.map(select, reversed(arguments)))
    assert sequential == tuple(reversed(scheduled))
    for unit in ("unit-a", "unit-b"):
        for epoch in (0, 1, 23):
            selected = {
                result
                for arguments_row, result in zip(arguments, sequential)
                if arguments_row[1:] == (unit, epoch)
            }
            assert len(selected) == 1
            assert selected <= set(roster)


@pytest.fixture(scope="module")
def campaign_views(scenario_campaign):
    from histdatacom.data_quality.training_contracts import TrainingConsumerMode
    from histdatacom.data_quality.training_scenarios import (
        ScenarioViewKind,
        TrainingScenarioCampaignBindingV1,
        TrainingScenarioPlanV1,
        TrainingScenarioPolicyV1,
        TrainingScenarioRationalV1,
        TrainingScenarioRequestV1,
        materialize_training_scenario_view,
    )
    from tests.fixtures.campaign_verification import END_NS, START_NS, SYMBOLS

    index = scenario_campaign["index"]
    plan = TrainingScenarioPlanV1(
        scenario_campaign["source"],
        scenario_campaign["ownership"],
        TrainingScenarioPolicyV1(
            quantiles=tuple(
                TrainingScenarioRationalV1.from_fraction(Fraction(p, 2))
                for p in (0, 1, 2)
            )
        ),
        campaign_binding=TrainingScenarioCampaignBindingV1(
            index.path,
            index.metadata["product_index_id"],
            index.sha256,
        ),
    )
    request = TrainingScenarioRequestV1(
        ScenarioViewKind.MEMBER_PANEL,
        TrainingConsumerMode.DESCRIPTIVE,
        START_NS,
        END_NS,
        SYMBOLS,
    )
    panel = materialize_training_scenario_view(plan, request)
    # A frozen identity-based baseline choice, never a value-ranked selection.
    first_per_unit = {}
    for member in panel.members:
        first_per_unit.setdefault(member.evidence_unit_id, member.member_key)
    central_plan = replace(
        plan,
        policy=replace(
            plan.policy,
            central_member_keys=tuple(sorted(first_per_unit.values())),
        ),
    )
    views = {ScenarioViewKind.MEMBER_PANEL: panel}
    for kind in ScenarioViewKind:
        if kind in views:
            continue
        selected_plan = (
            central_plan
            if kind is ScenarioViewKind.CENTRAL_COUNTERFACTUAL
            else plan
        )
        views[kind] = materialize_training_scenario_view(
            selected_plan, replace(request, view=kind)
        )
    return views


def assert_native_scalar_oracle(view):
    """Read actual retained event streams, independently of scenario reducers."""
    from histdatacom.synthetic.persistence import (
        load_reconstruction_manifest,
        read_reconstruction_streams,
    )

    streams = {}
    for path in view.plan.source.product_manifest_paths:
        manifest = load_reconstruction_manifest(path)
        streams[manifest.manifest_id] = tuple(
            event
            for stream in read_reconstruction_streams(path)
            for event in stream.events
        )
    members = {member.member_key: member for member in view.members}
    actual_values = {}
    for scalar in view.scalars:
        member = members[scalar.member_key]
        c = scalar.coordinate
        events = tuple(
            event
            for identity in member.product_manifest_ids
            for event in streams[identity]
            if event.ensemble_member_id == member.ensemble_member_id
            and event.symbol.upper() == c.symbol
            and c.start_ns <= event.event_time_ns < c.end_ns
        )
        if member.status.value in ("refused", "unavailable"):
            assert scalar.value is None
            assert scalar.status is member.status
            assert scalar.reason_codes
            continue
        if not (member.start_ns <= c.start_ns < c.end_ns <= member.end_ns):
            assert scalar.value is None
            assert "incomplete_member_support" in scalar.reason_codes
            continue
        assert events, "positive fixture must have actual member events"
        feature = c.feature
        if feature == "event_count":
            expected = Fraction(len(events))
        elif feature in ("observed_count", "synthetic_count"):
            expected = Fraction(
                sum(
                    (event.origin.value == "observed")
                    == (feature == "observed_count")
                    for event in events
                )
            )
        else:
            values = tuple(
                (
                    Fraction.from_float(event.ask)
                    - Fraction.from_float(event.bid)
                    if feature == "spread_mean"
                    else Fraction.from_float(
                        event.bid if feature.startswith("bid_") else event.ask
                    )
                )
                for event in events
            )
            expected = (
                min(values)
                if feature.endswith("_min")
                else (
                    max(values)
                    if feature.endswith("_max")
                    else sum(values, Fraction()) / len(values)
                )
            )
        assert scalar.value is not None
        assert scalar.value.value == expected
        actual_values[(c.artifact_id, scalar.member_key)] = expected
    for summary in view.summaries:
        if summary.mean is None:
            assert summary.reason_codes and not summary.weights
            continue
        values = tuple(
            actual_values[(summary.coordinate.artifact_id, key)]
            for key in summary.member_keys
        )
        masses = (Fraction(1),) * len(values)
        probabilities = tuple(p.value for p in view.plan.policy.quantiles)
        expected = exact_population_summary(values, masses, probabilities)
        assert summary.population_variance is not None
        assert (
            summary.mean.value,
            summary.population_variance.value,
            tuple(q.value.value for q in summary.quantiles),
        ) == expected
        assert sum(w.weight.value for w in summary.weights) == 1


def test_all_six_native_campaign_views_reconcile_and_keep_axes(campaign_views):
    from histdatacom.data_quality.training_scenarios import (
        ScenarioAxis,
        ScenarioAxisState,
        ScenarioViewKind,
    )

    assert set(campaign_views) == set(ScenarioViewKind)
    panel = campaign_views[ScenarioViewKind.MEMBER_PANEL]
    assert panel.members and panel.rows and panel.scalars
    for kind, view in campaign_views.items():
        assert view.members == panel.members
        assert view.denominator == panel.denominator
        assert view.verification_roots
        for member in view.members:
            axes = {axis.axis: axis for axis in member.axes}
            # Empirical motif is a valid baseline, not marked uncertainty proof.
            assert axes[ScenarioAxis.OBSERVATION_RETENTION].state is (
                ScenarioAxisState.NOT_APPLICABLE
            )
            assert axes[ScenarioAxis.TRANSITION].state is (
                ScenarioAxisState.NOT_APPLICABLE
            )
        if kind is ScenarioViewKind.OBSERVED_ONLY:
            assert all(row.member_key is None for row in view.rows)
            assert len({row.row.source_row_key for row in view.rows}) == len(
                view.rows
            )
            assert not view.scalars and not view.summaries
        else:
            assert_native_scalar_oracle(view)
        if kind in (
            ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
            ScenarioViewKind.SAMPLE_ONE_MEMBER,
        ):
            by_unit = {member.evidence_unit_id for member in view.members}
            assert len(view.selected_member_keys) == len(by_unit)
            assert {row.member_key for row in view.rows} <= set(
                view.selected_member_keys
            )


def test_each_native_view_replays_from_exact_inputs(campaign_views):
    from histdatacom.data_quality.training_scenarios import (
        replay_training_scenario_view,
    )

    for view in campaign_views.values():
        replayed = replay_training_scenario_view(
            view, expected_view_id=view.artifact_id
        )
        assert replayed.to_json() == view.to_json()


@pytest.fixture(scope="module")
def research_views(scenario_research):
    from histdatacom.data_quality.training_contracts import TrainingConsumerMode
    from histdatacom.data_quality.training_scenarios import (
        ScenarioAxis,
        ScenarioViewKind,
        TrainingScenarioPlanV1,
        TrainingScenarioPolicyV1,
        TrainingScenarioRationalV1,
        TrainingScenarioRequestV1,
        materialize_training_scenario_view,
    )

    fixture = scenario_research
    plan = TrainingScenarioPlanV1(
        fixture["source"],
        fixture["ownership"],
        TrainingScenarioPolicyV1(
            quantiles=tuple(
                TrainingScenarioRationalV1.from_fraction(Fraction(p, 2))
                for p in (0, 1, 2)
            )
        ),
        research_binding=fixture["binding"],
    )
    request = TrainingScenarioRequestV1(
        ScenarioViewKind.MEMBER_PANEL,
        TrainingConsumerMode.DESCRIPTIVE,
        fixture["start_ns"],
        fixture["end_ns"],
        fixture["symbols"],
    )
    panel = materialize_training_scenario_view(plan, request)
    central = {}
    for member in panel.members:
        if any(
            axis.axis is ScenarioAxis.OBSERVATION_RETENTION
            and axis.value_id == "central_fitted_retention"
            for axis in member.axes
        ) and any(
            axis.axis is ScenarioAxis.TRANSITION
            and axis.value_id == "early_right_adoption"
            for axis in member.axes
        ):
            central.setdefault(member.evidence_unit_id, member.member_key)
    assert central, "real research fixture must expose actual fitted centrality"
    central_plan = replace(
        plan,
        policy=replace(
            plan.policy, central_member_keys=tuple(sorted(central.values()))
        ),
    )
    result = {ScenarioViewKind.MEMBER_PANEL: panel}
    for kind in ScenarioViewKind:
        if kind not in result:
            result[kind] = materialize_training_scenario_view(
                (
                    central_plan
                    if kind is ScenarioViewKind.CENTRAL_COUNTERFACTUAL
                    else plan
                ),
                replace(request, view=kind),
            )
    return result


def test_actual_crossed_native_uncertainty_reconciles_without_axis_flattening(
    research_views, scenario_research
):
    from histdatacom.data_quality.training_scenarios import (
        ScenarioAxis,
        ScenarioAxisState,
        ScenarioMemberStatus,
        ScenarioViewKind,
    )

    panel = research_views[ScenarioViewKind.MEMBER_PANEL]
    assert len(panel.members) == 18
    assert len(scenario_research["cells"]) == 18
    cells = {
        cell.window.ensemble_member_id: cell
        for cell in scenario_research["cells"]
    }
    combinations = {}
    for member in panel.members:
        cell = cells[member.ensemble_member_id]
        axes = {axis.axis: axis for axis in member.axes}
        observation = axes[ScenarioAxis.OBSERVATION_RETENTION]
        transition = axes[ScenarioAxis.TRANSITION]
        assert observation.state is transition.state is ScenarioAxisState.KNOWN
        assert observation.value_id == cell.observation_kind
        assert transition.value_id == cell.transition_kind
        assert cell.observation_id in observation.evidence_ids
        assert cell.transition_id in transition.evidence_ids
        combinations.setdefault(
            (observation.value_id, transition.value_id), []
        ).append(member)
    assert len(combinations) == 9
    assert {key[0] for key in combinations} == {
        "low_retention_high_infill",
        "central_fitted_retention",
        "high_retention_low_infill",
    }
    assert {key[1] for key in combinations} == {
        "left_persistence",
        "linear_bridge",
        "early_right_adoption",
    }
    assert all(len(members) == 2 for members in combinations.values())
    # The native atomic envelope genuinely refuses left/linear under this
    # fixture's fixed limits. Preserve that outcome, including the individually
    # less demanding cells, instead of silently certifying the survivors.
    assert panel.denominator.eligible == 6
    assert panel.denominator.refused == 12
    for member in panel.members:
        cell = cells[member.ensemble_member_id]
        assert member.status.value == cell.status
        assert member.reason_codes == cell.reasons
        assert (member.status is ScenarioMemberStatus.ELIGIBLE) == (
            cell.transition_kind == "early_right_adoption"
        )
    for kind, view in research_views.items():
        assert view.members == panel.members
        assert view.denominator == panel.denominator
        assert_native_scalar_oracle(view)
        if kind in (
            ScenarioViewKind.MARGINALIZED_FEATURES,
            ScenarioViewKind.UNCERTAINTY_FEATURES,
        ):
            assert view.collapsed_axes == (ScenarioAxis.PATH_REALIZATION,)
            assert view.summaries
            assert all(
                len(summary.member_keys) == 2 for summary in view.summaries
            )
            for summary in view.summaries:
                transition = next(
                    axis.value_id
                    for axis in summary.preserved_axes
                    if axis.axis is ScenarioAxis.TRANSITION
                )
                if transition == "early_right_adoption":
                    assert summary.mean is not None
                    assert summary.denominator.eligible == 2
                else:
                    assert summary.mean is None
                    assert summary.denominator.refused == 2
                    assert not summary.weights and summary.reason_codes
                assert {axis.axis for axis in summary.preserved_axes} == set(
                    ScenarioAxis
                ) - {ScenarioAxis.PATH_REALIZATION}
        else:
            assert not view.collapsed_axes
        if kind in (
            ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
            ScenarioViewKind.SAMPLE_ONE_MEMBER,
        ):
            assert len(view.selected_member_keys) == 1
            chosen = next(
                member
                for member in view.members
                if member.member_key in view.selected_member_keys
            )
            assert {row.member_key for row in view.rows} == (
                {chosen.member_key}
                if chosen.status is ScenarioMemberStatus.ELIGIBLE
                else set()
            )


def test_research_sampling_is_whole_unit_and_projection_stable(research_views):
    from histdatacom.data_quality.training_scenarios import (
        ScenarioViewKind,
        materialize_training_scenario_view,
    )

    sampled = research_views[ScenarioViewKind.SAMPLE_ONE_MEMBER]
    # Independent framing/oracle over the actual native roster. A sampler that
    # simply picks the first key cannot pass this law across semantic epochs.
    for epoch in (0, 1, 17):
        actual = materialize_training_scenario_view(
            sampled.plan, replace(sampled.request, epoch=epoch)
        )
        unit = sampled.members[0].evidence_unit_id
        scores = {}
        for member in sampled.members:
            payload = [
                sampled.plan.policy.sampling_seed,
                unit,
                "joint-native-context-roster.v1",
                epoch,
                member.member_key,
            ]
            wire = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
            scores[member.member_key] = hashlib.sha256(
                b"histdatacom.training-scenario-sample.v1\n"
                + wire.encode("ascii")
            ).digest()
        expected = min(scores, key=lambda key: (scores[key], key))
        assert actual.selected_member_keys == (expected,)
    narrower = replace(
        sampled.request,
        start_ns=sampled.request.start_ns + 1,
        symbols=(sampled.request.symbols[0],),
    )
    projected = materialize_training_scenario_view(sampled.plan, narrower)
    assert projected.selected_member_keys == sampled.selected_member_keys
    assert projected.members == sampled.members
    assert projected.denominator == sampled.denominator
    assert (
        tuple(
            row
            for row in sampled.rows
            if row.row.event_time_ns >= narrower.start_ns
            and row.row.symbol in narrower.symbols
        )
        == projected.rows
    )


def test_each_research_view_is_reproducible(research_views):
    from histdatacom.data_quality.training_scenarios import (
        replay_training_scenario_view,
    )

    for view in research_views.values():
        assert (
            replay_training_scenario_view(
                view, expected_view_id=view.artifact_id
            ).to_json()
            == view.to_json()
        )
