"""Actual native view admission and bounded kernel refusal controls."""

from __future__ import annotations

from dataclasses import replace
from fractions import Fraction
from pathlib import Path

import pytest

from histdatacom.data_quality import training_scenario_native as native
from histdatacom.data_quality import training_scenario_views as views
from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode,
    TrainingRequestV1,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_scenario_contracts import (
    SCENARIO_AXES,
    ScenarioAxis,
    ScenarioAxisState,
    ScenarioMemberStatus,
    ScenarioViewKind,
    TrainingScenarioAxisV1,
    TrainingScenarioCampaignBindingV1,
    TrainingScenarioMemberV1,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRationalV1,
    TrainingScenarioRequestV1,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from tests.fixtures.training_substrate_v1 import (
    BASE,
    SYMBOLS,
    TIMES,
    observed_source,
    published_product,
    with_products,
)

pytest_plugins = ("tests.fixtures.training_scenario_v1",)


@pytest.fixture(scope="module")
def observed_plan(tmp_path_factory):
    source, _ = observed_source(tmp_path_factory.mktemp("scenario-view-source"))
    return TrainingScenarioPlanV1(
        source, build_training_ownership(source), TrainingScenarioPolicyV1()
    )


def request(kind=ScenarioViewKind.OBSERVED_ONLY, **kwargs):
    return TrainingScenarioRequestV1(
        kind,
        TrainingConsumerMode.DESCRIPTIVE,
        kwargs.pop("start", BASE),
        kwargs.pop("end", max(TIMES) + 1),
        kwargs.pop("symbols", SYMBOLS),
        **kwargs,
    )


def test_observed_projection_exactly_matches_actual_legacy_rows(observed_plan):
    selected = request()
    actual = views.materialize_training_scenario_view(observed_plan, selected)
    legacy = materialize_training_rows(
        observed_plan.source,
        observed_plan.ownership,
        TrainingRequestV1(
            selected.consumer_mode,
            selected.start_ns,
            selected.end_ns,
            selected.symbols,
        ),
    )
    assert {item.row.to_json() for item in actual.rows} == {
        row.to_json() for row in legacy.rows
    }
    assert actual.members == actual.selected_member_keys == ()
    assert actual.denominator.total == 0
    assert all(item.member_key is None for item in actual.rows)
    assert (
        views.replay_training_scenario_view(
            actual, expected_view_id=actual.artifact_id
        )
        == actual
    )


@pytest.mark.parametrize("kind", tuple(ScenarioViewKind)[1:])
def test_observed_source_does_not_fabricate_counterfactual_members(
    observed_plan, kind
):
    with pytest.raises(ValueError, match="member inventory"):
        views.materialize_training_scenario_view(observed_plan, request(kind))


def test_projection_does_not_invent_an_unobserved_symbol(observed_plan):
    with pytest.raises(ValueError, match="source support"):
        views.materialize_training_scenario_view(
            observed_plan, request(symbols=("USDJPY",))
        )


def test_counterfactual_sources_cannot_hide_unbound_products(tmp_path):
    source, version = observed_source(tmp_path / "observed")
    product, _ = published_product(tmp_path / "product", version)
    source = with_products(source, product)
    plan = TrainingScenarioPlanV1(
        source, build_training_ownership(source), TrainingScenarioPolicyV1()
    )
    # Even observed_only must not bypass the complete product inventory.
    with pytest.raises(ValueError, match="native evidence binding"):
        views.materialize_training_scenario_view(plan, request())


@pytest.mark.parametrize(
    "identity", ("", "wrong", "training-row:sha256:" + "a" * 64)
)
def test_invalid_expected_identity_refuses_before_native_execution(
    identity, monkeypatch
):
    def poison(*args, **kwargs):
        pytest.fail("invalid identity reached native execution")

    monkeypatch.setattr(views, "materialize_training_scenario_view", poison)
    with pytest.raises(ValueError, match="independently selected"):
        views.replay_training_scenario_view(None, expected_view_id=identity)


def test_real_source_read_is_rechecked_after_row_derivation(
    observed_plan, monkeypatch
):
    original = native.verify_training_ownership
    calls = []

    def observe(*args):
        result = original(*args)
        calls.append(result)
        return result

    monkeypatch.setattr(native, "verify_training_ownership", observe)
    views.materialize_training_scenario_view(observed_plan, request())
    assert len(calls) == 2
    assert calls[0] == calls[1]


def test_sampling_one_whole_member_per_unit_across_context_and_config_strata(
    observed_plan,
):
    # Structural selection-kernel vectors, not fabricated native authority.
    members = []
    for unit in observed_plan.ownership.units:
        for context in ("low", "central", "high"):
            for config in ("config-a", "config-b"):
                for path in ("path-a", "path-b"):
                    coordinates = {
                        ScenarioAxis.OBSERVATION_RETENTION: context,
                        ScenarioAxis.PATH_REALIZATION: path,
                        ScenarioAxis.ENGINE: "same-engine",
                    }
                    axes = tuple(
                        TrainingScenarioAxisV1(
                            axis,
                            (
                                ScenarioAxisState.KNOWN
                                if axis in coordinates
                                else ScenarioAxisState.NOT_APPLICABLE
                            ),
                            coordinates.get(axis),
                            ("structural-kernel-vector",),
                        )
                        for axis in SCENARIO_AXES
                    )
                    members.append(
                        TrainingScenarioMemberV1(
                            unit.artifact_id,
                            path,
                            unit.start_ns,
                            unit.end_ns,
                            (config,),
                            ("run",),
                            (),
                            axes,
                            ScenarioMemberStatus.REFUSED,
                            ("controlled_native_refusal",),
                            ("structural-kernel-vector",),
                        )
                    )
    roster = tuple(sorted(members, key=lambda item: item.member_key))
    selected = views._selection(
        observed_plan, request(ScenarioViewKind.SAMPLE_ONE_MEMBER), roster
    )
    assert len(selected) == len(observed_plan.ownership.units)
    by_key = {member.member_key: member for member in roster}
    assert {by_key[key].evidence_unit_id for key in selected} == {
        unit.artifact_id for unit in observed_plan.ownership.units
    }
    assert all(
        by_key[key].status is ScenarioMemberStatus.REFUSED for key in selected
    )
    assert (
        views._selection(
            observed_plan,
            request(ScenarioViewKind.SAMPLE_ONE_MEMBER),
            tuple(reversed(roster)),
        )
        == selected
    )
    assert (
        views._selection(
            observed_plan,
            request(ScenarioViewKind.SAMPLE_ONE_MEMBER, start=BASE + 1),
            roster,
        )
        == selected
    )


@pytest.fixture(scope="module")
def campaign_plan(scenario_campaign):
    index = scenario_campaign["index"]
    return TrainingScenarioPlanV1(
        scenario_campaign["source"],
        scenario_campaign["ownership"],
        TrainingScenarioPolicyV1(
            features=("event_count", "spread_mean"),
            quantiles=(
                TrainingScenarioRationalV1.from_fraction(Fraction(1, 2)),
            ),
        ),
        TrainingScenarioCampaignBindingV1(
            index.path, index.metadata["product_index_id"], index.sha256
        ),
    )


def campaign_request(plan, kind, **kwargs):
    from histdatacom.campaign_verification import read_campaign_control_json

    index = read_campaign_control_json(plan.campaign_binding.index_path)
    return TrainingScenarioRequestV1(
        kind,
        TrainingConsumerMode.DESCRIPTIVE,
        index["requested_start_ns"],
        index["requested_end_ns"],
        plan.ownership.units[0].graph_symbols,
        **kwargs,
    )


def test_actual_campaign_panel_has_no_fabricated_observation_axes(
    campaign_plan,
):
    result = views.materialize_training_scenario_view(
        campaign_plan,
        campaign_request(campaign_plan, ScenarioViewKind.MEMBER_PANEL),
    )
    assert result.rows and len(result.members) >= 2
    assert result.denominator.eligible == len(result.members)
    assert result.selected_member_keys == tuple(
        member.member_key for member in result.members
    )
    assert all(
        member.status is ScenarioMemberStatus.ELIGIBLE
        for member in result.members
    )
    assert all(
        any(
            identity.startswith("campaign-product-verification:sha256:")
            for identity in member.native_evidence_ids
        )
        for member in result.members
    )
    assert all(
        axis.state is ScenarioAxisState.NOT_APPLICABLE
        for member in result.members
        for axis in member.axes
        if axis.axis
        in (ScenarioAxis.OBSERVATION_RETENTION, ScenarioAxis.TRANSITION)
    )
    assert all(
        item.member_key in result.selected_member_keys for item in result.rows
    )


@pytest.mark.parametrize("kind", tuple(ScenarioViewKind))
def test_six_views_use_genuine_complete_native_campaign(campaign_plan, kind):
    plan = campaign_plan
    if kind is ScenarioViewKind.CENTRAL_COUNTERFACTUAL:
        panel = views.materialize_training_scenario_view(
            plan, campaign_request(plan, ScenarioViewKind.MEMBER_PANEL)
        )
        units = sorted({member.evidence_unit_id for member in panel.members})
        keys = tuple(
            sorted(
                next(
                    member.member_key
                    for member in panel.members
                    if member.evidence_unit_id == unit
                )
                for unit in units
            )
        )
        plan = replace(
            plan, policy=replace(plan.policy, central_member_keys=keys)
        )
    result = views.materialize_training_scenario_view(
        plan, campaign_request(plan, kind)
    )
    assert result.verification_roots
    assert result.denominator.total == len(result.members)
    if kind in (
        ScenarioViewKind.MARGINALIZED_FEATURES,
        ScenarioViewKind.UNCERTAINTY_FEATURES,
    ):
        assert result.scalars and result.summaries
        assert any(summary.mean is not None for summary in result.summaries)
        assert result.collapsed_axes == (ScenarioAxis.PATH_REALIZATION,)
    else:
        assert result.rows
        assert result.collapsed_axes == ()


def test_campaign_binding_is_checked_before_native_traversal(
    campaign_plan, monkeypatch
):
    from histdatacom import campaign_verification

    def poison(*args, **kwargs):
        pytest.fail("wrong selected index hash reached native traversal")

    monkeypatch.setattr(
        campaign_verification, "open_campaign_verification", poison
    )
    plan = replace(
        campaign_plan,
        campaign_binding=replace(
            campaign_plan.campaign_binding, index_sha256="0" * 64
        ),
    )
    with pytest.raises(ValueError, match="campaign binding"):
        views.materialize_training_scenario_view(
            plan, campaign_request(plan, ScenarioViewKind.MEMBER_PANEL)
        )


def test_same_identity_reformatted_index_cannot_change_selected_byte_root(
    campaign_plan, monkeypatch
):
    from histdatacom import campaign_verification

    target = Path(campaign_plan.campaign_binding.index_path)
    before = target.read_bytes()
    original = campaign_verification.open_campaign_verification
    visited = []

    def change_after_initial_read(path):
        visited.append(path)
        # Same decoded native index identity; different actual source bytes.
        target.write_bytes(before + b"\n")
        return original(path)

    monkeypatch.setattr(
        campaign_verification,
        "open_campaign_verification",
        change_after_initial_read,
    )
    try:
        with pytest.raises(ValueError, match="campaign identity changed"):
            views.materialize_training_scenario_view(
                campaign_plan,
                campaign_request(campaign_plan, ScenarioViewKind.MEMBER_PANEL),
            )
        assert visited
    finally:
        target.write_bytes(before)
