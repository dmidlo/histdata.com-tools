"""Pure structural vectors; these fabricated roots never authorize native views."""

import json
from dataclasses import replace
from fractions import Fraction as F

import pytest

from histdatacom.data_quality.training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingConsumerMode,
    TrainingEvidenceUnitV1,
    TrainingOwnershipV1,
    TrainingRootV1,
    TrainingSourceV1,
    TrainingVerificationLevel,
)
from histdatacom.data_quality.training_scenario_contracts import (
    SCENARIO_AXES,
    SCENARIO_FEATURES,
    ScenarioAxis,
    ScenarioAxisState,
    ScenarioMemberStatus,
    ScenarioViewKind,
    TrainingScenarioAxisV1,
    TrainingScenarioCampaignBindingV1,
    TrainingScenarioCoordinateV1,
    TrainingScenarioDenominatorV1,
    TrainingScenarioMemberV1,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRationalV1,
    TrainingScenarioRequestV1,
    TrainingScenarioResearchBindingV1,
    TrainingScenarioScalarV1,
    TrainingScenarioSummaryV1,
    TrainingScenarioViewV1,
    TrainingScenarioWeightV1,
    readmit_scenario,
)


def rational(value):
    return TrainingScenarioRationalV1.from_fraction(F(value))


@pytest.fixture
def structure():
    root = TrainingRootV1(
        "synthetic-structure-only",
        "root",
        "a" * 64,
        TrainingVerificationLevel.SOURCE_BYTES,
    )
    unit = TrainingEvidenceUnitV1("dataset", 0, 100, ("EURUSD",), "b" * 64)
    ownership = TrainingOwnershipV1("dataset", (unit,), (root,))
    plan = TrainingScenarioPlanV1(
        TrainingSourceV1("{}", "dataset"), ownership, TrainingScenarioPolicyV1()
    )
    axes = tuple(
        TrainingScenarioAxisV1(
            axis,
            (
                ScenarioAxisState.KNOWN
                if axis is ScenarioAxis.PATH_REALIZATION
                else ScenarioAxisState.NOT_APPLICABLE
            ),
            "member-a" if axis is ScenarioAxis.PATH_REALIZATION else None,
            ("native-evidence",),
        )
        for axis in SCENARIO_AXES
    )
    member = TrainingScenarioMemberV1(
        unit.artifact_id,
        "member-a",
        0,
        100,
        ("config",),
        ("run",),
        ("product",),
        axes,
        ScenarioMemberStatus.ELIGIBLE,
        (),
        ("native-evidence",),
    )
    request = TrainingScenarioRequestV1(
        ScenarioViewKind.MEMBER_PANEL,
        TrainingConsumerMode.DESCRIPTIVE,
        0,
        100,
        ("EURUSD",),
    )
    view = TrainingScenarioViewV1(
        plan,
        request,
        (member,),
        (),
        (),
        (),
        TrainingScenarioDenominatorV1(1, 1, 0, 0, 0),
        (),
        SCENARIO_AXES,
        (root,),
        selected_member_keys=(member.member_key,),
    )
    return plan, request, member, view


def test_exact_canonical_roundtrips_and_detached_readmission(structure):
    for value in structure:
        assert type(value).from_json(value.to_json()) == value
        assert readmit_scenario(value, type(value)) == value
        assert readmit_scenario(value, type(value)) is not value


@pytest.mark.parametrize(
    "numerator,denominator",
    [
        ("-0", "1"),
        ("01", "1"),
        ("1", "0"),
        ("1", "-1"),
        ("2", "4"),
        ("+1", "2"),
        ("0", "2"),
        ("1.0", "1"),
    ],
)
def test_noncanonical_rationals_refuse(numerator, denominator):
    with pytest.raises(ValueError):
        TrainingScenarioRationalV1(numerator, denominator)


@pytest.mark.parametrize("value", [0, True, 0.1])
def test_rational_factory_requires_exact_fraction(value):
    with pytest.raises(ValueError):
        TrainingScenarioRationalV1.from_fraction(value)


def test_binary64_rational_roundtrip():
    value = F.from_float(0.1)
    assert rational(value).value == value
    assert rational(value).value != F(1, 10)


def test_unknown_duplicate_identity_and_schema_fields_refuse(structure):
    value = structure[1]
    for key, replacement in (
        ("extra", 1),
        ("artifact_id", "forged"),
        ("schema_version", "v99"),
    ):
        wire = value.to_dict()
        wire[key] = replacement
        with pytest.raises(ValueError):
            type(value).from_dict(wire)
    with pytest.raises(ValueError, match="JSON"):
        type(value).from_json('{"view":"observed_only","view":"member_panel"}')


@pytest.mark.parametrize("view", tuple(ScenarioViewKind))
def test_all_six_views_representable_but_not_authorized(view):
    request = TrainingScenarioRequestV1(
        view, TrainingConsumerMode.DESCRIPTIVE, 0, 1, ("EURUSD",)
    )
    assert TrainingScenarioRequestV1.from_json(request.to_json()) == request


@pytest.mark.parametrize(
    "kwargs",
    [
        {"epoch": True},
        {"start_ns": True},
        {"view": "observed_only"},
        {"symbols": ["EURUSD"]},
    ],
)
def test_exact_types_refuse_before_shared_wire(structure, kwargs):
    with pytest.raises(TypeError):
        replace(structure[1], **kwargs)


def test_causal_and_nonsampling_epoch_refuse(structure):
    with pytest.raises(ValueError, match="causal"):
        replace(structure[1], consumer_mode=TrainingConsumerMode.CAUSAL)
    with pytest.raises(ValueError, match="epoch"):
        replace(structure[1], epoch=1)


def test_nested_subclass_serializer_never_called(structure):
    class Poison(TrainingScenarioPolicyV1):
        def to_json(self):
            raise AssertionError("subclass serializer was called")

    poison = object.__new__(Poison)
    with pytest.raises(TypeError, match="exact"):
        replace(structure[0], policy=poison)


@pytest.mark.parametrize("axis", tuple(ScenarioAxis))
def test_all_axes_distinguish_known_not_applicable_unavailable(axis):
    for state in ScenarioAxisState:
        record = TrainingScenarioAxisV1(
            axis,
            state,
            "actual-native-value" if state is ScenarioAxisState.KNOWN else None,
            ("native-evidence",),
        )
        assert type(record).from_json(record.to_json()) == record
    with pytest.raises(ValueError):
        TrainingScenarioAxisV1(
            axis,
            ScenarioAxisState.UNAVAILABLE,
            "caller-label",
            ("native-evidence",),
        )
    with pytest.raises(ValueError):
        TrainingScenarioAxisV1(
            axis, ScenarioAxisState.KNOWN, None, ("native-evidence",)
        )


def test_unsupported_features_calibrated_weights_and_unsupported_collapse():
    assert set(SCENARIO_FEATURES) == {
        "event_count",
        "observed_count",
        "synthetic_count",
        "bid_mean",
        "ask_mean",
        "spread_mean",
        "bid_min",
        "bid_max",
        "ask_min",
        "ask_max",
    }
    for feature in ("timestamp_mean", "category_mean", "event_path_mean"):
        with pytest.raises(ValueError):
            TrainingScenarioPolicyV1(features=(feature,))
    with pytest.raises(ValueError, match="calibrated"):
        TrainingScenarioPolicyV1(weight_policy="uncertainty-calibrated607")
    for axis in (ScenarioAxis.BROKER, ScenarioAxis.POPULATION):
        with pytest.raises(ValueError):
            TrainingScenarioPolicyV1(collapse_axes=(axis,))


def test_member_key_excludes_physical_proofs_but_binds_science(structure):
    member = structure[2]
    assert (
        replace(
            member,
            product_manifest_ids=("other-product",),
            run_ids=("other-run",),
            native_evidence_ids=("other-evidence",),
        ).member_key
        == member.member_key
    )
    assert (
        replace(member, generator_config_ids=("other-config",)).member_key
        != member.member_key
    )
    assert (
        replace(member, ensemble_member_id="other-member").member_key
        != member.member_key
    )


def test_member_requires_complete_axis_inventory_and_native_reasons(structure):
    member = structure[2]
    with pytest.raises(ValueError, match="every"):
        replace(member, axes=member.axes[:-1])
    with pytest.raises(ValueError, match="reasons"):
        replace(member, status=ScenarioMemberStatus.REFUSED)
    with pytest.raises(ValueError, match="product"):
        replace(member, product_manifest_ids=())


def test_denominator_conservation_and_exact_integer_types():
    assert TrainingScenarioDenominatorV1(4, 1, 1, 1, 1).total == 4
    with pytest.raises(ValueError):
        TrainingScenarioDenominatorV1(3, 1, 1, 1, 1)
    with pytest.raises(TypeError):
        TrainingScenarioDenominatorV1(True, 1, 0, 0, 0)


def test_empty_count_is_zero_but_empty_quote_is_not_zero(structure):
    member = structure[2]
    count = TrainingScenarioCoordinateV1(
        member.evidence_unit_id, "EURUSD", 0, 100, "event_count"
    )
    scalar = TrainingScenarioScalarV1(
        count, member.member_key, rational(0), ScenarioMemberStatus.EMPTY
    )
    quote = replace(count, feature="bid_mean")
    assert replace(scalar, coordinate=quote, value=None).value is None
    with pytest.raises(ValueError):
        replace(scalar, coordinate=quote)
    with pytest.raises(ValueError):
        replace(scalar, value=rational(1))


def test_summary_refuses_renormalizing_missing_members(structure):
    member = structure[2]
    coordinate = TrainingScenarioCoordinateV1(
        member.evidence_unit_id, "EURUSD", 0, 100, "event_count"
    )
    keys = tuple(sorted((member.member_key, "second")))
    with pytest.raises(ValueError, match="renormalize"):
        TrainingScenarioSummaryV1(
            coordinate,
            (),
            keys,
            TrainingScenarioDenominatorV1(2, 1, 0, 1, 0),
            tuple(TrainingScenarioWeightV1(k, rational(F(1, 2))) for k in keys),
            rational(2),
            rational(0),
            (),
        )


def test_view_denominator_and_axis_partition_refuse(structure):
    view = structure[3]
    with pytest.raises(ValueError, match="counts"):
        replace(view, denominator=TrainingScenarioDenominatorV1(1, 0, 0, 1, 0))
    with pytest.raises(ValueError, match="partition"):
        replace(view, preserved_axes=SCENARIO_AXES[:-1])


def summary_view(structure):
    member, view = structure[2:]
    coordinate = TrainingScenarioCoordinateV1(
        member.evidence_unit_id, "EURUSD", 0, 100, "event_count"
    )
    scalar = TrainingScenarioScalarV1(
        coordinate,
        member.member_key,
        rational(3),
        ScenarioMemberStatus.ELIGIBLE,
    )
    axes = tuple(
        a for a in member.axes if a.axis is not ScenarioAxis.PATH_REALIZATION
    )
    summary = TrainingScenarioSummaryV1(
        coordinate,
        axes,
        (member.member_key,),
        view.denominator,
        (TrainingScenarioWeightV1(member.member_key, rational(1)),),
        rational(3),
        rational(0),
        (),
        generator_config_ids=member.generator_config_ids,
    )
    return replace(
        view,
        request=replace(
            view.request, view=ScenarioViewKind.MARGINALIZED_FEATURES
        ),
        scalars=(scalar,),
        summaries=(summary,),
        collapsed_axes=(ScenarioAxis.PATH_REALIZATION,),
        preserved_axes=tuple(a.axis for a in axes),
    )


def test_view_recomputes_mean_and_variance_from_retained_scalars(structure):
    view = summary_view(structure)
    assert TrainingScenarioViewV1.from_json(view.to_json()) == view
    with pytest.raises(ValueError, match="arithmetic"):
        replace(view, summaries=(replace(view.summaries[0], mean=rational(4)),))
    with pytest.raises(ValueError, match="arithmetic"):
        replace(
            view,
            summaries=(
                replace(view.summaries[0], population_variance=rational(1)),
            ),
        )


def test_summary_does_not_pool_distinct_generator_laws(structure):
    view = summary_view(structure)
    other = replace(view.members[0], generator_config_ids=("another-law",))
    combined = replace(
        view,
        members=tuple(
            sorted((*view.members, other), key=lambda m: m.member_key)
        ),
        denominator=TrainingScenarioDenominatorV1(2, 2, 0, 0, 0),
        selected_member_keys=tuple(
            sorted((view.members[0].member_key, other.member_key))
        ),
    )
    assert combined.summaries[0].member_keys == view.summaries[0].member_keys
    with pytest.raises(ValueError, match="membership"):
        replace(
            view,
            summaries=(
                replace(
                    view.summaries[0], generator_config_ids=("another-law",)
                ),
            ),
        )


def test_unknown_collapsed_axis_refuses_numeric_summary(structure):
    view = summary_view(structure)
    member = view.members[0]
    axes = tuple(
        (
            replace(a, state=ScenarioAxisState.UNAVAILABLE, value_id=None)
            if a.axis is ScenarioAxis.PATH_REALIZATION
            else a
        )
        for a in member.axes
    )
    unknown = replace(member, axes=axes)
    summary = replace(
        view.summaries[0],
        member_keys=(unknown.member_key,),
        weights=(TrainingScenarioWeightV1(unknown.member_key, rational(1)),),
    )
    with pytest.raises(ValueError, match="unknown scenario axis"):
        replace(
            view,
            members=(unknown,),
            selected_member_keys=(unknown.member_key,),
            scalars=(replace(view.scalars[0], member_key=unknown.member_key),),
            summaries=(summary,),
        )


def test_incomplete_member_support_is_unavailable_not_trimmed(structure):
    member, view = structure[2:]
    partial = replace(member, end_ns=50)
    coordinate = TrainingScenarioCoordinateV1(
        member.evidence_unit_id, "EURUSD", 0, 100, "event_count"
    )
    scalar = TrainingScenarioScalarV1(
        coordinate,
        partial.member_key,
        None,
        ScenarioMemberStatus.UNAVAILABLE,
        ("incomplete_member_support",),
    )
    assert (
        replace(view, members=(partial,), scalars=(scalar,)).scalars[0].value
        is None
    )
    with pytest.raises(ValueError, match="incomplete"):
        replace(
            view,
            members=(partial,),
            scalars=(
                replace(
                    scalar,
                    value=rational(2),
                    status=ScenarioMemberStatus.ELIGIBLE,
                    reason_codes=(),
                ),
            ),
        )
    with pytest.raises(ValueError, match="fixed common"):
        replace(
            view,
            members=(partial,),
            scalars=(
                replace(scalar, coordinate=replace(coordinate, end_ns=50)),
            ),
        )


@pytest.mark.parametrize(
    "kind",
    (
        ScenarioViewKind.CENTRAL_COUNTERFACTUAL,
        ScenarioViewKind.SAMPLE_ONE_MEMBER,
    ),
)
def test_exactly_one_selected_joint_member_per_unit(structure, kind):
    member, view = structure[2:]
    other = replace(
        member,
        ensemble_member_id="member-b",
        generator_config_ids=("other-law",),
    )
    members = tuple(sorted((member, other), key=lambda m: m.member_key))
    policy = replace(view.plan.policy, central_member_keys=(member.member_key,))
    selected = replace(
        view,
        plan=replace(view.plan, policy=policy),
        request=replace(view.request, view=kind),
        members=members,
        denominator=TrainingScenarioDenominatorV1(2, 2, 0, 0, 0),
    )
    assert selected.selected_member_keys == (member.member_key,)
    for keys in ((), tuple(m.member_key for m in members)):
        with pytest.raises(ValueError, match="exactly one"):
            replace(selected, selected_member_keys=keys)
    refused = replace(
        member,
        status=ScenarioMemberStatus.REFUSED,
        reason_codes=("native_refusal",),
    )
    retained = replace(
        selected,
        members=tuple(sorted((refused, other), key=lambda m: m.member_key)),
        denominator=TrainingScenarioDenominatorV1(2, 1, 0, 1, 0),
    )
    assert retained.selected_member_keys == (refused.member_key,)
    assert retained.rows == ()


def test_view_selection_and_collapsed_axis_shapes(structure):
    view = structure[3]
    with pytest.raises(ValueError, match="complete native roster"):
        replace(view, selected_member_keys=())
    with pytest.raises(ValueError, match="observed-only"):
        replace(
            view,
            request=replace(view.request, view=ScenarioViewKind.OBSERVED_ONLY),
        )
    observed = replace(
        view,
        request=replace(view.request, view=ScenarioViewKind.OBSERVED_ONLY),
        selected_member_keys=(),
    )
    assert observed.members == view.members  # The full denominator is retained.
    with pytest.raises(ValueError, match="collapsed axes"):
        replace(
            view,
            collapsed_axes=(ScenarioAxis.PATH_REALIZATION,),
            preserved_axes=tuple(
                a
                for a in SCENARIO_AXES
                if a is not ScenarioAxis.PATH_REALIZATION
            ),
        )
    summary = summary_view(structure)
    with pytest.raises(ValueError, match="collapsed axes"):
        replace(summary, collapsed_axes=(), preserved_axes=SCENARIO_AXES)


def test_campaign_binding_is_only_locator_and_exact_hash():
    value = TrainingScenarioCampaignBindingV1(
        "/absent/index.json", "index", "a" * 64
    )
    assert type(value).from_json(value.to_json()) == value
    with pytest.raises(ValueError):
        replace(value, index_sha256="not-a-hash")


def test_research_binding_is_canonical_and_exclusive(structure):
    binding = TrainingScenarioResearchBindingV1(
        "{}", "training-scenario-research-recipe:sha256:" + "a" * 64
    )
    assert (
        replace(structure[0], research_binding=binding).research_binding
        == binding
    )
    with pytest.raises(ValueError, match="exclusive"):
        replace(
            structure[0],
            research_binding=binding,
            campaign_binding=TrainingScenarioCampaignBindingV1(
                "/index", "index", "a" * 64
            ),
        )
    with pytest.raises(ValueError, match="canonical"):
        replace(binding, recipe_json="{ }")
    with pytest.raises(ValueError, match="identity"):
        replace(binding, expected_recipe_id="arbitrary-proof-token")


@pytest.mark.parametrize(
    "axis",
    tuple(a for a in ScenarioAxis if a is not ScenarioAxis.PATH_REALIZATION),
)
def test_context_axes_cannot_be_averaged_as_probabilities(axis):
    with pytest.raises(ValueError, match="only path"):
        TrainingScenarioPolicyV1(collapse_axes=(axis,))


def test_delta_budget_refuses_before_json_encoding(monkeypatch):
    def poison(*args, **kwargs):
        raise AssertionError("oversized input reached JSON encoding")

    monkeypatch.setattr(json, "dumps", poison)
    with pytest.raises(ValueError, match="budget"):
        TrainingScenarioPolicyV1(
            sampling_seed="\x7f" * (MAX_TRAINING_BYTES // 6 + 1)
        )


@pytest.mark.parametrize(
    "value",
    (
        "",
        "ASCII nested JSON text " * 2048,
        "".join(chr(i) for i in range(32, 127)),
        '\\"/\b\f\n\r\t',
        "".join(chr(i) for i in range(32)),
        "\x7f\x80\u00ff\u0100\u2028\u2029\uffff",
        "\ud800\udbff\udc00\udfff",
        "\U00010000\U0001f600\U0010ffff",
        "".join(chr(i) for i in range(65536)),
        'quote:" slash:\\ DEL:\x7f pair:\ud800\udc00 nonBMP:\U00010000',
    ),
    ids=(
        "empty",
        "large-ascii",
        "printable-ascii",
        "short-escapes",
        "all-controls",
        "del-and-bmp",
        "lone-surrogates",
        "non-bmp",
        "entire-bmp",
        "mixed",
    ),
)
def test_exact_string_budget_matches_independent_json_encoding(value):
    from histdatacom.data_quality import training_scenario_contracts as module

    expected = len(json.dumps(value, ensure_ascii=True).encode("ascii"))
    assert module._ascii_string_bytes(value, expected) == expected
    budget = [2, expected]
    module._exact(value, str, budget)
    assert budget == [1, 0]
    with pytest.raises(ValueError, match="budget"):
        module._ascii_string_bytes(value, expected - 1)


def test_exact_string_budget_accumulates_across_nested_strings():
    from histdatacom.data_quality import training_scenario_contracts as module

    values = ('a"b', "\udfff", "\U0001f600", "plain")
    cost = sum(len(json.dumps(value, ensure_ascii=True)) for value in values)
    budget = [100, cost]
    module._exact(values, tuple[str, ...], budget)
    assert budget == [95, 0]
    with pytest.raises(ValueError, match="budget"):
        module._exact(values, tuple[str, ...], [100, cost - 1])


def test_escaped_cost_refuses_early_without_encoding(monkeypatch):
    from histdatacom.data_quality import training_scenario_contracts as module

    seen = []

    def counted_ord(character):
        seen.append(character)
        return ord(character)

    def poison(*args, **kwargs):
        raise AssertionError("prospective count must not encode JSON")

    monkeypatch.setattr(module, "ord", counted_ord, raising=False)
    monkeypatch.setattr(json, "dumps", poison)
    # The cheap lower bound fits; escaped bytes exceed the budget at char four.
    with pytest.raises(ValueError, match="budget"):
        module._exact("\u00e9" * 20, str, [100, 24])
    assert seen == ["\u00e9"] * 4
    seen.clear()
    with pytest.raises(ValueError, match="budget"):
        module._exact("a" * 30, str, [100, 24])
    assert seen == []  # Impossible raw lengths refuse before scanning.


def test_final_serialized_byte_guard_remains_active(monkeypatch):
    from histdatacom.data_quality import training_contracts as base

    monkeypatch.setattr(base, "MAX_TRAINING_BYTES", 64)
    value = "\x7f" * 11
    assert len(json.dumps(value, ensure_ascii=True)) == 68
    with pytest.raises(ValueError, match="aggregate byte bound"):
        base.training_json(value)
