"""Synthetic structural canaries, not source replay or clock certification."""

from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.data_quality.training_contracts import (
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import (
    JOIN_NAMESPACES,
    JOIN_NONCLAIMS,
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    JoinState,
    TrainingJoinBatchV1,
    TrainingJoinColumnV1,
    TrainingJoinEntityV1,
    TrainingJoinRefusalEvidenceV1,
    TrainingJoinRowV1,
    TrainingJoinSourceV1,
    TrainingJoinValueV1,
    training_join_parent_namespaces,
)
from tests.fixtures.training_join_v1 import join_fixture


def _column(**changes):
    return replace(
        TrainingJoinColumnV1(
            "market.tick.EURUSD.bid",
            "quotes",
            TrainingJoinEntityV1("EURUSD"),
            "bid",
            JoinDirection.EXACT,
            JoinMeaning.STATE,
            0,
        ),
        **changes,
    )


def _value(**changes):
    return replace(
        TrainingJoinValueV1(
            "market.tick.EURUSD.bid",
            JoinState.AVAILABLE,
            '{"value":1.25}',
            "synthetic_contract_example",
            "synthetic-binding",
            ("synthetic-source",),
            "synthetic.schema.v1",
            "synthetic_contract_example_not_verified",
            10,
            10,
            9,
            11,
            ("synthetic-parent",),
        ),
        **changes,
    )


def _refusal(**changes):
    return replace(
        TrainingJoinRefusalEvidenceV1(
            ("synthetic-source",),
            "synthetic.schema.v1",
            "synthetic_contract_example_not_verified",
            10,
            10,
            None,
            9,
            11,
            ("synthetic-parent",),
        ),
        **changes,
    )


@pytest.fixture(scope="module")
def plan(tmp_path_factory):
    return join_fixture(tmp_path_factory.mktemp("join-contract-source"))


def _batch(plan, *, mode=JoinInformationMode.NORMALIZED_AS_OF, **changes):
    """Construct claimed cells solely to exercise the public envelope boundary."""
    normalized = replace(plan, information_mode=mode)
    rows = tuple(
        TrainingJoinRowV1(
            row.artifact_id,
            row.evidence_unit_id,
            row.decision_time_ns,
            (
                _value(
                    source_binding_id=normalized.sources[0].artifact_id,
                    source_time_ns=row.decision_time_ns,
                    available_at_ns=row.decision_time_ns,
                    dependency_start_ns=row.decision_time_ns,
                    dependency_end_ns=row.decision_time_ns + 1,
                ),
            ),
        )
        for row in normalized.spine.rows
    )
    first = rows[0]
    rows = (
        replace(first, values=(replace(first.values[0], **changes),)),
    ) + rows[1:]
    return TrainingJoinBatchV1(normalized, rows)


@pytest.mark.parametrize("namespace", JOIN_NAMESPACES)
def test_registered_namespaces_have_distinct_canonical_columns(namespace):
    column = _column(name=namespace + "EURUSD.value")
    assert TrainingJoinColumnV1.from_json(column.to_json()) == column
    assert column.name == namespace + "EURUSD.value"


@pytest.mark.parametrize(
    "name",
    (
        "bid",
        "market.volume.value",
        "market.tickish.value",
        "Market.tick.EURUSD.bid",
        "market.tick.EURUSD/bid",
        "market.tick.EURUSD bid",
        "market.tick.EURUSD.\nvalue",
        " market.tick.EURUSD.bid",
    ),
)
def test_unknown_or_malformed_namespace_refuses(name):
    with pytest.raises(ValueError):
        _column(name=name)


@pytest.mark.parametrize("currency", (None, "EUR", "USD"))
def test_entity_retains_explicit_pair_currency_economy_and_series(currency):
    entity = TrainingJoinEntityV1("EURUSD", currency, "EA", "gdp")
    assert TrainingJoinEntityV1.from_json(entity.to_json()) == entity
    assert entity.currency == currency
    assert (entity.economy, entity.series) == ("EA", "gdp")


@pytest.mark.parametrize(
    "changes",
    (
        {"symbol": "eurusd"},
        {"symbol": "EUR/USD"},
        {"symbol": "EUR"},
        {"currency": "JPY"},
        {"currency": "usd"},
        {"economy": ""},
        {"series": " value "},
    ),
)
def test_invalid_entity_mapping_refuses(changes):
    with pytest.raises(ValueError):
        replace(TrainingJoinEntityV1("EURUSD"), **changes)


def test_plan_rejects_symbol_or_source_outside_exact_inventory(plan):
    for column in (
        replace(plan.columns[0], entity=TrainingJoinEntityV1("AUDCAD")),
        replace(plan.columns[0], source_key="not-retained"),
    ):
        with pytest.raises(ValueError, match="source or graph inventory"):
            replace(plan, columns=(column,))


@pytest.mark.parametrize(
    "meaning", (JoinMeaning.EVENT, JoinMeaning.SNAPSHOT, JoinMeaning.CLOSED_BAR)
)
def test_nonpersistent_semantics_are_exact_zero_age_only(meaning):
    valid = _column(meaning=meaning)
    assert TrainingJoinColumnV1.from_json(valid.to_json()) == valid
    for direction, age in (
        (JoinDirection.EXACT, 1),
        (JoinDirection.BOUNDED_PRIOR, 1),
        (JoinDirection.INTERVAL, 0),
    ):
        with pytest.raises(ValueError):
            replace(valid, direction=direction, max_age_ns=age)


def test_persistent_state_has_explicit_age_and_direction_rules():
    for direction, age in (
        (JoinDirection.PRIOR, 0),
        (JoinDirection.BOUNDED_PRIOR, 1),
        (JoinDirection.INTERVAL, 0),
    ):
        column = _column(direction=direction, max_age_ns=age)
        assert column.meaning is JoinMeaning.STATE
        assert TrainingJoinColumnV1.from_json(column.to_json()) == column
    with pytest.raises(ValueError, match="positive frozen age"):
        _column(direction=JoinDirection.BOUNDED_PRIOR)
    with pytest.raises(ValueError, match="zero source age"):
        _column(max_age_ns=1)


@pytest.mark.parametrize("bad_clock", (True, 1.0, "1", -1, 2**63))
def test_all_join_nanosecond_fields_require_exact_nonnegative_int64(bad_clock):
    with pytest.raises(ValueError):
        _column(max_age_ns=bad_clock)
    with pytest.raises(ValueError):
        TrainingJoinRowV1("spine", "unit", bad_clock, ())
    for name in (
        "source_time_ns",
        "available_at_ns",
        "dependency_start_ns",
        "dependency_end_ns",
    ):
        with pytest.raises(ValueError):
            _value(**{name: bad_clock})
        with pytest.raises(ValueError):
            _refusal(**{name: bad_clock})
    with pytest.raises(ValueError):
        _refusal(selection_time_ns=bad_clock)


@pytest.mark.parametrize(
    "family,expected",
    (
        (JoinFamily.TICK, ("lineage.quote_record.EURUSD",)),
        (
            JoinFamily.BAR,
            ("market.tick.EURUSD.quote", "lineage.closed_bar_support.EURUSD"),
        ),
        (
            JoinFamily.INDICATOR,
            ("market.tick.EURUSD.quote", "lineage.closed_bar_support.EURUSD"),
        ),
        (
            JoinFamily.TRIANGLE,
            (
                "market.tick.EURGBP.quote",
                "market.tick.EURUSD.quote",
                "market.tick.GBPUSD.quote",
                "lineage.triangle_closed_bar_support",
            ),
        ),
        (JoinFamily.CALENDAR, ("lineage.calendar_release_or_schedule",)),
        (
            JoinFamily.VINTAGE,
            (
                "lineage.vintage_observations",
                "lineage.vintage_metadata_schedule_transform",
            ),
        ),
        (
            JoinFamily.FORECAST,
            (
                "lineage.forecast_training_inputs",
                "lineage.forecast_inference_inputs",
                "lineage.forecast_model_execution",
            ),
        ),
        (
            JoinFamily.POSITIONING,
            (
                "lineage.cftc_raw_release_and_vintage",
                "lineage.cftc_symbol_mapping",
            ),
        ),
        (
            JoinFamily.ACTIVITY,
            ("market.tick.EURUSD.quote", "lineage.committed_activity_product"),
        ),
        (
            JoinFamily.BROKER,
            (
                "lineage.broker_capture_sessions",
                "lineage.broker_full_support_fit",
            ),
        ),
        (JoinFamily.UNSUPPORTED, ()),
    ),
)
def test_derived_parent_namespaces_are_exact_native_roles(
    plan, family, expected
):
    source = TrainingJoinSourceV1("quotes", family)
    column = _column()
    assert training_join_parent_namespaces(family, column) == expected
    column = replace(column, parent_namespaces=expected)
    valid = replace(plan, sources=(source,), columns=(column,))
    assert valid.columns[0].parent_namespaces == expected
    for wrong in (
        expected + ("lineage.invented_dependency",),
        ("lineage.unrelated_source",),
    ):
        with pytest.raises(ValueError, match="frozen native dependency"):
            replace(valid, columns=(replace(column, parent_namespaces=wrong),))
    if expected:
        with pytest.raises(ValueError, match="frozen native dependency"):
            replace(valid, columns=(replace(column, parent_namespaces=()),))
    if len(expected) > 1:
        with pytest.raises(ValueError, match="frozen native dependency"):
            replace(
                valid,
                columns=(replace(column, parent_namespaces=expected[::-1]),),
            )


def test_triangle_projection_adds_its_delivery_parent():
    base = _column(field="residual")
    projected = replace(base, field="projection.residual")
    native = training_join_parent_namespaces(JoinFamily.TRIANGLE, base)
    assert native[:3] == (
        "market.tick.EURGBP.quote",
        "market.tick.EURUSD.quote",
        "market.tick.GBPUSD.quote",
    )
    assert training_join_parent_namespaces(
        JoinFamily.TRIANGLE, projected
    ) == native + ("lineage.triangle_projection_delivery",)


def test_duplicate_self_and_malformed_parent_namespaces_refuse():
    for parents in (
        ("lineage.same", "lineage.same"),
        ("market.tick.EURUSD.bid",),
        ("not_a_namespace",),
    ):
        with pytest.raises(ValueError):
            _column(parent_namespaces=parents)


@pytest.mark.parametrize("value", (True, False, 0, 2**63 - 1, 1.25, -0.0, "x"))
def test_wide_scalar_values_keep_exact_json_scalar_type(value):
    cell = _value(value_json=training_json({"value": value}))
    restored = TrainingJoinValueV1.from_json(cell.to_json())
    scalar = training_load(restored.value_json)["value"]
    assert type(scalar) is type(value)
    assert restored.value_json == training_json({"value": value})
    assert restored == cell


@pytest.mark.parametrize(
    "text",
    (
        '{"value":[]}',
        '{"value":{}}',
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":9223372036854775808}',
        '{"value":1,"extra":true}',
        '{"value":1,"value":2}',
        '{ "value":1}',
        '{"value":1e0}',
        "[1]",
    ),
)
def test_wide_value_is_one_finite_canonical_scalar(text):
    with pytest.raises(ValueError):
        _value(value_json=text)


@pytest.mark.parametrize("state", tuple(JoinState))
def test_null_state_consistency_is_not_truthiness(state):
    if state is JoinState.AVAILABLE:
        with pytest.raises(ValueError, match="value/null state"):
            _value(value_json='{"value":null}')
        return
    valid = _value(state=state, value_json='{"value":null}')
    assert TrainingJoinValueV1.from_json(valid.to_json()) == valid
    for scalar in (0, False, ""):
        with pytest.raises(ValueError, match="value/null state"):
            replace(valid, value_json=training_json({"value": scalar}))


@pytest.mark.parametrize(
    "changes",
    (
        {"source_ids": ()},
        {"source_time_ns": None},
        {"dependency_start_ns": None, "dependency_end_ns": None},
        {"dependency_start_ns": None},
        {"dependency_end_ns": None},
        {"dependency_end_ns": 9},
        {"source_ids": ("duplicate", "duplicate")},
        {"parent_source_ids": ("duplicate", "duplicate")},
        {"source_schema": ""},
        {"origin": ""},
        {"source_binding_id": ""},
    ),
)
def test_available_cells_require_consistent_native_provenance(changes):
    with pytest.raises(ValueError):
        _value(**changes)


def test_refusal_evidence_is_value_free_and_cannot_accompany_available():
    evidence = _refusal(source_time_ns=20, available_at_ns=30)
    cell = _value(
        state=JoinState.UNKNOWN_AVAILABILITY,
        value_json='{"value":null}',
        refusal_evidence=(evidence,),
    )
    assert TrainingJoinValueV1.from_json(cell.to_json()) == cell
    assert "value" not in evidence.payload()
    assert "value_json" not in evidence.payload()
    with pytest.raises(ValueError, match="only accompanies nulls"):
        _value(refusal_evidence=(evidence,))
    with pytest.raises(ValueError, match="native source support"):
        _refusal(source_ids=())


def test_no_constructor_or_wire_flag_can_promote_historical_availability():
    for state, value in (
        (JoinState.AVAILABLE, 1),
        (JoinState.UNKNOWN_AVAILABILITY, None),
    ):
        with pytest.raises(ValueError, match="cannot attest historical"):
            _value(
                state=state,
                value_json=training_json({"value": value}),
                historical_availability_verified=True,
            )
    for incorrect_bool in (0, 1, "false", None):
        with pytest.raises(ValueError, match="exact types"):
            _value(historical_availability_verified=incorrect_bool)
    wire = _value().to_dict()
    wire["historical_availability_verified"] = True
    with pytest.raises(ValueError, match="cannot attest historical"):
        TrainingJoinValueV1.from_dict(wire)


def test_normalized_boundary_retains_spine_and_all_nonclaims(plan):
    batch = _batch(plan)
    assert TrainingJoinBatchV1.from_json(batch.to_json()) == batch
    assert batch.plan.spine.to_json() == plan.spine.to_json()
    assert all(row.available_at_ns is None for row in batch.plan.spine.rows)
    assert batch.nonclaims == JOIN_NONCLAIMS
    assert all(
        not value.historical_availability_verified
        for row in batch.rows
        for value in row.values
    )
    with pytest.raises(ValueError, match="retain nonclaims"):
        replace(batch, nonclaims=batch.nonclaims[:-1])
    with pytest.raises(FrozenInstanceError):
        batch.rows[0].values[0].historical_availability_verified = True


@pytest.mark.parametrize(
    "clock,delta",
    (
        ("source_time_ns", 1),
        ("available_at_ns", 1),
        ("available_at_ns", None),
        ("dependency_end_ns", 2),
    ),
)
def test_normalized_future_or_unknown_support_refuses_but_expost_is_explicit(
    plan, clock, delta
):
    decision = plan.spine.rows[0].decision_time_ns
    changes = {clock: None if delta is None else decision + delta}
    with pytest.raises(ValueError, match="future/unknown support"):
        _batch(plan, **changes)
    expost = _batch(plan, mode=JoinInformationMode.EX_POST, **changes)
    assert TrainingJoinBatchV1.from_json(expost.to_json()) == expost
    assert not expost.rows[0].values[0].historical_availability_verified
    assert expost.plan.spine.to_json() == plan.spine.to_json()


def test_normalized_null_can_retain_future_value_free_refusal_without_promotion(
    plan,
):
    decision = plan.spine.rows[0].decision_time_ns
    refused = _refusal(
        selection_time_ns=decision + 1,
        source_time_ns=decision + 1,
        available_at_ns=decision + 2,
        dependency_start_ns=decision,
        dependency_end_ns=decision + 3,
    )
    batch = _batch(
        plan,
        state=JoinState.UNKNOWN_AVAILABILITY,
        value_json='{"value":null}',
        refusal_evidence=(refused,),
    )
    assert TrainingJoinBatchV1.from_json(batch.to_json()) == batch
    assert batch.rows[0].values[0].value_json == '{"value":null}'
    assert batch.rows[0].values[0].refusal_evidence == (refused,)
    assert batch.plan.spine.to_json() == plan.spine.to_json()
