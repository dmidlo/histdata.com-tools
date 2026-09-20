"""Newly generated fixtures only; no historical inputs or empirical outcomes."""

from dataclasses import replace
from copy import copy
from fractions import Fraction

import pytest

import histdatacom.data_quality.training_weight_generator_diagnostics as diagnostics
from histdatacom.data_quality.training_contracts import (
    training_json,
    training_load,
)
from histdatacom.data_quality.training_weight_candidates import (
    TrainingWeightOperationalFailure,
    _decode_stream,
    _generate_member_symbol,
    _mapping_condition,
)
from histdatacom.data_quality.training_weight_lineage import (
    create_training_weight_degradation,
    WeightEvidenceKind,
)
from histdatacom.synthetic.generation import (
    EmpiricalMotifGeneratorConfigV1,
    _observe_motif_planning,
    _plan_transforms,
    generate_empirical_motif_candidates,
)
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)
from tests.unit.test_synthetic_generation import (
    BASE_NS,
    MEMBER_ID,
    SECOND,
    _anchor,
    _condition,
    _index,
    _result,
    _run,
    _window,
)


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    root = tmp_path_factory.mktemp("generator-diagnostic-fixture")
    plan, _ = fixture_weight_source(
        root / "source", dates=("2010-01-04",), step_seconds=8
    )
    degradation = create_training_weight_degradation(plan, root / "subset")
    model = fixture_weight_model(root / "model")
    return degradation.bridges[0], model, model.index


def _generation_inputs(intensity=8.0):
    condition = _condition(intensity=intensity)
    config = EmpiricalMotifGeneratorConfigV1()
    run = _run(config)
    return dict(
        run=run,
        window=_window(run, BASE_NS, BASE_NS + SECOND + 1),
        left_anchor=_anchor(run, BASE_NS, sequence=1, row_id=1),
        right_anchor=_anchor(run, BASE_NS + SECOND, sequence=2, row_id=2),
        query_result=_result(_index(condition), condition),
        config=config,
    )


@pytest.mark.parametrize("intensity", [0.0, 1.0, 8.0, 100.0])
def test_observer_equivalence_and_context_restoration(intensity):
    kwargs = _generation_inputs(intensity)
    baseline = generate_empirical_motif_candidates(**kwargs)
    observations = []
    with _observe_motif_planning(observations.append):
        observed = generate_empirical_motif_candidates(**kwargs)
    assert observed == baseline
    assert observed.payload() == baseline.payload()
    assert [e.to_dict() for e in observed.events] == [
        e.to_dict() for e in baseline.events
    ]
    assert [t.to_dict() for t in observed.transformations] == [
        t.to_dict() for t in baseline.transformations
    ]
    count = len(observations)
    assert generate_empirical_motif_candidates(**kwargs) == baseline
    assert len(observations) == count
    if observations:
        with pytest.raises(AttributeError):
            observations[0].output_cursor = 999


def test_observer_exception_is_operational_and_does_not_leak():
    kwargs = _generation_inputs()

    def fail(_):
        raise RuntimeError("observer failed")

    with pytest.raises(RuntimeError, match="observer failed"):
        with _observe_motif_planning(fail):
            generate_empirical_motif_candidates(**kwargs)
    assert generate_empirical_motif_candidates(**kwargs).events


def test_complete_twelve_cells_retain_native_lineage_and_roundtrip(inputs):
    bridge, model, index = inputs
    progress = []
    day = diagnostics.diagnose_training_weight_generator_day(
        bridge, model, index, on_cell=lambda m, s: progress.append((m, s))
    )
    assert day.status == "complete"
    assert len(day.cells) == len(progress) == 12
    assert all(
        c.status == "success" and c.not_attempted_intervals == 0
        for c in day.cells
    )
    assert (
        diagnostics.TrainingWeightGeneratorDayDiagnosticV1.from_json(
            day.to_json()
        )
        == day
    )
    for cell in day.cells:
        native = _decode_stream(cell.stream_encoding_json)
        assert native["events"]
        assert all("vol" not in e for e in native["events"])
        records = [training_load(t) for t in cell.interval_records]
        assert any(
            r["planning"] and r["planning"]["target_times_ns"] for r in records
        )
        assert all(
            r["planning"] is not None
            or r["native_batch"]["target_event_count"] == 0
            for r in records
        )
        assert all(
            r["native_batch"]["hard_carving_status"] == "not_evaluated"
            for r in records
        )
    assert diagnostics._embedded_nodes(day.to_dict()) <= 131072
    assert len(day.to_json()) <= 8 * 1024 * 1024


def test_refused_cell_does_not_suppress_other_member_symbol_cells(inputs):
    bridge, model, index = inputs
    fragments = tuple(
        replace(
            f,
            transform_policy=replace(
                f.transform_policy,
                min_time_scale=1.0,
                max_time_scale=1.0,
                policy_id="",
            ),
            fragment_id="",
        )
        for f in index.fragments
    )
    index = replace(index, fragments=fragments, index_id="")
    # Pure typed synthetic model: no byte/source verification claim here.
    model = replace(model, index_json=training_json(index.to_dict()))
    day = diagnostics.diagnose_training_weight_generator_day(
        bridge, model, index
    )
    assert day.status == "complete"
    assert len(day.cells) == 12
    for cell in day.cells:
        assert cell.status == "deterministic_refused"
        assert cell.attempted_intervals >= 1
        assert (
            cell.not_attempted_intervals
            == cell.interval_count - cell.attempted_intervals
        )
        record = training_load(cell.interval_records[-1])
        plan = record["planning"]
        assert plan["refusal"] == "no_admissible_retrieved_time_scale"
        assert plan["segment_seed"].isdigit()
        assert plan["segment_ordinal"] == 1
        assert all(
            r["inside_count"] == 0 for r in plan["retrieved_scale_summaries"]
        )
        assert record["native_batch"]["decision_details"]


def test_expired_deadline_and_bad_index_refuse_before_generation(
    inputs, monkeypatch
):
    bridge, model, index = inputs

    def forbidden(**_):
        pytest.fail("must not generate")

    monkeypatch.setattr(
        diagnostics, "generate_empirical_motif_candidates", forbidden
    )
    with pytest.raises(TrainingWeightOperationalFailure, match="deadline"):
        diagnostics.diagnose_training_weight_generator_day(
            bridge, model, index, deadline=0.0
        )
    with pytest.raises(ValueError, match="index differs"):
        diagnostics.diagnose_training_weight_generator_day(
            bridge,
            model,
            replace(
                index,
                index_id="",
                source_window_count=1,
                fragments=index.fragments[:1],
            ),
        )


def test_day_diagnostic_bound_is_explicit_not_successful_truncation(
    inputs, monkeypatch
):
    bridge, model, index = inputs
    original = diagnostics._Retainer.put
    calls = []

    def bounded(self, value):
        calls.append(value)
        if len(calls) == 4:
            raise diagnostics._DiagnosticLimit("diagnostic_day_byte_bound")
        return original(self, value)

    monkeypatch.setattr(diagnostics._Retainer, "put", bounded)
    day = diagnostics.diagnose_training_weight_generator_day(
        bridge, model, index
    )
    assert day.status == "diagnostic_failed"
    assert day.cells[0].status == "diagnostic_failed"
    assert all(c.status == "not_attempted" for c in day.cells[1:])
    assert all(not c.counts_complete for c in day.cells)
    assert (
        diagnostics.TrainingWeightGeneratorDayDiagnosticV1.from_json(
            day.to_json()
        )
        == day
    )


@pytest.mark.parametrize("count", range(1, 9))
@pytest.mark.parametrize("cadence", [62500000, 125000000, 250000000, 500000000])
def test_planner_small_exhaustive_single_segment_oracle(count, cadence):
    kwargs = _generation_inputs()
    result = kwargs["query_result"]
    fragment = result.matches[0].fragment
    times = tuple(BASE_NS + cadence * i for i in range(1, count + 1))
    observed = []
    with _observe_motif_planning(observed.append):
        plans, error = _plan_transforms(
            run=kwargs["run"],
            ensemble_member_id=MEMBER_ID,
            interval_id="synthetic-independent-oracle",
            event_times=times,
            cadence_ns=cadence,
            left_time_ns=BASE_NS,
            query_result=result,
            config=kwargs["config"],
        )
    cursor = 0
    expected_counts = []
    while cursor < count:
        remaining = count - cursor
        chosen = None
        for n in range(remaining, 0, -1):
            ratio = Fraction(cadence * n * 2, fragment.duration_ns * (n + 1))
            if n == remaining and ratio < Fraction(1, 2):
                terminal = Fraction(
                    cadence * (n + 1) * 2, fragment.duration_ns * (n + 1)
                )
                if Fraction(1, 2) <= terminal <= 2:
                    ratio = terminal
            if Fraction(1, 2) <= ratio <= 2:
                chosen = n
                break
        if chosen is None:
            break
        expected_counts.append(chosen)
        cursor += chosen
    assert observed[0].output_cursor == cursor
    assert [
        t.output_end_ordinal - t.output_start_ordinal + 1
        for t in observed[0].prefix
    ] == expected_counts
    assert (error is None) == (cursor == count)
    assert len(plans) == (len(expected_counts) if error is None else 0)


def test_transformation_limit_retains_successful_prefix():
    kwargs = _generation_inputs()
    # First event is legal, but the long tail cannot be combined into it.
    observed = []
    with _observe_motif_planning(observed.append):
        plans, error = _plan_transforms(
            run=kwargs["run"],
            ensemble_member_id=MEMBER_ID,
            interval_id="synthetic-limit",
            event_times=(BASE_NS + 125000000, BASE_NS + 1000000000),
            cadence_ns=125000000,
            left_time_ns=BASE_NS,
            query_result=kwargs["query_result"],
            config=replace(
                kwargs["config"],
                max_transformations_per_interval=1,
                config_id="",
            ),
        )
    assert not plans and "transformation count" in error
    assert len(observed[0].prefix) == 1
    assert observed[0].refusal == "transformation_count_limit"
    assert observed[0].segment_ordinal == 2
    assert (
        observed[0].segment_seed is None
    )  # Limit checked before seed derivation.


def test_greedy_dead_end_is_observed_not_silently_backtracked():
    """There is another partition, but the frozen engine is intentionally greedy."""
    kwargs = _generation_inputs()
    condition = _condition()
    result = _result(_index(condition, event_offsets_ns=(0, 1, 2)), condition)
    observations = []
    with _observe_motif_planning(observations.append):
        plans, error = _plan_transforms(
            run=kwargs["run"],
            ensemble_member_id=MEMBER_ID,
            interval_id="synthetic-greedy-dead-end",
            event_times=tuple(BASE_NS + t for t in (3, 4, 9)),
            cadence_ns=1,
            left_time_ns=BASE_NS,
            query_result=result,
            config=kwargs["config"],
        )
    assert plans == () and "output ordinal 3" in error
    observation = observations[0]
    assert observation.output_cursor == 2
    assert observation.prefix[0].output_end_ordinal == 2
    assert observation.segment_ordinal == 2
    # Independent exact witness: split1+2 has ratios3/2 and2, both allowed.
    assert Fraction(2 * 3, 2 * 2) == Fraction(3, 2)
    assert Fraction(2 * (9 - 3), 2 * 3) == 2
    # Greedy split2 leaves a scale5/2 >2; the observer does not fix it.
    summary = diagnostics._scale_summaries(observation, result)[0]
    assert summary["tested_count"] == summary["above_count"] == 1
    assert summary["normal_min"] == summary["normal_max"] == 2.5
    assert summary["inside_count"] == 0


@pytest.mark.parametrize(
    "cadence,expected", [(62500000, 0.5), (125000000, 0.5), (500000000, 2.0)]
)
def test_terminal_padding_and_inclusive_exact_scale_boundaries(
    cadence, expected
):
    kwargs = _generation_inputs()
    observations = []
    with _observe_motif_planning(observations.append):
        plans, error = _plan_transforms(
            run=kwargs["run"],
            ensemble_member_id=MEMBER_ID,
            interval_id="synthetic-boundary",
            event_times=(BASE_NS + cadence,),
            cadence_ns=cadence,
            left_time_ns=BASE_NS,
            query_result=kwargs["query_result"],
            config=kwargs["config"],
        )
    assert error is None and plans[0].record.time_scale == expected
    assert observations[0].prefix == (plans[0].record,)


def test_canonical_nested_scope_unknown_fields_and_counter_tampering(inputs):
    day = diagnostics.diagnose_training_weight_generator_day(*inputs)
    cell = day.cells[0]
    record = training_load(cell.interval_records[0])
    record["unknown_later_semantics"] = True
    with pytest.raises(ValueError, match="schema"):
        replace(
            cell,
            interval_records=(
                training_json(record),
                *cell.interval_records[1:],
            ),
        )
    run = training_load(cell.run_json)
    run["extra"] = 1
    with pytest.raises(ValueError, match="unknown/coercive"):
        replace(cell, run_json=training_json(run))
    resource = training_load(day.resource_counts_json)
    resource["charged_component_expanded_nodes"] += 1
    with pytest.raises(ValueError, match="resource counts"):
        replace(day, resource_counts_json=training_json(resource))
    query = training_load(day.query_results[0])
    query["query"]["unrecognized"] = True
    with pytest.raises(ValueError):
        replace(
            day, query_results=(training_json(query), *day.query_results[1:])
        )


def test_diagnostic_record_bound_at_query_publication_is_atomic(
    inputs, monkeypatch
):
    original = diagnostics._Retainer.put
    calls = 0

    def bounded(self, value):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise diagnostics._DiagnosticLimit(
                "diagnostic_day_expanded_node_bound"
            )
        return original(self, value)

    monkeypatch.setattr(diagnostics._Retainer, "put", bounded)
    day = diagnostics.diagnose_training_weight_generator_day(*inputs)
    assert day.status == "diagnostic_failed"
    assert day.query_results == day.query_supports == ()
    assert day.cells[0].attempted_intervals == 1
    assert day.cells[0].interval_records == ()
    assert all(c.status == "not_attempted" for c in day.cells[1:])


def test_declared_triangle_limit_before_any_query(inputs, monkeypatch):
    monkeypatch.setattr(
        diagnostics, "_target_cardinality", lambda *_: (5000, 1)
    )

    def forbidden(*_, **__):
        pytest.fail("query is after bounded preflight")

    monkeypatch.setattr(diagnostics, "query_reference_motifs", forbidden)
    day = diagnostics.diagnose_training_weight_generator_day(*inputs)
    assert day.status == "complete"
    assert all(
        c.status == "deterministic_refused"
        and c.attempted_intervals == 0
        and c.reason == "declared_triangle_event_budget"
        for c in day.cells
    )


def test_unexpected_engine_error_aborts_not_scientific_refusal(
    inputs, monkeypatch
):
    def fail(**_):
        raise ValueError("unexpected engine invariant")

    monkeypatch.setattr(
        diagnostics, "generate_empirical_motif_candidates", fail
    )
    with pytest.raises(ValueError, match="unexpected engine invariant"):
        diagnostics.diagnose_training_weight_generator_day(*inputs)


def test_mixed_bridge_model_evidence_kind_refuses_before_computation(
    inputs, monkeypatch
):
    bridge, model, index = inputs
    # Metadata-only adversarial boundary test; no empirical source or model.
    altered_plan = copy(bridge.source_plan)
    object.__setattr__(
        altered_plan, "evidence_kind", WeightEvidenceKind.PREREGISTERED
    )
    altered_bridge = copy(bridge)
    object.__setattr__(altered_bridge, "source_plan", altered_plan)

    def forbidden(*_, **__):
        pytest.fail("mismatched evidence must fail before model computation")

    monkeypatch.setattr(diagnostics, "_mapping_condition", forbidden)
    with pytest.raises(ValueError, match="evidence kinds differ"):
        diagnostics.diagnose_training_weight_generator_day(
            altered_bridge, model, index
        )


def test_complete_diagnostic_preserves_existing_wrapper_native_bytes(inputs):
    bridge, model, index = inputs
    day = diagnostics.diagnose_training_weight_generator_day(*inputs)
    cell = day.cells[0]
    mapping = bridge.symbols[0]
    previous = _generate_member_symbol(
        bridge,
        model,
        index,
        cell.member_id,
        cell.symbol,
        {},
        _mapping_condition(mapping),
        float("inf"),
    )
    assert cell.run_json == previous.run_json
    assert cell.window_json == previous.window_json
    assert cell.stream_encoding_json == previous.stream_encoding_json
    assert (
        cell.transformations_encoding_json
        == previous.transformations_encoding_json
    )


def test_zero_width_anchor_pairs_are_explicit_without_collapsing_ordinals(
    tmp_path,
):
    plan, _ = fixture_weight_source(
        tmp_path / "source",
        dates=("2010-01-04",),
        step_seconds=8,
        interior_duplicates=8,
    )
    bridge = create_training_weight_degradation(
        plan, tmp_path / "subset"
    ).bridges[0]
    model = fixture_weight_model(tmp_path / "model")
    day = diagnostics.diagnose_training_weight_generator_day(
        bridge, model, model.index
    )
    assert day.status == "complete"
    zeros = [
        training_load(t)
        for c in day.cells
        for t in c.interval_records
        if training_load(t)["decision"] == "no_strict_interior_timestamp"
    ]
    assert zeros
    for row in zeros:
        assert row["left_time_ns"] == row["right_time_ns"]
        assert row["left_anchor_event_id"] != row["right_anchor_event_id"]
        assert "planning" not in row and "query_result_id" not in row
