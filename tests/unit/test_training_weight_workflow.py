"""Newly generated source/native workflow fixtures; not empirical evidence."""

import hashlib
import json
import math
from dataclasses import replace
from fractions import Fraction

import pytest
import polars as pl

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file

from histdatacom.data_quality import training_weight_workflow as workflow
from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingVerificationLevel,
    TrainingSourceV1,
    training_json,
)
from histdatacom.data_quality.training_weight_calibration import (
    WEIGHT_MEMBERS,
    WEIGHT_PIP,
    WeightSupport,
)
from histdatacom.data_quality.training_weight_candidates import (
    _encode_stream,
    generate_training_weight_candidate_shard,
)
from histdatacom.data_quality.training_weight_lineage import (
    CORE_START_NS,
    CORE_END_NS,
    SECOND_NS,
    WeightEvidenceKind,
    TrainingWeightSourcePlanV1,
    _day_ns,
    create_training_weight_degradation,
)
from histdatacom.data_quality.training_weights import TrainingMemberPolicy
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)


def _late_boundary_plan(plan, directory):
    """New IPC bytes whose permitted right support lies after the core."""
    adapter = HistDataProviderAdapter()
    root = directory / "ASCII" / "T"
    for symbol in ("EURGBP", "EURUSD", "GBPUSD"):
        path = histdata_cache_path(root, symbol, plan.period)
        frame = pl.read_ipc(path, memory_map=False)
        frame = frame.with_columns(
            pl.when(pl.col("datetime") % 86_400_000 == CORE_END_NS // 1_000_000)
            .then(pl.col("datetime") + 30_000)
            .otherwise(pl.col("datetime"))
            .alias("datetime")
        )
        frame.write_ipc(path)
    descriptor = DatasetDescriptorV1(
        "training-weight-contract-fixture",
        "Late-boundary fixture",
        "New synthetic IPC bytes, not market evidence.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=("EURGBP", "EURUSD", "GBPUSD"),
        periods=(plan.period,),
        qualification_evidence=(
            artifact_ref_for_file(
                directory / "fixture-evidence.json",
                kind="synthetic-contract-fixture",
            ),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return TrainingWeightSourcePlanV1(
        TrainingSourceV1(catalog.to_json(), version.dataset_version_id),
        plan.role,
        WeightEvidenceKind.FIXTURE,
    )


@pytest.fixture(scope="module")
def source_workflow(tmp_path_factory):
    directory = tmp_path_factory.mktemp("weight-workflow")
    model = fixture_weight_model(directory / "model")
    shards = []
    for role, date, period in (
        ("calibration", "2010-01-04", "201001"),
        ("calibration", "2011-01-01", "201101"),
        ("application", "2011-02-01", "201102"),
    ):
        plan, _ = fixture_weight_source(
            directory / period, dates=(date,), period=period
        )
        if role == "application":
            plan = _late_boundary_plan(plan, directory / period)
        degraded = create_training_weight_degradation(
            plan, directory / (period + "-subset")
        )
        shards.extend(
            generate_training_weight_candidate_shard(
                degraded, period + f"-{index}", model
            )
            for index in range(1, 4)
        )
    calibration, application = tuple(shards[:6]), tuple(shards[6:])
    fit = workflow.fit_training_weight_campaign(calibration)
    allocation = workflow.allocate_training_weight_campaign(
        application, fit, calibration
    )
    path = workflow.write_training_weight_allocation(
        allocation, directory / "allocations", application, calibration
    )
    rows = workflow.weighted_training_weight_rows(
        path,
        application,
        calibration,
        "2011-02-01",
        TrainingMemberPolicy.EQUAL,
        members=(WEIGHT_MEMBERS[0],),
    )
    evaluation = workflow.evaluate_training_weight_campaign(
        path, application, calibration
    )
    return (
        directory,
        calibration,
        application,
        fit,
        allocation,
        path,
        rows,
        evaluation,
    )


def test_actual_source_native_fit_allocate_persist_rows_evaluate(
    source_workflow,
):
    directory, calibration, application, fit, allocation, path, rows, report = (
        source_workflow
    )
    assert fit.evidence_kind is WeightEvidenceKind.FIXTURE
    assert fit.calibration.status is WeightSupport.INSUFFICIENT
    assert len(fit.calibration.days) == 1
    assert len(fit.inventory) == 42
    assert sum(d.status == "admitted" for d in fit.inventory) == 1
    assert not any(
        d.status == "unavailable_missing_shard" for d in fit.inventory
    )
    assert len(allocation.inventory) == 20
    assert len(allocation.days) == 1
    day = allocation.days[0]
    assert len(day.policies) == 9
    assert day.policies[-2].status == "confidence_unavailable"
    assert day.policies[-1].status == "failed_negative_control"
    for policy in day.policies:
        if policy.members:
            expected = Fraction(
                4 if policy.policy is TrainingMemberPolicy.NAIVE else 1
            )
            assert sum((m.mass for m in policy.members), Fraction()) == expected
            masses = tuple(
                m.mass / m.complete_core_rows
                for m in policy.members
                for _ in range(m.complete_core_rows)
            )
            assert policy.row_kish_ess == float(
                sum(masses) ** 2 / sum(w * w for w in masses)
            )
    assert path.read_bytes() == allocation.to_json().encode("ascii")
    assert (
        path.name
        == "weight-allocation-"
        + hashlib.sha256(path.read_bytes()).hexdigest()
        + ".json"
    )
    assert rows.rows
    assert sum((r.mass for r in rows.rows), Fraction()) == Fraction(1, 4)
    parent = application[0].degradation.bridges[0].parent_unit
    assert all(r.row.evidence_unit_id == parent.artifact_id for r in rows.rows)
    assert {r.row.origin for r in rows.rows} == {
        TrainingOrigin.OBSERVED,
        TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
    }
    native = {
        e.event_id: e.to_dict()
        for c in application[0].days[0].candidates
        if c.member_id == WEIGHT_MEMBERS[0]
        for e in c.stream.events
    }
    for weighted in rows.rows:
        row = weighted.row
        value = json.loads(row.value_json)
        assert value == native[value["event_id"]]
        assert "vol" not in value
        assert row.available_at_ns is None
        assert (
            row.decision_time_ns
            == _day_ns("2011-02-01") + CORE_END_NS + 30 * SECOND_NS + 1
        )
        assert row.information_mode is TrainingInformationMode.EX_POST
        assert TrainingConsumerMode.CAUSAL not in row.admissible_modes
        assert any(
            r.kind == "research-native-generated-replay"
            and r.verification is TrainingVerificationLevel.DERIVED_REPLAY
            for r in row.verification_roots
        )
    assert len(report.outcomes) == 1
    assert len(report.comparisons) == 7
    assert report.holm_pvalues == (None,) * 7
    assert all(
        c.status
        in (
            "insufficient_common_historical_units",
            "unavailable_no_common_units",
        )
        for c in report.comparisons
    )
    assert report.pre_abstention_coverage.denominator == 0
    assert report.admitted_coverage.denominator == 0
    assert len(report.pre_abstention_coverage.unavailable_unit_ids) == 1
    assert len(report.pre_abstention_coverage.refused_or_missing_dates) == 19
    assert (
        workflow.TrainingWeightEvaluationV1.from_json(report.to_json())
        == report
    )
    assert workflow.TrainingWeightedRowsV1.from_json(rows.to_json()) == rows
    assert len(report.policy_reports) == 9
    equal = report.policy_reports[1]
    one = report.policy_reports[2]
    naive = report.policy_reports[-1]
    assert equal.historical_unit_count == one.historical_unit_count == 1
    assert equal.complete_core_rows == one.complete_core_rows
    assert one.positive_weight_core_rows < one.complete_core_rows
    assert naive.total_mass_numerator == "4"
    assert naive.status == "failed_negative_control_not_default"
    assert report.member_feature_dimensions == (
        workflow.TrainingWeightDimensionV1(
            day.historical_unit_id, day.member_feature_dimension
        ),
    )


def test_fixed_probe_and_piecewise_anchor_math_matches_independent_formula(
    source_workflow,
):
    _, _, application, _, allocation, _, _, _ = source_workflow
    shard = application[0]
    day = shard.days[0]
    bridge = shard.degradation.bridges[0]
    start = _day_ns(bridge.utc_date) + CORE_START_NS
    expected = []
    for candidate in day.candidates:
        events = candidate.stream.events
        anchors = [e for e in events if e.origin.value == "observed"]
        mids = []
        spreads = []
        linear = []
        for second in range(600):
            clock = start + second * SECOND_NS
            prior = [e for e in events if e.event_time_ns <= clock][-1]
            mids.append((prior.bid + prior.ask) / 2)
            spreads.append(prior.ask - prior.bid)
            left = [e for e in anchors if e.event_time_ns <= clock][-1]
            right = next((e for e in anchors if e.event_time_ns > clock), None)
            value = (left.bid + left.ask) / 2
            if clock > left.event_time_ns:
                value += (
                    (clock - left.event_time_ns)
                    / (right.event_time_ns - left.event_time_ns)
                    * ((right.bid + right.ask) / 2 - value)
                )
            linear.append(value)
        value = max(
            WEIGHT_PIP,
            sum(spreads) / 600,
            max(abs(a - b) for a, b in zip(mids, linear)),
        )
        expected.append(value)
    actual = [
        v
        for m in allocation.days[0].scales.member_scales
        for v in m.symbol_scales
    ]
    assert actual == pytest.approx(expected, abs=1e-15)


def test_missing_or_misnamed_allocation_refuses_before_any_source_or_truth(
    tmp_path, monkeypatch
):
    calls = []
    monkeypatch.setattr(workflow, "_truth", lambda *a: calls.append("truth"))
    monkeypatch.setattr(
        workflow, "_campaign", lambda *a: calls.append("source")
    )
    for path in (tmp_path / "missing.json", tmp_path / "forged.json"):
        if path.name == "forged.json":
            path.write_text("{}")
        with pytest.raises((ValueError, OSError)):
            workflow.evaluate_training_weight_campaign(path, (), ())
    assert calls == []


@pytest.mark.parametrize(
    "numerator,denominator",
    [
        ("01", "2"),
        ("1", "0"),
        ("2", "4"),
        ("-1", "2"),
        ("1", "2" * 4097),
        ("١", "2"),
    ],
)
def test_exact_mass_rejects_unbounded_noncanonical_integers(
    numerator, denominator
):
    with pytest.raises(ValueError):
        workflow.TrainingWeightMemberMassV1(
            WEIGHT_MEMBERS[0], numerator, denominator, 1, 1
        )


def test_preflight_wrong_origin_policy_and_unbounded_selection_before_replay(
    monkeypatch,
):
    monkeypatch.setattr(
        workflow,
        "read_training_weight_allocation",
        lambda *a, **k: pytest.fail("must refuse before source replay"),
    )
    for kwargs in (
        {"consumer_mode": TrainingConsumerMode.CAUSAL},
        {"members": ("x",) * 5},
        {"row_keys": tuple(str(i) for i in range(4097))},
        {"training_epoch": 4},
    ):
        with pytest.raises(ValueError):
            workflow.weighted_training_weight_rows(
                "absent",
                (),
                (),
                "2011-02-01",
                TrainingMemberPolicy.ONE,
                **kwargs,
            )


def test_native_probe_staleness_and_equal_timestamp_order(source_workflow):
    _, _, application, _, _, _, _, _ = source_workflow
    candidate = application[0].days[0].candidates[0]
    events = candidate.stream.events
    start = _day_ns("2011-02-01") + CORE_START_NS
    with pytest.raises(ValueError, match="stale"):
        workflow._native_probes((events[0],), start)
    with pytest.raises(ValueError, match="left"):
        workflow._anchor_bridge(
            tuple(e for e in events if e.event_time_ns > start), start
        )
    observed = tuple(e for e in events if e.origin.value == "observed")
    duplicated = (
        observed[0],
        replace(
            observed[0],
            bid=observed[0].bid + 0.00001,
            ask=observed[0].ask + 0.00001,
        ),
        *observed[1:],
    )
    probes, _ = workflow._native_probes(duplicated, start)
    assert probes[0] == workflow._mid(duplicated[1].bid, duplicated[1].ask)


def test_source_scope_duplicate_and_mixed_model_refuse_before_replay(
    source_workflow, monkeypatch
):
    _, calibration, application, _, _, _, _, _ = source_workflow
    monkeypatch.setattr(
        workflow,
        "replay_training_weight_candidate_shard",
        lambda *a, **k: pytest.fail("preflight before replay"),
    )
    with pytest.raises(ValueError, match="repeats"):
        workflow.fit_training_weight_campaign((calibration[0],) * 6)
    with pytest.raises(ValueError, match="scope"):
        workflow.fit_training_weight_campaign(application)
    with pytest.raises(ValueError, match="scope"):
        workflow.fit_training_weight_campaign(calibration * 7)


def test_row_filter_retains_original_mass_with_source_boundary_stub(
    source_workflow, monkeypatch
):
    _, calibration, application, _, allocation, path, full_rows, _ = (
        source_workflow
    )
    # Full real replay is exercised in the integration test. This isolated
    # selection test substitutes only that verified boundary, never row math.
    monkeypatch.setattr(
        workflow, "read_training_weight_allocation", lambda *a, **k: allocation
    )
    keys = tuple(r.row.source_row_key for r in full_rows.rows[:3])
    sliced = workflow.weighted_training_weight_rows(
        path,
        application,
        calibration,
        "2011-02-01",
        TrainingMemberPolicy.EQUAL,
        members=(WEIGHT_MEMBERS[0],),
        row_keys=keys,
    )
    assert sliced.rows == full_rows.rows[:3]
    assert sum((r.mass for r in sliced.rows), Fraction()) < Fraction(1, 4)
    observed = workflow.weighted_training_weight_rows(
        path,
        application,
        calibration,
        "2011-02-01",
        TrainingMemberPolicy.OBSERVED,
    )
    assert sum((r.mass for r in observed.rows), Fraction()) == 1
    assert all(
        r.row.origin is TrainingOrigin.OBSERVED
        and "vol" not in json.loads(r.row.value_json)
        for r in observed.rows
    )
    assert len(observed.rows) < len(full_rows.rows)
    with pytest.raises(ValueError, match="outside"):
        workflow.weighted_training_weight_rows(
            path,
            application,
            calibration,
            "2011-02-01",
            TrainingMemberPolicy.OBSERVED,
            row_keys=("invented",),
        )
    with pytest.raises(ValueError, match="unsupported"):
        workflow.weighted_training_weight_rows(
            path,
            application,
            calibration,
            "2011-02-01",
            TrainingMemberPolicy.UNCERTAINTY,
        )


def test_closed_multi_policy_export_replays_once_not_per_slice(
    source_workflow, monkeypatch
):
    _, calibration, application, _, allocation, path, _, _ = source_workflow
    calls = []

    def boundary(*args, **kwargs):
        calls.append("full-campaign-replay")
        return allocation

    monkeypatch.setattr(workflow, "read_training_weight_allocation", boundary)
    requests = (
        workflow.TrainingWeightRowRequestV1(
            "2011-02-01", TrainingMemberPolicy.OBSERVED
        ),
        workflow.TrainingWeightRowRequestV1(
            "2011-02-01",
            TrainingMemberPolicy.EQUAL,
            members=(WEIGHT_MEMBERS[0],),
        ),
        workflow.TrainingWeightRowRequestV1(
            "2011-02-01", TrainingMemberPolicy.ONE, members=(WEIGHT_MEMBERS[1],)
        ),
        workflow.TrainingWeightRowRequestV1(
            "2011-02-01",
            TrainingMemberPolicy.MARGINALIZED,
            members=(WEIGHT_MEMBERS[2],),
        ),
        workflow.TrainingWeightRowRequestV1(
            "2011-02-01",
            TrainingMemberPolicy.NAIVE,
            members=(WEIGHT_MEMBERS[3],),
        ),
    )
    batches = list(
        workflow.iter_weighted_training_weight_rows(
            path, application, calibration, requests
        )
    )
    assert calls == ["full-campaign-replay"]
    assert len(batches) == 5
    assert all(
        len(b.rows) <= 4096 and len(b.to_json()) <= 8 * 1024 * 1024
        for b in batches
    )
    assert sum((r.mass for r in batches[-1].rows), Fraction()) == 1
    with pytest.raises(ValueError, match="count"):
        list(
            workflow.iter_weighted_training_weight_rows(
                path, application, calibration, requests * 205
            )
        )
    assert calls == ["full-campaign-replay"]


def test_large_native_member_streams_node_bounded_chunks_without_replay(
    source_workflow, monkeypatch
):
    """Export-shape boundary only; synthetic additions are not source proof."""
    _, calibration, application, fit, allocation, path, _, _ = source_workflow
    shard = application[0]
    original = shard.days[0]
    candidate = original.candidates[0]
    stream = candidate.stream
    template = next(e for e in stream.events if e.origin.value == "synthetic")
    extras = tuple(
        replace(
            template,
            event_time_ns=template.event_time_ns + i + 1,
            event_sequence=10000 + i,
            event_id="",
        )
        for i in range(1200)
    )
    expanded = replace(stream, events=(*stream.events, *extras), stream_id="")
    candidate = replace(
        candidate, stream_encoding_json=_encode_stream(dict(expanded.to_dict()))
    )
    day = replace(original, candidates=(candidate, *original.candidates[1:]))
    changed_shard = replace(shard, days=(day,))
    applications = (changed_shard, *application[1:])
    bridge = next(
        b for b in shard.degradation.bridges if b.artifact_id == day.bridge_id
    )
    inventory = {
        d.utc_date: (
            replace(d, candidate_day_id=day.artifact_id)
            if d.utc_date == bridge.utc_date
            else d
        )
        for d in allocation.inventory
    }
    larger = workflow._allocate(
        applications,
        {bridge.utc_date: (changed_shard, day, bridge)},
        inventory,
        fit,
    )
    calls = []

    def boundary(*args, **kwargs):
        calls.append("verified-once")
        return larger

    monkeypatch.setattr(workflow, "read_training_weight_allocation", boundary)
    request = workflow.TrainingWeightRowRequestV1(
        "2011-02-01", TrainingMemberPolicy.EQUAL, members=(WEIGHT_MEMBERS[0],)
    )
    batches = list(
        workflow.iter_weighted_training_weight_rows(
            path, applications, calibration, (request,)
        )
    )
    assert calls == ["verified-once"]
    assert len(batches) > 1
    rows = tuple(r for b in batches for r in b.rows)
    assert (
        len({r.row.source_row_key for r in rows})
        == len(rows)
        == larger.days[0].policies[1].members[0].complete_core_rows
    )
    assert sum((r.mass for r in rows), Fraction()) == Fraction(1, 4)
    for batch in batches:
        assert workflow._expanded_nodes(batch.to_dict()) <= 131072
        assert len(batch.to_json()) <= 8 * 1024 * 1024
        assert (
            workflow.TrainingWeightedRowsV1.from_json(batch.to_json()) == batch
        )
    with pytest.raises(ValueError, match="expanded"):
        replace(batches[0], rows=rows)


def test_entire_shard_omission_cannot_authorize_supported_fit_or_comparison(
    source_workflow, monkeypatch
):
    _, calibration, application, fit, allocation, path, _, _ = source_workflow
    monkeypatch.setattr(
        workflow,
        "replay_training_weight_candidate_shard",
        lambda *a, **k: pytest.fail("omission refused before replay"),
    )
    for missing in (calibration[:4], calibration[:-1], application[:2]):
        role = "calibration" if len(missing) > 2 else "application"
        with pytest.raises(ValueError, match="complete"):
            workflow._campaign(missing, role, None)
    with pytest.raises(ValueError, match="complete"):
        replace(fit, shards=fit.shards[:-1])
    with pytest.raises(ValueError, match="complete"):
        replace(allocation, shards=allocation.shards[:-1])


def test_aggregate_kish_recomputed_from_all_row_masses_not_summed_day_ess(
    source_workflow,
):
    allocation = source_workflow[4]
    original = allocation.days[0]
    unit = "fixture:second-independent-unit-not-source-evidence"
    scales = replace(
        original.scales, historical_unit_id=unit, utc_date="2011-02-02"
    )
    reference = (*original.policies[0].members, *original.policies[1].members)
    counts = {m.member_id: m.complete_core_rows * 2 for m in reference}
    support = {m.member_id: m.complete_support_rows * 2 for m in reference}
    radii = replace(original.radii, scales=scales)
    policies = tuple(
        workflow._policy_allocation(p, e, unit, counts, support, radii)
        for p, e in workflow._POLICIES
    )
    second = replace(
        original,
        utc_date="2011-02-02",
        historical_unit_id=unit,
        candidate_day_id="fixture:second-candidate-day",
        scales=scales,
        radii=radii,
        policies=policies,
    )
    inventory = tuple(
        (
            replace(
                d,
                status="admitted",
                historical_unit_id=unit,
                candidate_day_id=second.candidate_day_id,
            )
            if d.utc_date == "2011-02-02"
            else d
        )
        for d in allocation.inventory
    )
    # This is a pure accounting fixture, not accepted as source replay.
    hypothetical = replace(
        allocation,
        days=(original, second),
        inventory=inventory,
        shards=(
            replace(
                allocation.shards[0],
                candidate_day_ids=(
                    *allocation.shards[0].candidate_day_ids,
                    second.candidate_day_id,
                ),
                refusal_records=tuple(
                    r
                    for r in allocation.shards[0].refusal_records
                    if json.loads(r)["utc_date"] != "2011-02-02"
                ),
            ),
            *allocation.shards[1:],
        ),
    )
    reports = workflow._policy_reports(hypothetical)
    equal = reports[1]
    weights = [
        m.mass / m.complete_core_rows
        for day in hypothetical.days
        for m in day.policies[1].members
        for _ in range(m.complete_core_rows)
    ]
    assert equal.aggregate_row_kish_ess == float(
        sum(weights) ** 2 / sum(w * w for w in weights)
    )
    assert equal.aggregate_row_kish_ess != sum(
        d.policies[1].row_kish_ess for d in hypothetical.days
    )
    assert equal.historical_unit_count == 2
    assert reports[-1].total_mass_numerator == "8"


def test_changed_source_model_bytes_refuse_fresh_allocation_read(
    source_workflow,
):
    _, calibration, application, _, _, path, _, _ = source_workflow
    source = calibration[0].model.source_files[0]
    from pathlib import Path

    file = Path(source.path)
    original = file.read_bytes()
    try:
        file.write_bytes(original + b"changed-fixture")
        with pytest.raises(ValueError, match="bound|retained|bytes"):
            workflow.read_training_weight_allocation(
                path, application, calibration
            )
    finally:
        file.write_bytes(original)


def test_truth_is_loaded_once_per_original_month_not_member_row_or_shard(
    source_workflow, monkeypatch
):
    calibration = source_workflow[1]
    original = workflow.verify_training_source
    calls = []

    def verify(source):
        calls.append(source.dataset_version_id)
        return original(source)

    monkeypatch.setattr(workflow, "verify_training_source", verify)
    truth = workflow._truth(calibration)
    assert len(calls) == len(set(calls)) == 2
    assert len(truth) == 3
    assert all(len(values) == 600 for values in truth.values())


def test_available_scales_never_call_truth_computation(
    source_workflow, monkeypatch
):
    _, _, application, _, allocation, _, _, _ = source_workflow
    monkeypatch.setattr(
        workflow,
        "_truth",
        lambda *a: pytest.fail(
            "available scale cannot inspect application outcome"
        ),
    )
    assert (
        workflow.source_bound_training_weight_scales(
            application[0], "2011-02-01"
        )
        == allocation.days[0].scales
    )


def test_resealed_allocation_tamper_refuses_before_application_truth(
    source_workflow, tmp_path, monkeypatch
):
    _, calibration, application, fit, allocation, _, _, _ = source_workflow
    # Different valid diagnostic values keep all structural mass constraints.
    day = replace(allocation.days[0], member_feature_dimension=2.0)
    assert day != allocation.days[0]
    altered = replace(allocation, days=(day,))
    payload = altered.to_json().encode("ascii")
    path = tmp_path / (
        "weight-allocation-" + hashlib.sha256(payload).hexdigest() + ".json"
    )
    path.write_bytes(payload)
    assert workflow._read_allocation(path) == altered
    monkeypatch.setattr(
        workflow, "fit_training_weight_campaign", lambda *a, **k: fit
    )
    original_campaign = workflow._campaign
    original_allocate = workflow._allocate
    monkeypatch.setattr(
        workflow, "_campaign", lambda *a, **k: (application, {}, {})
    )
    monkeypatch.setattr(workflow, "_allocate", lambda *a, **k: allocation)
    monkeypatch.setattr(
        workflow,
        "_truth",
        lambda *a: pytest.fail(
            "allocation mismatch must precede outcome access"
        ),
    )
    with pytest.raises(ValueError, match="before outcome"):
        workflow.evaluate_training_weight_campaign(
            path, application, calibration
        )
    assert original_campaign and original_allocate


def test_report_resealed_false_coverage_or_summary_refuses(source_workflow):
    report = source_workflow[-1]
    with pytest.raises(ValueError, match="inference"):
        replace(report, holm_pvalues=(0.0,) * 7)
    coverage = replace(
        report.pre_abstention_coverage,
        evaluated_unit_ids=report.pre_abstention_coverage.unavailable_unit_ids,
        covered_unit_ids=report.pre_abstention_coverage.unavailable_unit_ids,
        unavailable_unit_ids=(),
    )
    with pytest.raises(ValueError, match="coverage"):
        replace(report, pre_abstention_coverage=coverage)


def test_persistence_reuses_exact_bytes_and_preserves_conflicts(
    source_workflow, tmp_path, monkeypatch
):
    _, calibration, application, _, allocation, _, _, _ = source_workflow
    monkeypatch.setattr(
        workflow,
        "allocate_training_weight_campaign",
        lambda *a, **k: allocation,
    )
    path = workflow.write_training_weight_allocation(
        allocation, tmp_path, application, calibration
    )
    assert (
        workflow.write_training_weight_allocation(
            allocation, tmp_path, application, calibration
        )
        == path
    )
    path.write_bytes(b"conflict")
    with pytest.raises(ValueError, match="existing"):
        workflow.write_training_weight_allocation(
            allocation, tmp_path, application, calibration
        )
    assert path.read_bytes() == b"conflict"


def test_numeric_point_loss_extremes_and_outcome_bounds():
    assert workflow._point_loss((1.0,) * 600, (1.0,) * 600) == 0
    assert workflow._mid(1e308, 1e308) == 1e308
    for pair in ((0.0, 1.0), (2.0, 1.0), (1.0, math.inf)):
        with pytest.raises(ValueError):
            workflow._mid(*pair)
    with pytest.raises(ValueError):
        workflow._point_loss((1.0,), (1.0,))


def test_expanded_budget_counts_embedded_native_json_not_only_wire_strings():
    native = training_json({"array": [0] * 4096})
    # The external wire tree has only dozens of string nodes, but the frozen
    # bound includes the semantic native JSON carried inside those strings.
    assert workflow._expanded_nodes([{"value_json": native}] * 31) <= 131072
    with pytest.raises(ValueError, match="expanded"):
        workflow._expanded_nodes([{"value_json": native}] * 32)
