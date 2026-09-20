"""New synthetic IPC/rows only: no old source, index, or attempt is opened."""

import hashlib
import json
from dataclasses import replace

import polars as pl
import pytest

from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.data_quality.training_contracts import (
    TrainingSourceV1,
    training_json,
)
from histdatacom.data_quality.training_lineage import (
    _ownership,
    verify_training_source,
)
from histdatacom.data_quality.training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticEvidenceKind,
    read_training_weight_diagnostic_protocol,
)
from histdatacom.data_quality.training_weight_lineage import (
    CORE_START_NS,
    SECOND_NS,
    WEIGHT_SYMBOLS,
    TrainingWeightSourcePlanV1,
    WeightEvidenceKind,
    _create_training_weight_degradation_from_verified,
    _day_ns,
    _probe_rows,
    _selected_days,
    create_training_weight_degradation,
)
import histdatacom.data_quality.training_weight_source_diagnostics as diagnostics
from tests.fixtures.training_weight_sources import fixture_weight_source


def _verified(plan):
    source = verify_training_source(plan.parent_source)
    return plan, source, _ownership(source)


@pytest.fixture(scope="module")
def baseline(tmp_path_factory):
    plan, _ = fixture_weight_source(
        tmp_path_factory.mktemp("source-diagnostic-baseline"),
        dates=("2010-01-04",),
        duplicate=True,
    )
    return _verified(plan)


def _report(inputs, **kwargs):
    return diagnostics.diagnose_verified_training_weight_source(
        *inputs, **kwargs
    )


def _first(report, symbol="EURGBP"):
    return next(
        c
        for c in report.cells
        if c.utc_date == "2010-01-04" and c.symbol == symbol
    )


def _synthetic_source(tmp_path, seconds_by_symbol, *, invalid=()):
    """Build actual freshly qualified IPC bytes with explicit synthetic times."""
    period = "201001"
    start = _day_ns("2010-01-04") + CORE_START_NS
    root = tmp_path / "ASCII" / "T"
    for symbol in WEIGHT_SYMBOLS:
        seconds = seconds_by_symbol[symbol]
        clocks = [start + int(s * SECOND_NS) for s in seconds]
        bids = [1.2345] * len(clocks)
        asks = [1.2346] * len(clocks)
        for target_symbol, index in invalid:
            if symbol == target_symbol:
                bids[index] = 1.2347
        frame = pl.DataFrame(
            {
                "datetime": [t // 1_000_000 for t in clocks],
                "bid": bids,
                "ask": asks,
                "vol": [17] * len(clocks),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        )
        path = histdata_cache_path(root, symbol, period)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_ipc(path)
    evidence = tmp_path / "new-synthetic-evidence.json"
    evidence.write_text('{"synthetic":true}', encoding="ascii")
    adapter = HistDataProviderAdapter()
    descriptor = DatasetDescriptorV1(
        "training-weight-contract-fixture",
        "Synthetic diagnostic fixture",
        "New synthetic timestamps; no empirical evidence.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=WEIGHT_SYMBOLS,
        periods=(period,),
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="synthetic-diagnostic-test"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    source = TrainingSourceV1(catalog.to_json(), version.dataset_version_id)
    return _verified(
        TrainingWeightSourcePlanV1(
            source, "calibration", WeightEvidenceKind.FIXTURE
        )
    )


def _oracle(rows, start):
    selected = []
    ages = []
    statuses = []
    for offset in range(600):
        prior = [
            r for r in rows if r.event_time_ns <= start + offset * SECOND_NS
        ]
        row = prior[-1] if prior else None
        selected.append(row)
        age = (
            None
            if row is None
            else start + offset * SECOND_NS - row.event_time_ns
        )
        ages.append(age)
        statuses.append(
            "missing_prior"
            if age is None
            else "stale" if age > 60 * SECOND_NS else "success"
        )
    return selected, ages, statuses


def test_complete_186_cells_and_nine_source_shards_from_new_tiny_ipcs(tmp_path):
    all_cells = []
    all_days = []
    shards = []
    for period, day, expected in (
        ("201001", "2010-01-04", 63),
        ("201101", "2011-01-04", 63),
        ("201102", "2011-02-01", 60),
    ):
        plan, _ = fixture_weight_source(
            tmp_path / period, dates=(day,), period=period
        )
        report = _report(_verified(plan))
        assert (
            report.evidence_kind is TrainingWeightDiagnosticEvidenceKind.FIXTURE
        )
        assert (
            report.protocol_id
            == read_training_weight_diagnostic_protocol().artifact_id
        )
        assert len(report.cells) == expected
        assert sum(d.original_admitted for d in report.days) == 1
        split = diagnostics.split_training_weight_source_diagnostics(report)
        assert tuple(len(s.days) for s in split) == (8, 8, expected // 3 - 16)
        assert all(len(s.cells) <= 24 for s in split)
        restored = tuple(
            diagnostics.TrainingWeightSourceDiagnosticShardV1.from_json(
                s.to_json()
            )
            for s in split
        )
        assert (
            diagnostics.verify_training_weight_source_diagnostic_shards(
                restored
            )
            == report
        )
        assert (
            diagnostics.TrainingWeightSourceDiagnosticsV1.from_json(
                report.to_json()
            )
            == report
        )
        all_cells.extend(report.cells)
        all_days.extend(report.days)
        shards.extend(split)
    assert len(all_cells) == 186 and len(all_days) == 62 and len(shards) == 9
    assert (
        tuple(d.utc_date for d in all_days)
        == read_training_weight_diagnostic_protocol().scheduled_dates()
    )
    assert len({(c.utc_date, c.symbol) for c in all_cells}) == 186


def test_exact_duplicate_ordinal_map_and_independent_probe_oracle(baseline):
    plan, source, owner = baseline
    report = _report(baseline)
    cell = _first(report)
    support = tuple(
        r
        for r in source.observed
        if r.symbol == "EURGBP"
        and cell.core_start_ns <= r.event_time_ns <= cell.core_end_ns
    )[1:]
    subset = (support[0], *support[1:-1:4], support[-1])
    assert cell.left_parent_row_id == 3
    assert cell.duplicate_time_count == 1
    assert cell.classification == "both_passed"
    assert cell.retained_row_count == 32
    assert cell.hidden_row_count == len(support) - len(subset)
    mapping = {
        "mapping": "cell-local-subset-ordinal.v1",
        "source_id": plan.parent_source.artifact_id,
        "source_series_id": support[0].series_id,
        "period": plan.period,
        "utc_date": cell.utc_date,
        "ordinals": [
            [i, r.row_id, r.event_time_ns] for i, r in enumerate(subset, 1)
        ],
    }
    assert (
        cell.cell_local_ordinal_map_sha256
        == hashlib.sha256(training_json(mapping).encode("ascii")).hexdigest()
    )
    for rows, actual in (
        (support, cell.parent_probes),
        (subset, cell.subset_probes),
    ):
        _, ages, statuses = _oracle(rows, cell.core_start_ns)
        assert actual.success_count == statuses.count("success") == 600
        assert actual.maximum_defined_age_ns == max(
            a for a in ages if a is not None
        )
        assert actual.earliest_failure is None
        assert _probe_rows(rows, cell.core_start_ns)
    assert (
        diagnostics.replay_verified_training_weight_source_diagnostics(
            report, plan, source, owner
        )
        == report
    )


@pytest.mark.parametrize(
    ("seconds", "classification", "old_reason"),
    [
        (
            tuple(range(60)) + tuple(range(100, 600, 40)) + (600,),
            "subset_only_failed",
            "missing_or_stale_fixed_probe",
        ),
        (
            tuple(range(64)) + (200, 400, 600),
            "parent_failed",
            "missing_or_stale_fixed_probe",
        ),
        ((0, 600), "parent_failed", "fewer_than_64_core_parent_rows"),
        (
            tuple(range(1, 602, 5)),
            "not_evaluable",
            "missing_boundary_anchor_within_fixed_support",
        ),
    ],
)
def test_parent_subset_failures_are_independent_of_original_short_circuit(
    tmp_path, seconds, classification, old_reason
):
    inputs = _synthetic_source(tmp_path, {s: seconds for s in WEIGHT_SYMBOLS})
    report = _report(inputs)
    cell = _first(report)
    assert cell.classification == classification
    day = next(d for d in report.days if d.utc_date == cell.utc_date)
    assert day.original_reason == old_reason
    assert day.first_failing_symbol == "EURGBP"
    assert len(report.cells) == 63
    # Later symbols are evaluated, not hidden behind EURGBP's refusal.
    assert _first(report, "GBPUSD").classification == classification
    if classification == "not_evaluable":
        assert cell.parent_probes.maximum_defined_age_ns is None
        assert cell.parent_probes.success_count == 0
        assert cell.cell_local_ordinal_map_sha256 is None
    else:
        raw = tuple(r for r in inputs[1].observed if r.symbol == "EURGBP")
        subset = (raw[0], *raw[1:-1:4], raw[-1])
        for rows, probes in (
            (raw, cell.parent_probes),
            (subset, cell.subset_probes),
        ):
            selected, ages, statuses = _oracle(rows, cell.core_start_ns)
            assert probes.success_count == statuses.count("success")
            assert probes.stale_count == statuses.count("stale")
            first = next(
                (i for i, v in enumerate(statuses) if v != "success"), None
            )
            if first is None:
                assert probes.earliest_failure is None
            else:
                witness = probes.earliest_failure
                assert witness.probe_offset == first
                assert witness.selected_parent_row_id == selected[first].row_id
                assert witness.age_ns == ages[first]


def test_all_gate_counts_continue_after_other_symbol_and_budget_failure(
    tmp_path,
):
    values = {
        "EURGBP": tuple(range(1, 602, 5)),
        "EURUSD": tuple(range(0, 601, 5)),
        "GBPUSD": (0,) + (100,) * 4100 + (600,),
    }
    report = _report(
        _synthetic_source(
            tmp_path, values, invalid=(("EURUSD", 10), ("GBPUSD", 4098))
        )
    )
    budget = _first(report, "GBPUSD")
    assert budget.window_row_count == 4102
    assert budget.core_row_count == 4101
    assert budget.duplicate_time_count == 4099
    assert budget.invalid_window_quote_count == 1
    assert budget.count_status == "complete_exact"
    assert budget.selected_support_row_count is None
    assert (
        budget.parent_probes.status
        == budget.subset_probes.status
        == "not_evaluable"
    )
    assert budget.left_parent_row_id == 1 and budget.right_parent_row_id == 4102
    invalid = _first(report, "EURUSD")
    assert invalid.invalid_support_quote_count == 1
    assert (
        dict((g.name, g.status) for g in invalid.gates)["valid_support_quotes"]
        == "failed"
    )
    assert invalid.parent_probes.status == "not_evaluable"
    first = next(d for d in report.days if d.utc_date == budget.utc_date)
    assert first.original_reason == "source_support_exceeds_frozen_row_budget"
    assert first.first_failing_symbol is None  # Old day-global precedence.


def test_invalid_unused_halo_is_counted_but_does_not_change_old_gate(tmp_path):
    values = {s: (-30,) + tuple(range(0, 601, 5)) for s in WEIGHT_SYMBOLS}
    report = _report(
        _synthetic_source(tmp_path, values, invalid=(("EURGBP", 0),))
    )
    cell = _first(report)
    assert cell.invalid_window_quote_count == 1
    assert cell.invalid_support_quote_count == 0
    assert cell.left_distance_ns == 0
    assert cell.classification == "both_passed"
    assert next(
        d for d in report.days if d.utc_date == cell.utc_date
    ).original_admitted


@pytest.mark.parametrize("crossed_core", (False, True))
def test_projection_metadata_does_not_relabel_or_change_raw_quote_census(
    tmp_path, crossed_core
):
    # Enough crossed rows outside the diagnostic support trigger the existing
    # adapter's >=25% metadata flag, without changing any returned raw prices.
    values = {s: (-200,) * 64 + tuple(range(0, 601, 5)) for s in WEIGHT_SYMBOLS}
    invalid = tuple(("EURGBP", i) for i in range(64))
    if crossed_core:
        invalid += (("EURGBP", 74),)
    inputs = _synthetic_source(tmp_path, values, invalid=invalid)
    raw = tuple(r for r in inputs[1].observed if r.symbol == "EURGBP")
    assert all(r.quote_projection for r in raw)
    assert raw[0].bid > raw[0].ask  # Reader did not project crossed quotes.
    assert raw[64].bid < raw[64].ask  # Valid row still carries partition flag.
    report = _report(inputs)
    cell = _first(report)
    assert cell.invalid_window_quote_count == int(crossed_core)
    assert cell.invalid_support_quote_count == int(crossed_core)
    day = next(d for d in report.days if d.utc_date == cell.utc_date)
    if crossed_core:
        assert (
            day.original_reason
            == "nonpositive_nonfinite_or_crossed_source_quotes"
        )
        assert cell.parent_probes.status == "not_evaluable"
    else:
        assert day.original_admitted
        assert cell.classification == "both_passed"
    assert _selected_days(inputs[0], inputs[1])[0].keys() == {
        d.utc_date for d in report.days if d.original_admitted
    }


def test_allowed_physical_timestamp_regression_keeps_exact_ordinal_identity(
    tmp_path,
):
    seconds = [-1, *range(0, 601, 5)]
    seconds.insert(4, seconds[3] - 0.001)
    inputs = _synthetic_source(
        tmp_path, {s: tuple(seconds) for s in WEIGHT_SYMBOLS}
    )
    raw = tuple(r for r in inputs[1].observed if r.symbol == "EURGBP")
    assert tuple(r.row_id for r in raw[:8]) == (1, 2, 3, 5, 4, 6, 7, 8)
    selected, refused = _selected_days(inputs[0], inputs[1])
    assert "2010-01-04" in selected
    report = _report(inputs)
    day = next(d for d in report.days if d.utc_date == "2010-01-04")
    assert day.original_admitted and day.original_reason is None
    assert tuple(
        d.utc_date for d in report.days if not d.original_admitted
    ) == tuple(r.utc_date for r in refused)
    subset = selected[day.utc_date]["EURGBP"]
    expected_map = {
        "mapping": "cell-local-subset-ordinal.v1",
        "source_id": inputs[0].parent_source.artifact_id,
        "source_series_id": subset[0].series_id,
        "period": "201001",
        "utc_date": day.utc_date,
        "ordinals": [
            [i, r.row_id, r.event_time_ns] for i, r in enumerate(subset, 1)
        ],
    }
    assert (
        _first(report).cell_local_ordinal_map_sha256
        == hashlib.sha256(
            training_json(expected_map).encode("ascii")
        ).hexdigest()
    )


def test_inclusive_sixty_second_boundary_and_missing_prior_witness(baseline):
    original = baseline[1].observed[0]
    start = _day_ns("2010-01-04") + CORE_START_NS

    def rows(seconds):
        return tuple(
            replace(original, row_id=i, event_time_ns=start + s * SECOND_NS)
            for i, s in enumerate(seconds, 1)
        )

    exact = diagnostics._probes(rows(range(0, 601, 61)), start)
    assert exact.success_count == 600
    assert exact.maximum_defined_age_ns == 60 * SECOND_NS
    stale = diagnostics._probes(rows(range(0, 601, 62)), start)
    assert stale.earliest_failure.probe_offset == 61
    assert stale.earliest_failure.age_ns == 61 * SECOND_NS
    missing = diagnostics._probes(rows((1, 600)), start)
    assert missing.missing_prior_count == 1
    assert missing.earliest_failure.status == "missing_prior"
    assert missing.earliest_failure.selected_parent_row_id is None
    assert missing.earliest_failure.age_ns is None


def test_pure_api_performs_no_reads_and_progress_precedes_cell_work(
    baseline, monkeypatch
):
    import histdatacom.data_quality.training_lineage as source_module

    def forbidden(*args, **kwargs):
        pytest.fail("pure diagnostics cannot open a source or bypass approval")

    monkeypatch.setattr(source_module, "verify_training_source", forbidden)
    monkeypatch.setattr(source_module, "read_training_regular", forbidden)
    announced = []
    original = diagnostics._cell

    def checked(*args):
        assert announced[-1] == (args[1], args[2])
        return original(*args)

    monkeypatch.setattr(diagnostics, "_cell", checked)
    report = _report(
        baseline, on_cell=lambda day, symbol: announced.append((day, symbol))
    )
    assert announced == [(c.utc_date, c.symbol) for c in report.cells]

    def failed_progress(*args):
        raise OSError("synthetic checkpoint failure")

    with pytest.raises(OSError, match="checkpoint"):
        _report(baseline, on_cell=failed_progress)


@pytest.mark.parametrize(
    "mutation",
    (
        "missing_row",
        "repeated_ordinal",
        "unsorted",
        "wrong_series",
        "wrong_ownership",
        "wrong_protocol",
    ),
)
def test_process_local_source_identity_and_physical_order_refuse(
    baseline, mutation
):
    plan, source, owner = baseline
    rows = list(source.observed)
    kwargs = {}
    if mutation == "missing_row":
        rows.pop()
    elif mutation == "repeated_ordinal":
        rows[1] = replace(rows[1], row_id=rows[0].row_id)
    elif mutation == "unsorted":
        rows[0], rows[1] = rows[1], rows[0]
    elif mutation == "wrong_series":
        rows[0] = replace(rows[0], series_id="not-source-series")
    elif mutation == "wrong_ownership":
        owner = replace(owner, units=owner.units[1:])
    elif mutation == "wrong_protocol":
        kwargs["protocol_id"] = "unbound-protocol"
    with pytest.raises(ValueError):
        _report((plan, replace(source, observed=tuple(rows)), owner), **kwargs)


def test_reports_have_no_hidden_prices_and_resealed_false_counts_fail_replay(
    baseline,
):
    report = _report(baseline)
    wire = json.loads(report.to_json())

    def all_keys(value):
        if isinstance(value, dict):
            return set(value).union(*(all_keys(v) for v in value.values()))
        if isinstance(value, list):
            return set().union(*(all_keys(v) for v in value))
        return set()

    assert not {
        "bid",
        "ask",
        "vol",
        "mid",
        "price",
        "loss",
        "error",
    } & all_keys(wire)
    assert "1.234" not in report.to_json()
    cell = _first(report)
    index = report.cells.index(cell)
    day_index = index // 3
    # Legal aggregate count but false source evidence: a fresh valid envelope
    # cannot substitute for replay against the exact same verified inputs.
    changed = replace(cell, invalid_window_quote_count=1)
    first_day = replace(
        report.days[day_index],
        cell_ids=(changed.artifact_id, *report.days[day_index].cell_ids[1:]),
    )
    forged = replace(
        report,
        cells=(*report.cells[:index], changed, *report.cells[index + 1 :]),
        days=(
            *report.days[:day_index],
            first_day,
            *report.days[day_index + 1 :],
        ),
    )
    assert (
        diagnostics.TrainingWeightSourceDiagnosticsV1.from_json(
            forged.to_json()
        )
        == forged
    )
    with pytest.raises(ValueError, match="exact source replay"):
        diagnostics.replay_verified_training_weight_source_diagnostics(
            forged, *baseline
        )


def test_shard_reassembly_rejects_omission_reorder_and_provenance_substitution(
    baseline,
):
    report = _report(baseline)
    shards = diagnostics.split_training_weight_source_diagnostics(report)
    for invalid in (
        shards[:2],
        shards[::-1],
        (shards[0], shards[0], shards[2]),
    ):
        with pytest.raises(ValueError):
            diagnostics.verify_training_weight_source_diagnostic_shards(invalid)
    with pytest.raises(ValueError, match="report identity"):
        diagnostics.verify_training_weight_source_diagnostic_shards(
            (replace(shards[0], ownership_id="different-owner"), *shards[1:])
        )
    with pytest.raises(ValueError, match="scope"):
        replace(shards[0], cells=shards[0].cells[:-1])
    raw = shards[0].to_dict()
    raw["future_unknown"] = True
    with pytest.raises(ValueError, match="unknown"):
        diagnostics.TrainingWeightSourceDiagnosticShardV1.from_json(
            training_json(raw)
        )


def test_old_public_degradation_and_verified_extraction_have_same_exact_selection(
    tmp_path, baseline
):
    plan, source, _ = baseline
    direct = _create_training_weight_degradation_from_verified(
        plan, source, tmp_path / "direct"
    )
    public = create_training_weight_degradation(plan, tmp_path / "public")
    selected, refusals = _selected_days(plan, source)
    assert direct.refusals == public.refusals == refusals
    assert direct.parent_ownership == public.parent_ownership
    assert tuple(b.utc_date for b in direct.bridges) == tuple(selected)
    for left, right in zip(direct.bridges, public.bridges):
        assert left.parent_unit == right.parent_unit
        for a, b in zip(left.symbols, right.symbols):
            assert a.ordinals == b.ordinals
    with pytest.raises(ValueError, match="exact verified source"):
        _create_training_weight_degradation_from_verified(
            plan, replace(source, source=direct.subset_source), tmp_path / "bad"
        )
