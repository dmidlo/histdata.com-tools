"""Newly generated native candidate fixtures, never empirical qualification."""

import json
import multiprocessing
import os
import shutil
import time
from dataclasses import replace

import pytest
import histdatacom.data_quality.training_weight_candidates as candidates

from histdatacom.data_quality.training_weight_candidates import (
    TrainingWeightCandidateDayV1,
    generate_training_weight_candidate_day,
    verify_training_weight_model,
    generate_training_weight_candidate_shard,
    CandidateRefusalCode,
    TrainingWeightDeterministicRefusal,
    TrainingWeightOperationalFailure,
)
from histdatacom.data_quality.training_weight_lineage import (
    create_training_weight_degradation,
)
from histdatacom.data_quality.training_contracts import training_load
from histdatacom.data_quality.training_weight_contracts import (
    read_training_weight_preregistration,
)
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)


def test_positive_exact_generator_replay_retains_all_native_ancestry(tmp_path):
    plan, _ = fixture_weight_source(tmp_path / "source", dates=("2010-01-04",))
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")
    day = generate_training_weight_candidate_day(degraded, "2010-01-04", model)
    assert len(day.candidates) == 12
    assert TrainingWeightCandidateDayV1.from_json(day.to_json()) == day
    assert (
        generate_training_weight_candidate_day(degraded, "2010-01-04", model)
        == day
    )
    assert all(c.stream.synthetic_event_count > 0 for c in day.candidates)
    observed_ids = set()
    for candidate in day.candidates:
        assert candidate.status == "research_candidate_not_production_qualified"
        stream = candidate.stream
        observed_ids.update(
            e.event_id for e in stream.events if e.origin.value == "observed"
        )
        records = tuple(
            training_load(text) for text in candidate.interval_records
        )
        assert all(
            r["information_mode"]
            == "ex_post_whole_retained_window_not_right_anchor_availability"
            for r in records
        )
        transforms = [t.to_dict() for t in candidate.native_transformations]
        assert transforms
        assert all(
            type(t["seed"]) is int and 0 <= t["seed"] < 2**64
            for t in transforms
        )
        assert all(
            r["candidate_metadata"]["hard_carving_status"] == "not_evaluated"
            for r in records
        )
    assert observed_ids
    raw = day.to_dict()
    stream = json.loads(raw["candidates"][0]["stream_encoding_json"])
    stream["events"]["constants"]["later_hidden_truth"] = 1.0
    raw["candidates"][0]["stream_encoding_json"] = json.dumps(
        stream, sort_keys=True, separators=(",", ":")
    )
    with pytest.raises(ValueError, match="unknown|identity|differ"):
        TrainingWeightCandidateDayV1.from_dict(raw)


def test_model_bytes_are_reverified_not_authorized_by_a_caller_id(tmp_path):
    model = fixture_weight_model(tmp_path / "model")
    assert verify_training_weight_model(model).index_id == model.index.index_id
    source = model.source_files[0]
    with open(source.path, "ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(ValueError):
        verify_training_weight_model(model)
    with pytest.raises(ValueError):
        replace(model, adapter_source_fingerprint="not-a-hash")


def test_zero_width_geometry_preserves_both_ordinals_without_generation(
    tmp_path, monkeypatch
):
    plan, _ = fixture_weight_source(
        tmp_path / "source", dates=("2010-01-04",), interior_duplicates=8
    )
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")
    bridge = degraded.bridges[0]
    mapping = bridge.symbols[0]
    original = candidates.generate_empirical_motif_candidates
    attempted = []

    def generator(**kwargs):
        left, right = kwargs["left_anchor"], kwargs["right_anchor"]
        assert left.event_time_ns < right.event_time_ns
        attempted.append((left.event_id, right.event_id))
        return original(**kwargs)

    monkeypatch.setattr(
        candidates, "generate_empirical_motif_candidates", generator
    )
    result = candidates._generate_member_symbol(
        bridge,
        model,
        model.index,
        "member-60701",
        mapping.symbol,
        {},
        candidates._mapping_condition(mapping),
        float("inf"),
    )
    records = [training_load(r) for r in result.interval_records]
    zeros = [
        r for r in records if r.get("status") == "no_strict_interior_timestamp"
    ]
    assert zeros and len(attempted) + len(zeros) == len(mapping.ordinals) - 1
    observed = {
        e.event_id: e
        for e in result.stream.events
        if e.origin.value == "observed"
    }
    for record in zeros:
        left, right = (
            observed[record[name]]
            for name in ("left_anchor_event_id", "right_anchor_event_id")
        )
        assert left.event_time_ns == right.event_time_ns
        assert left.source_row_id != right.source_row_id
        assert record["generated_event_ids"] == []
        assert "query_result" not in record


def test_preflight_triangle_budget_refuses_before_query_or_generator(
    tmp_path, monkeypatch
):
    plan, _ = fixture_weight_source(tmp_path / "source", dates=("2010-01-04",))
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")
    monkeypatch.setattr(candidates, "_target_cardinality", lambda *_: (5000, 1))

    def forbidden(*args, **kwargs):
        pytest.fail(
            "expensive query/generation must not run after failed preflight"
        )

    monkeypatch.setattr(candidates, "query_reference_motifs", forbidden)
    monkeypatch.setattr(
        candidates, "generate_empirical_motif_candidates", forbidden
    )
    monkeypatch.setattr(
        candidates, "_generate_day", candidates._generate_day_inline
    )
    with pytest.raises(ValueError, match="declared_triangle"):
        candidates._generate_day(degraded.bridges[0], model, model.index)
    shard = generate_training_weight_candidate_shard(
        degraded, "201001-1", model
    )
    assert not shard.days and len(shard.refusals) == 8
    assert any(
        r.reason == "candidate:declared_triangle_event_budget"
        for r in shard.refusals
    )


def test_positive_width_failure_refuses_the_whole_day_and_shard_is_fixed(
    tmp_path, monkeypatch
):
    plan, _ = fixture_weight_source(tmp_path / "source", dates=("2010-01-04",))
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")

    def refused(**kwargs):
        raise TrainingWeightDeterministicRefusal(
            candidates.MotifGenerationDecision.NO_SUPPORTED_CELL
        )

    monkeypatch.setattr(
        candidates, "generate_empirical_motif_candidates", refused
    )
    monkeypatch.setattr(
        candidates, "_generate_day", candidates._generate_day_inline
    )
    shard = generate_training_weight_candidate_shard(
        degraded, "201001-1", model
    )
    assert not shard.days and len(shard.refusals) == 8
    assert [r.utc_date for r in shard.refusals] == list(
        candidates._shard_dates(degraded, "201001-1")
    )
    assert any("engine:no_supported_cell" in r.reason for r in shard.refusals)
    with pytest.raises(ValueError, match="outside"):
        generate_training_weight_candidate_shard(
            degraded, "caller-chosen-window", model
        )


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("unexpected internal error"),
        TrainingWeightOperationalFailure(
            "candidate_day_deadline", "2010-01-04"
        ),
    ],
)
def test_operational_or_unexpected_failure_never_becomes_day_refusal(
    tmp_path, monkeypatch, failure
):
    plan, _ = fixture_weight_source(tmp_path / "source", dates=("2010-01-04",))
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")

    def fail(*args):
        raise failure

    monkeypatch.setattr(candidates, "_generate_day", fail)
    with pytest.raises(type(failure), match=str(failure)):
        generate_training_weight_candidate_shard(degraded, "201001-1", model)


def test_copied_model_and_subset_bytes_cannot_select_new_random_path(tmp_path):
    plan, _ = fixture_weight_source(
        tmp_path / "source", dates=("2010-01-04",), step_seconds=8
    )
    left = create_training_weight_degradation(plan, tmp_path / "left-subset")
    right = create_training_weight_degradation(plan, tmp_path / "right-subset")
    assert (
        left.subset_source.dataset_version_id
        == right.subset_source.dataset_version_id
    )
    assert left.bridges[0].parent_unit == right.bridges[0].parent_unit
    assert left.bridges[0].artifact_id != right.bridges[0].artifact_id
    model = fixture_weight_model(tmp_path / "model")
    copy_root = tmp_path / "copy"
    copy_root.mkdir()

    def copied(reference):
        path = copy_root / reference.sha256
        shutil.copyfile(reference.path, path)
        return replace(reference, path=str(path))

    moved = replace(
        model,
        index_file=copied(model.index_file),
        source_files=tuple(
            sorted(
                (copied(f) for f in model.source_files),
                key=lambda f: (f.sha256, f.path),
            )
        ),
    )
    assert model.artifact_id != moved.artifact_id
    assert model.stochastic_model_id == moved.stochastic_model_id
    assert verify_training_weight_model(moved) == model.index

    def generated(degradation, current):
        bridge = degradation.bridges[0]
        mapping = bridge.symbols[0]
        return candidates._generate_member_symbol(
            bridge,
            current,
            current.index,
            "member-60701",
            mapping.symbol,
            {},
            candidates._mapping_condition(mapping),
            float("inf"),
        )

    a, b = generated(left, model), generated(right, moved)
    assert a.run_json == b.run_json
    assert a.stream_json == b.stream_json
    assert a.interval_records == b.interval_records
    run_a = candidates.ReconstructionRunV1.from_dict(training_load(a.run_json))
    run_b = candidates.ReconstructionRunV1.from_dict(training_load(b.run_json))
    assert run_a.seed_for("member-60701", "same-interval") == run_b.seed_for(
        "member-60701", "same-interval"
    )


def test_known_empirical_inputs_cannot_be_downgraded_to_fixture_metadata(
    tmp_path,
):
    model = fixture_weight_model(tmp_path / "model")
    policy = read_training_weight_preregistration().to_dict()
    known = (
        policy["fixed_model"]["index_sha256"],
        policy["fixed_model"]["training_sources"][0]["sha256"],
        policy["epoch_mapping"]["sha256"],
        policy["source_partitions"][0]["sha256"],
    )
    for digest in known:
        declared = replace(
            model.index_file, path="/not-present/fixture", sha256=digest
        )
        with pytest.raises(ValueError, match="fixture cannot relabel"):
            # Guard precedes native JSON parsing and filesystem verification.
            replace(model, index_file=declared, index_json="not JSON")
    with pytest.raises(TypeError, match="closed reason"):
        TrainingWeightDeterministicRefusal("caller-free-form")
    assert (
        TrainingWeightDeterministicRefusal(
            CandidateRefusalCode.DAY_BYTES
        ).reason
        == "candidate:candidate_day_artifact_byte_budget"
    )


def _slow_day_fixture(path):
    from pathlib import Path

    Path(path).write_text(str(os.getpid()), encoding="ascii")
    time.sleep(30)


def test_day_supervisor_terminates_and_reaps_late_native_call(tmp_path):
    before = {p.pid for p in multiprocessing.active_children()}
    start = time.monotonic()
    with pytest.raises(TrainingWeightOperationalFailure, match="day_deadline"):
        candidates._supervise_day_worker(
            _slow_day_fixture,
            (str(tmp_path / "worker.pid"),),
            2.0,
            "2010-01-04",
        )
    assert time.monotonic() - start < 10.0
    assert {p.pid for p in multiprocessing.active_children()} == before


def test_expanded_native_json_counts_against_frozen_aggregate_guard():
    budget = [5]
    with pytest.raises(
        TrainingWeightDeterministicRefusal, match="expanded_node"
    ):
        candidates._consume_nodes({"native": [0, 1, 2, 3]}, budget)


@pytest.mark.parametrize("step", [5, 2])
def test_dense_transparent_native_tables_keep_full_matches_and_native_bytes(
    tmp_path, step
):
    plan, _ = fixture_weight_source(
        tmp_path / "source", dates=("2010-01-04",), step_seconds=step
    )
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")
    old = model.index
    fragments = tuple(
        replace(
            old.fragments[i % len(old.fragments)],
            source_window_id="reference-motif-source-window:sha256:"
            + f"{i + 1:064x}",
            fragment_id="",
        )
        for i in range(256)
    )
    index = replace(
        old, fragments=fragments, source_window_count=256, index_id=""
    )
    from pathlib import Path
    from histdatacom.data_quality.training_weight_lineage import (
        WeightEvidenceKind,
    )

    Path(model.index_file.path).write_text(index.to_json(), encoding="utf-8")
    model = candidates.read_training_weight_model(
        model.index_file.path,
        evidence_kind=WeightEvidenceKind.FIXTURE,
        adapter_source_fingerprint="a" * 64,
    )
    if step == 2:
        # The final lossless representation still exceeds the independent
        # frozen node limit at this density. Preserve honest refusal rather
        # than changing samples, matches or budgets to force a positive run.
        with pytest.raises(
            TrainingWeightDeterministicRefusal, match="expanded_node"
        ):
            candidates._generate_day_inline(degraded.bridges[0], model, index)
        return
    day = candidates._generate_day_inline(degraded.bridges[0], model, index)
    assert len(day.to_json()) <= 8 * 1024 * 1024
    assert len(day.query_supports) == 3
    assert all(
        len(training_load(s)["matches"]) == 32 for s in day.query_supports
    )
    assert TrainingWeightCandidateDayV1.from_json(day.to_json()) == day
    for candidate in day.candidates:
        native = candidate.stream.to_dict()
        assert (
            candidates._encode_stream(dict(native))
            == candidate.stream_encoding_json
        )
        assert candidate.stream_json == candidates.training_json(native)
        assert candidate.native_transformations


def test_native_table_inverse_and_resealed_malformed_columns_and_uint64_refuse(
    tmp_path,
    monkeypatch,
):
    plan, _ = fixture_weight_source(
        tmp_path / "source", dates=("2010-01-04",), step_seconds=8
    )
    degraded = create_training_weight_degradation(plan, tmp_path / "subset")
    model = fixture_weight_model(tmp_path / "model")
    bridge = degraded.bridges[0]
    mapping = bridge.symbols[0]
    candidate = candidates._generate_member_symbol(
        bridge,
        model,
        model.index,
        "member-60701",
        mapping.symbol,
        {},
        candidates._mapping_condition(mapping),
        float("inf"),
    )
    original = training_load(candidate.stream_encoding_json)
    for mutation in (
        "unknown",
        "overlap",
        "duplicate",
        "row_width",
        "tampered_bid",
    ):
        encoded = json.loads(json.dumps(original))
        events = encoded["events"]
        if mutation == "unknown":
            events["constants"]["unknown_later_field"] = 0
        elif mutation == "overlap":
            events["constants"][events["columns"][0]] = 0
        elif mutation == "duplicate":
            events["columns"].append(events["columns"][0])
        elif mutation == "row_width":
            events["rows"][0].append(0)
        else:
            events["rows"][0][events["columns"].index("bid")] = -1.0
        with pytest.raises(ValueError):
            replace(
                candidate,
                stream_encoding_json=candidates.training_json(encoded),
            )
    table = training_load(candidate.transformations_encoding_json)
    rows = candidates._decode_native_rows(table, "transformation")
    assert candidates._native_json(rows) == candidates._native_json(
        [t.to_dict() for t in candidate.native_transformations]
    )
    seed_column = (
        table["columns"].index("seed") if "seed" in table["columns"] else None
    )
    for invalid in ("18446744073709551616", "01", "-1", 1, 1.0, True):
        changed = json.loads(json.dumps(table))
        if seed_column is None:
            changed["constants"]["seed"] = invalid
        else:
            changed["rows"][0][seed_column] = invalid
        with pytest.raises(ValueError):
            replace(
                candidate,
                transformations_encoding_json=candidates.training_json(changed),
            )
    original_transform = candidate.native_transformations[0]
    extrema = [
        replace(
            original_transform, match_distance=0.0, transformation_id=""
        ).to_dict(),
        replace(
            original_transform,
            match_distance=-0.0,
            seed=2**64 - 1,
            transformation_id="",
        ).to_dict(),
    ]
    encoded = candidates._encode_native_rows(extrema, "transformation")
    assert "match_distance" in encoded["columns"]
    assert candidates._native_json(
        candidates._decode_native_rows(encoded, "transformation")
    ) == candidates._native_json(extrema)
    for field in ("stream_encoding_json", "transformations_encoding_json"):
        text = getattr(candidate, field)
        pretty = json.dumps(json.loads(text), indent=2)
        with pytest.raises(ValueError, match="canonical"):
            replace(candidate, **{field: pretty})
    amplified = training_load(candidate.stream_encoding_json)
    amplified["events"]["constants"]["run_id"] = "x" * 250000

    def forbidden(*args, **kwargs):
        pytest.fail("native constructor must not run after expansion preflight")

    monkeypatch.setattr(
        candidates.SyntheticEventStreamV1, "from_dict", forbidden
    )
    with pytest.raises(
        TrainingWeightDeterministicRefusal, match="projection_byte"
    ):
        replace(
            candidate, stream_encoding_json=candidates.training_json(amplified)
        )
