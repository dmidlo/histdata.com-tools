"""Generated-only publication path guards, not scientific qualification.

Stage tests use explicit upstream doubles only to reach the actual derived
filesystem target. Partition positives execute real tiny Arrow/Parquet writers.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.managed_artifact_boundary import (
    MANAGED_ARTIFACT_MARKER,
    ManagedArtifactBoundaryError,
)
from histdatacom.synthetic import bars, persistence
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)


def _protect(path):
    path.mkdir(parents=True)
    (path / MANAGED_ARTIFACT_MARKER).write_bytes(b"incomplete marker")
    (path / "sentinel").write_bytes(b"generated evidence, preserve exactly")
    return path


def _tree(root):
    return tuple(
        (
            str(path.relative_to(root)),
            path.read_bytes() if path.is_file() else None,
        )
        for path in sorted(root.rglob("*"))
    )


def _stream():
    event = SyntheticEventV1.observed(
        symbol="eurusd",
        event_time_ns=1_577_836_800_000_000_000,
        event_sequence=0,
        bid=1.1,
        ask=1.1002,
        run_id="generated-boundary-run",
        ensemble_member_id="generated-member",
        source_version_id="generated-boundary-source",
        source_series_id="ascii:T:eurusd",
        source_period="202001",
        source_row_id=1,
    )
    return SyntheticEventStreamV1(
        run_id=event.run_id,
        ensemble_member_id=event.ensemble_member_id,
        symbol=event.symbol,
        events=(event,),
        source_version_ids=(event.source_version_id,),
    )


def _bar():
    stream = _stream()
    return bars.derive_reconstruction_bars(
        stream.events,
        source_product_manifest_id="generated-boundary-product",
        run_id=stream.run_id,
        ensemble_member_id=stream.ensemble_member_id,
        policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
    )[0]


@pytest.mark.parametrize("kind", ["events", "bars", "bar_partition"])
def test_publication_root_refuses_before_inspecting_scientific_inputs(
    tmp_path, kind
):
    managed = _protect(tmp_path / "managed")
    target = managed / "must-not-be-created"
    before = _tree(tmp_path)
    with pytest.raises(ManagedArtifactBoundaryError):
        if kind == "events":
            persistence.stage_reconstruction_publication(
                target,
                None,
                immutable_source_anchors=(),
                symbol_group_id="generated",
                retention_plan=None,
                storage_policy=None,
            )
        elif kind == "bars":
            bars.stage_derived_bar_publication(target, tmp_path / "absent")
        else:
            bars._PartitionWriter(target, None, row_group_size=1, buffer_rows=1)
    assert _tree(tmp_path) == before


@pytest.mark.parametrize("kind", ["events", "bars"])
def test_actual_derived_scratch_is_admitted_before_mkdir(
    tmp_path, monkeypatch, kind
):
    root = tmp_path / "ordinary"
    stream = _stream()
    if kind == "events":
        manifest = SimpleNamespace(
            run_id=stream.run_id,
            fingerprint_id="generated-profile",
            ensemble_member_id=stream.ensemble_member_id,
        )
        rendered = SimpleNamespace(streams=(stream,), manifest=manifest)
        axis = persistence._axis_directory(
            root,
            run_id=manifest.run_id,
            broker_profile_id=manifest.fingerprint_id,
            ensemble_member_id=manifest.ensemble_member_id,
            symbol_group_id="generated",
        )
        monkeypatch.setattr(
            persistence, "_require_broker_policy", lambda *_: None
        )
        monkeypatch.setattr(
            persistence, "_validate_publication_inputs", lambda *_: None
        )
    else:
        policy = bars.DerivedBarPolicyV1(intervals=("1m",))
        source = SimpleNamespace(
            manifest_id="generated-product", symbols=("eurusd",)
        )
        axis = bars._bar_axis_directory(
            root, source.manifest_id, policy.policy_id
        )
        monkeypatch.setattr(
            bars, "verify_reconstruction_publication", lambda *_: source
        )
    _protect(axis)
    before = _tree(tmp_path)
    with pytest.raises(ManagedArtifactBoundaryError):
        if kind == "events":
            persistence.stage_reconstruction_publication(
                root,
                rendered,
                immutable_source_anchors=stream.events,
                symbol_group_id="generated",
                retention_plan=None,
                storage_policy=None,
            )
        else:
            bars.stage_derived_bar_publication(
                root, tmp_path / "source.json", policy=policy
            )
    assert _tree(tmp_path) == before


@pytest.mark.parametrize("kind", ["events", "bars"])
@pytest.mark.parametrize(
    "protected_target",
    ["staging", "committed", "nested_staging", "nested_committed"],
)
def test_commit_preflights_both_trees_before_any_verification_or_rename(
    tmp_path, kind, protected_target
):
    staging = tmp_path / "staging"
    committed = tmp_path / "committed"
    selected = staging if "staging" in protected_target else committed
    _protect(
        selected / "child"
        if protected_target.startswith("nested")
        else selected
    )
    before = _tree(tmp_path)
    if kind == "events":
        staged = persistence.StagedReconstructionPublicationV1(
            tmp_path, staging, committed, None
        )
        commit = persistence.commit_reconstruction_publication
    else:
        staged = bars.StagedDerivedBarPublicationV1(
            tmp_path, staging, committed, None
        )
        commit = bars.commit_derived_bar_publication
    with pytest.raises(ManagedArtifactBoundaryError):
        commit(staged)
    assert _tree(tmp_path) == before


@pytest.mark.parametrize("kind", ["events", "bars"])
def test_partition_writer_guards_derived_target_before_parent_creation(
    tmp_path, kind
):
    staging = tmp_path / "ordinary"
    if kind == "events":
        relative = persistence._partition_relative_path("eurusd", "2020-01-01")
    else:
        bar = _bar()
        relative = bars._bar_partition_relative_path(
            bar.symbol,
            bar.scope,
            bar.interval_code,
            bars._bar_month(bar.bar_start_ns),
        )
    _protect(staging / Path(relative).parts[0])
    before = _tree(tmp_path)
    with pytest.raises(ManagedArtifactBoundaryError):
        if kind == "events":
            persistence._write_product_partitions(
                staging, (_stream(),), row_group_size=1
            )
        else:
            bars._PartitionWriter(staging, bar, row_group_size=1, buffer_rows=1)
    assert _tree(tmp_path) == before


@pytest.mark.parametrize("kind", ["events", "bars"])
def test_unmanaged_tiny_partition_writer_still_writes_real_parquet(
    tmp_path, kind
):
    import pyarrow.parquet as pq

    if kind == "events":
        partition = persistence._write_product_partitions(
            tmp_path, (_stream(),), row_group_size=1
        )[0]
    else:
        bar = _bar()
        writer = bars._PartitionWriter(
            tmp_path, bar, row_group_size=1, buffer_rows=1
        )
        writer.add(bar)
        partition = writer.close()
    path = tmp_path / partition.relative_path
    assert path.is_file()
    assert pq.ParquetFile(path).metadata.num_rows == partition.row_count == 1
