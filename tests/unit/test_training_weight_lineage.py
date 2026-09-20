"""Actual tiny IPC/catalog/ordinal replay, not a real calibration execution."""

from dataclasses import replace

import polars as pl
import pytest

import histdatacom.data_quality.training_weight_lineage as lineage
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_weight_lineage import (
    TrainingWeightDayBridgeV1,
    WeightDayRefusal,
    WeightEvidenceKind,
    create_training_weight_degradation,
    replay_training_weight_degradation,
)
from tests.fixtures.training_weight_sources import fixture_weight_source
from histdatacom.data_quality.training_weight_contracts import (
    read_training_weight_preregistration,
)
from histdatacom.data_quality.training_contracts import TrainingSourceV1


def test_actual_subset_bridge_preserves_full_parent_day_identity_and_ordinals(
    tmp_path, monkeypatch
):
    plan, _ = fixture_weight_source(tmp_path / "parent")
    calls = []
    original = lineage.verify_training_source

    def counted(source):
        calls.append(source.dataset_version_id)
        return original(source)

    monkeypatch.setattr(lineage, "verify_training_source", counted)
    result = create_training_weight_degradation(plan, tmp_path / "subset")
    assert calls == [
        plan.parent_source.dataset_version_id,
        result.subset_source.dataset_version_id,
    ]
    assert len(result.bridges) == 2
    assert len(result.refusals) == 19
    assert result.parent_ownership == build_training_ownership(
        plan.parent_source
    )
    assert (
        result.subset_source.dataset_version_id
        != plan.parent_source.dataset_version_id
    )
    for bridge in result.bridges:
        assert bridge.parent_unit in result.parent_ownership.units
        assert bridge.subset_unit in result.subset_ownership.units
        assert TrainingWeightDayBridgeV1.from_json(bridge.to_json()) == bridge
        for symbol in bridge.symbols:
            ordinals = symbol.ordinals
            # 121 source quotes at five-second cadence become two boundaries
            # plus interior indices 0,4,...,116: exactly 32 retained quotes.
            assert len(ordinals) == 32
            assert ordinals[1].parent_row_id == ordinals[0].parent_row_id + 1
            assert all(
                b.parent_row_id - a.parent_row_id == 4
                for a, b in zip(ordinals[1:-2], ordinals[2:-1])
            )
            assert ordinals[-1].parent_row_id == ordinals[0].parent_row_id + 120
    assert replay_training_weight_degradation(result) == result
    assert calls[-2:] == [
        plan.parent_source.dataset_version_id,
        result.subset_source.dataset_version_id,
    ]
    with pytest.raises(ValueError, match="new directory"):
        create_training_weight_degradation(plan, tmp_path / "subset")


def test_complete_month_inventory_cannot_hide_days_or_alter_source_mapping(
    tmp_path,
):
    plan, _ = fixture_weight_source(tmp_path / "parent")
    result = create_training_weight_degradation(plan, tmp_path / "subset")
    with pytest.raises(ValueError, match="ordinal bridge"):
        replay_training_weight_degradation(
            replace(result, bridges=result.bridges[:1])
        )
    with pytest.raises(ValueError, match="refusal inventory"):
        replay_training_weight_degradation(
            replace(result, refusals=result.refusals[:1])
        )
    bridge = result.bridges[0]
    symbol = bridge.symbols[0]
    # A caller can construct another internally consistent integer map, but
    # canonical construction must never be mistaken for source verification.
    changed = replace(
        symbol,
        ordinals=tuple(
            replace(o, parent_row_id=o.parent_row_id + 10000)
            for o in symbol.ordinals
        ),
    )
    altered = replace(bridge, symbols=(changed, *bridge.symbols[1:]))
    with pytest.raises(ValueError, match="ordinal bridge"):
        replay_training_weight_degradation(
            replace(result, bridges=(altered, *result.bridges[1:]))
        )
    raw = bridge.to_dict()
    raw["symbols"][0]["ordinals"][0]["hidden_parent_value"] = 1.0
    with pytest.raises(ValueError, match="unknown"):
        TrainingWeightDayBridgeV1.from_dict(raw)


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"include_end": False}, WeightDayRefusal.MISSING_BOUNDARY),
        ({"step_seconds": 10}, WeightDayRefusal.INSUFFICIENT_ROWS),
        ({"crossed": True}, WeightDayRefusal.INVALID_QUOTES),
        ({"before_seconds": 1}, WeightDayRefusal.MISSING_BOUNDARY),
    ],
)
def test_missing_invalid_or_insufficient_support_refuses_whole_triangle_day(
    tmp_path, kwargs, reason
):
    plan, _ = fixture_weight_source(
        tmp_path / "parent", dates=("2010-01-04",), **kwargs
    )
    result = create_training_weight_degradation(plan, tmp_path / "subset")
    assert result.bridges == () and result.subset_source is None
    assert (
        next(r.reason for r in result.refusals if r.utc_date == "2010-01-04")
        is reason
    )
    assert replay_training_weight_degradation(result) == result
    assert not (tmp_path / "subset").exists()


def test_duplicate_timestamp_ownership_and_zero_volume_placeholder(tmp_path):
    plan, _ = fixture_weight_source(
        tmp_path / "parent", dates=("2010-01-04",), duplicate=True
    )
    result = create_training_weight_degradation(plan, tmp_path / "subset")
    bridge = result.bridges[0]
    # Last parent-order quote at the exact left boundary owns the probe.
    assert bridge.symbols[0].ordinals[0].parent_row_id == 3
    catalog = lineage.DatasetCatalog.from_json(
        result.subset_source.catalog_json
    )
    for partition in catalog.versions[0].partitions:
        assert pl.read_ipc(partition.artifact.path)[
            "vol"
        ].unique().to_list() == [0]
    assert replay_training_weight_degradation(result) == result


def test_source_tamper_and_fixture_promotion_fail_closed(tmp_path):
    plan, version = fixture_weight_source(tmp_path / "parent")
    with pytest.raises(ValueError, match="preregistered source"):
        replace(plan, evidence_kind=WeightEvidenceKind.PREREGISTERED)
    result = create_training_weight_degradation(plan, tmp_path / "subset")
    path = version.partitions[0].artifact.path
    frame = pl.read_ipc(path).with_columns(
        (pl.col("bid") - 0.000001).alias("bid")
    )
    frame.write_ipc(path)
    with pytest.raises(ValueError):
        replay_training_weight_degradation(result)


def test_real_metadata_plan_requires_approval_before_any_decode_or_output(
    tmp_path, monkeypatch
):
    plan, version = fixture_weight_source(tmp_path / "parent")
    # Metadata-only allowlist-shaped catalog; the actual tiny fixture bytes do
    # NOT match it. The absent operational approval must stop before a reader
    # can even inspect those bytes. This is not an approved real-source plan.
    expected = {
        (p["symbol"], p["period"]): p
        for p in read_training_weight_preregistration().to_dict()[
            "source_partitions"
        ]
    }
    partitions = []
    for partition in version.partitions:
        item = expected[(partition.symbol, partition.period)]
        artifact = replace(
            partition.artifact,
            sha256=item["sha256"],
            size_bytes=item["size_bytes"],
        )
        partitions.append(
            replace(
                partition,
                artifact=artifact,
                source_artifact_sha256=item["sha256"],
                row_count=item["row_count"],
                partition_id="",
            )
        )
    declared = replace(
        version,
        partitions=tuple(partitions),
        dataset_version_id="",
        manifest_sha256="",
    )
    catalog = lineage.DatasetCatalog.from_json(plan.parent_source.catalog_json)
    catalog = replace(catalog, versions=(declared,), catalog_id="")
    source = TrainingSourceV1(catalog.to_json(), declared.dataset_version_id)
    metadata_plan = replace(
        plan,
        parent_source=source,
        evidence_kind=WeightEvidenceKind.PREREGISTERED,
    )
    with pytest.raises(ValueError, match="fixture cannot relabel"):
        replace(metadata_plan, evidence_kind=WeightEvidenceKind.FIXTURE)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "raw source decode must not run without operational approval"
        )

    monkeypatch.setattr(lineage, "verify_training_source", forbidden)
    with pytest.raises(ValueError, match="approval"):
        create_training_weight_degradation(
            metadata_plan, tmp_path / "unapproved"
        )
    assert not (tmp_path / "unapproved").exists()


def test_source_mutation_during_adapter_read_never_yields_earlier_hash_rows(
    tmp_path, monkeypatch
):
    plan, version = fixture_weight_source(tmp_path / "parent")
    original = lineage.HistDataProviderAdapter.read_partition
    changed = False

    def mutate_after_read(self, partition):
        nonlocal changed
        frame = original(self, partition)
        if not changed:
            changed = True
            frame.with_columns(
                (pl.col("bid") - 0.000001).alias("bid")
            ).write_ipc(partition.artifact.path)
        return frame

    monkeypatch.setattr(
        lineage.HistDataProviderAdapter, "read_partition", mutate_after_read
    )
    with pytest.raises(ValueError):
        create_training_weight_degradation(plan, tmp_path / "subset")
    assert changed and not (tmp_path / "subset").exists()
