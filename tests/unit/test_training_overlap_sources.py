"""Immutable source coordinates and full replay before projection."""

from dataclasses import replace

import pytest

from histdatacom.datasets import DatasetCatalog
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapSelectionV1,
)
from histdatacom.data_quality.training_overlap_sources import (
    verify_overlap_source,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
)
from tests.fixtures.training_overlap_v1 import overlap_fixture


def test_stride_changes_memberships_not_immutable_coordinates(tmp_path):
    plan = overlap_fixture(tmp_path)
    a = verify_overlap_source(plan)
    b = verify_overlap_source(
        replace(
            plan,
            geometry=replace(plan.geometry, stride_ns=plan.geometry.width_ns),
        )
    )
    assert tuple(c.coordinate_id for c in a.coordinates) == tuple(
        c.coordinate_id for c in b.coordinates
    )
    assert tuple(c.window_indices for c in a.coordinates) != tuple(
        c.window_indices for c in b.coordinates
    )
    assert (
        len({c.source_row_key for c in a.coordinates})
        == len(a.coordinates)
        == 18
    )


def test_empty_projection_still_replays_actual_source_bytes(tmp_path):
    plan = overlap_fixture(tmp_path)
    result = materialize_training_overlap(
        plan, TrainingOverlapSelectionV1(window_ids=())
    )
    assert result.rows == () and len(result.coordinates) == 18
    catalog = DatasetCatalog.from_json(plan.source.catalog_json)
    version = catalog.versions[0]
    # Resolve only this disposable controlled fixture's retained source file.
    paths = list(tmp_path.glob("ASCII/T/*/*.feather"))
    if not paths:
        paths = list(tmp_path.glob("ASCII/T/**/*.arrow"))
    if not paths:
        paths = [
            p for p in (tmp_path / "ASCII" / "T").rglob("*") if p.is_file()
        ]
    assert version.dataset_version_id == plan.source.dataset_version_id
    paths[0].write_bytes(b"corrupt fixture bytes")
    with pytest.raises((ValueError, OSError)):
        materialize_training_overlap(
            plan, TrainingOverlapSelectionV1(window_ids=())
        )


def test_false_ownership_refuses_even_short_span(tmp_path):
    plan = overlap_fixture(tmp_path)
    unit = replace(plan.ownership.units[0], anchor_content_sha256="a" * 64)
    ownership = replace(plan.ownership, units=(unit, *plan.ownership.units[1:]))
    split = replace(
        plan.split,
        ownership_id=ownership.artifact_id,
        assignments=(
            replace(
                plan.split.assignments[0], evidence_unit_id=unit.artifact_id
            ),
            *plan.split.assignments[1:],
        ),
    )
    altered = replace(
        plan,
        ownership=ownership,
        split=split,
        geometry=replace(plan.geometry, end_ns=plan.geometry.start_ns + 1),
    )
    with pytest.raises(ValueError):
        materialize_training_overlap(
            altered, TrainingOverlapSelectionV1(window_ids=())
        )


def test_complete_real_source_bound_is_not_bypassed_by_empty_projection(
    tmp_path, monkeypatch
):
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapGeometryV1,
        TrainingOverlapPlanV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalPartition,
        TrainingTemporalAssignmentV1,
        TrainingTemporalSplitV1,
    )
    from histdatacom.data_quality import training_overlap_views as views
    from tests.fixtures.training_join_v1 import native_observed_source
    from tests.fixtures.training_overlap_v1 import BASE

    source, _ = native_observed_source(
        tmp_path, tuple(BASE + i * 1_000_000 for i in range(1366))
    )
    ownership = build_training_ownership(source)
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(u.artifact_id, TemporalPartition.TRAIN)
            for u in ownership.units
        ),
    )
    plan = TrainingOverlapPlanV1(
        source,
        ownership,
        split,
        TrainingOverlapGeometryV1(
            BASE, BASE + 2_000_000_000, 1_000_000_000, 1_000_000_000
        ),
    )
    monkeypatch.setattr(
        views,
        "native_overlap_features",
        lambda *args: pytest.fail("selected-output native work must not start"),
    )
    with pytest.raises(ValueError, match="source/alias event inventory"):
        materialize_training_overlap(
            plan, TrainingOverlapSelectionV1(window_ids=())
        )


def test_same_source_coordinate_survives_actual_cross_day_ownership_coarsening(
    tmp_path,
):
    from unittest.mock import patch
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TemporalPartition,
        TrainingTemporalAssignmentV1,
        TrainingTemporalSplitV1,
    )
    from tests.fixtures import training_substrate_v1 as substrate
    from tests.fixtures.training_overlap_v1 import TIMES

    plan = overlap_fixture(tmp_path / "observed")
    original = verify_overlap_source(plan)
    version = DatasetCatalog.from_json(plan.source.catalog_json).versions[0]
    with patch.object(substrate, "TIMES", TIMES):
        product, _ = substrate.published_product(
            tmp_path / "cross-day-product", version, indices=(1, 2)
        )
    source = replace(
        plan.source, product_manifest_paths=(str(product.manifest_path),)
    )
    ownership = build_training_ownership(source)
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(u.artifact_id, TemporalPartition.TRAIN)
            for u in ownership.units
        ),
    )
    revised = verify_overlap_source(
        replace(plan, source=source, ownership=ownership, split=split)
    )
    before = {c.source_row_key: c for c in original.coordinates}
    after = {c.source_row_key: c for c in revised.coordinates}
    assert any(
        before[key].evidence_unit_id != after[key].evidence_unit_id
        for key in before
    )
    assert all(
        before[key].coordinate_id == after[key].coordinate_id for key in before
    )
    assert len(ownership.units) == len(plan.ownership.units) - 1
