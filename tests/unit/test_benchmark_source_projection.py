"""Contracts for canonical benchmark source projections."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pyarrow.dataset as ds
import pytest

import histdatacom.synthetic.benchmark_corpus as corpus_module
import histdatacom.synthetic.benchmark_source_projection as projection_module
from histdatacom.synthetic.benchmark_corpus import (
    ReverseDegradationCorpusProfileV1,
)
from histdatacom.synthetic.benchmark_source_projection import (
    CANONICAL_BENCHMARK_PROJECTION_COLUMNS,
    BenchmarkSourceProjectionManifestV1,
    build_benchmark_source_projection_manifest,
    inspect_benchmark_source_projection,
    read_benchmark_source_projection_manifest,
    validate_benchmark_source_projections,
    write_benchmark_source_projection_manifest,
)


def _parent(root: Path) -> Path:
    path = root / "eurusd" / "2026" / "7" / ".data"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "datetime": [1_000, 2_000, 1_500, 3_000],
            "bid": [1.01, 1.02, 1.015, 1.03],
            "ask": [1.011, 1.021, 1.016, 1.031],
            "vol": pl.Series([0, 1, 2, 3], dtype=pl.Int32),
            "unused_text": ["wide"] * 4,
            "unused_number": [10, 11, 12, 13],
        }
    ).write_ipc(path)
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_projection_preserves_values_order_and_content_lineage(
    tmp_path: Path,
) -> None:
    parent_root = tmp_path / "parent"
    projection_root = tmp_path / "projected"
    parent = _parent(parent_root)

    manifest = build_benchmark_source_projection_manifest(
        parent_root,
        projection_root,
        expected_parent_sha256={("EURUSD", "202607"): _sha256(parent)},
        frozen_at_utc="2026-09-13T03:39:39Z",
    )

    assert isinstance(manifest, BenchmarkSourceProjectionManifestV1)
    assert manifest.to_dict()["event_rows_embedded"] is False
    projection = manifest.projections[0]
    projected = projection_root / projection.projected_relative_path
    assert projection.projected_sha256 in projected.name
    assert projection.parent_row_count == projection.projected_row_count == 4
    assert projection.first_timestamp_ms == 1_000
    assert projection.last_timestamp_ms == 3_000
    assert projection.timestamp_regression_count == 1
    assert projection.maximum_timestamp_regression_ms == 500
    assert tuple(ds.dataset(projected, format="ipc").schema.names) == (
        CANONICAL_BENCHMARK_PROJECTION_COLUMNS
    )
    assert (
        pl.read_ipc(parent)
        .select(CANONICAL_BENCHMARK_PROJECTION_COLUMNS)
        .equals(pl.read_ipc(projected))
    )
    assert inspect_benchmark_source_projection(projected).values_sha256 == (
        projection.values_sha256
    )
    validate_benchmark_source_projections(manifest, projection_root)

    rebuilt = build_benchmark_source_projection_manifest(
        parent_root,
        projection_root,
        expected_parent_sha256={("EURUSD", "202607"): _sha256(parent)},
        frozen_at_utc="2026-09-13T03:39:39Z",
    )
    assert rebuilt == manifest
    assert len(tuple(projection_root.rglob("source-projection-*.arrow"))) == 1

    artifact = write_benchmark_source_projection_manifest(
        manifest, tmp_path / "evidence"
    )
    assert read_benchmark_source_projection_manifest(artifact.path) == manifest
    assert artifact.metadata == {"manifest_id": manifest.manifest_id}


def test_projection_rejects_unverified_parent_and_tampered_artifacts(
    tmp_path: Path,
) -> None:
    parent_root = tmp_path / "parent"
    projection_root = tmp_path / "projected"
    parent = _parent(parent_root)
    with pytest.raises(ValueError, match="parent hash differs"):
        build_benchmark_source_projection_manifest(
            parent_root,
            projection_root,
            expected_parent_sha256={("EURUSD", "202607"): "0" * 64},
            frozen_at_utc="2026-09-13T03:39:39Z",
        )

    manifest = build_benchmark_source_projection_manifest(
        parent_root,
        projection_root,
        expected_parent_sha256={("EURUSD", "202607"): _sha256(parent)},
        frozen_at_utc="2026-09-13T03:39:39Z",
    )
    artifact = write_benchmark_source_projection_manifest(
        manifest, tmp_path / "evidence"
    )
    manifest_path = Path(artifact.path)
    manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="content hash differs"):
        read_benchmark_source_projection_manifest(manifest_path)

    projection = manifest.projections[0]
    projected = projection_root / projection.projected_relative_path
    projected.write_bytes(projected.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="size differs"):
        validate_benchmark_source_projections(manifest, projection_root)


def test_corpus_source_discovery_uses_validated_projection_path(
    tmp_path: Path,
) -> None:
    parent_root = tmp_path / "parent"
    projection_root = tmp_path / "projected"
    parent = _parent(parent_root)
    manifest = build_benchmark_source_projection_manifest(
        parent_root,
        projection_root,
        expected_parent_sha256={("EURUSD", "202607"): _sha256(parent)},
        frozen_at_utc="2026-09-13T03:39:39Z",
    )
    for symbol in ("EURGBP", "GBPUSD"):
        destination = projection_root / symbol.lower() / "2026" / "7" / ".data"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(parent.read_bytes())
    profile = ReverseDegradationCorpusProfileV1(
        split_periods={
            "calibration": "202605",
            "validation": "202606",
            "final_holdout": "202607",
        }
    )
    for period in ("202605", "202606"):
        for symbol in profile.symbols:
            destination = (
                projection_root
                / symbol.lower()
                / "2026"
                / str(int(period[4:]))
                / ".data"
            )
            destination.parent.mkdir(parents=True)
            destination.write_bytes(parent.read_bytes())

    validate_benchmark_source_projections(manifest, projection_root)
    sources = corpus_module._discover_source_partitions(
        projection_root, profile, projection_manifest=manifest
    )

    selected = next(
        item
        for item in sources
        if item.symbol == "EURUSD" and item.period == "202607"
    )
    assert (
        selected.relative_path
        == manifest.projections[0].projected_relative_path
    )
    assert selected.sha256 == manifest.projections[0].projected_sha256


def test_projection_rejects_parent_mutation_during_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent_root = tmp_path / "parent"
    parent = _parent(parent_root)
    expected = _sha256(parent)
    original = projection_module._file_sha256

    def mutating_hash(path: Path) -> str:
        digest = original(path)
        if path == parent:
            path.write_bytes(path.read_bytes() + b"changed")
        return digest

    monkeypatch.setattr(projection_module, "_file_sha256", mutating_hash)
    with pytest.raises(ValueError, match="changed while hashing"):
        build_benchmark_source_projection_manifest(
            parent_root,
            tmp_path / "projected",
            expected_parent_sha256={("EURUSD", "202607"): expected},
            frozen_at_utc="2026-09-13T03:39:39Z",
        )
