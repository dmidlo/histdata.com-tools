"""Tests for sidecar provenance data-quality checks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from histdatacom.data_quality import (
    PROVENANCE_MANIFEST_RULE_ID,
    PROVENANCE_METADATA_KEY,
    QualitySeverity,
    QualityStatus,
    discover_quality_targets,
    provenance_manifest_metadata,
    quality_rules_for_groups,
    quality_run_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import CACHE_FILENAME
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.runtime_contracts import ArtifactRef, StageResult, WorkItem
from histdatacom.runtime_contracts import WorkStatus
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    write_ascii_case,
    write_zip_case,
)


def test_clean_sidecar_lineage_produces_provenance_section(
    tmp_path: Path,
) -> None:
    """Matching files, artifacts, checksums, and cache metadata should pass."""
    _write_lineage(tmp_path)

    report = _provenance_report(tmp_path)

    manifest = report.metadata[PROVENANCE_METADATA_KEY]
    assert report.status is QualityStatus.CLEAN
    assert manifest["schema_version"] == "histdatacom.provenance-manifest.v1"
    assert manifest["available"] is True
    assert manifest["store_count"] == 1
    assert manifest["work_item_count"] == 1
    assert manifest["data_artifact_count"] == 3
    assert manifest["finding_count"] == 0
    assert [
        rule.rule_id for rule in quality_run_rules_for_groups(("provenance",))
    ] == [PROVENANCE_MANIFEST_RULE_ID]


def test_provenance_reports_missing_artifact(
    tmp_path: Path,
) -> None:
    """Sidecar artifact refs to files that are gone should be hard failures."""
    paths = _write_lineage(tmp_path)
    paths.cache.unlink()

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_ARTIFACT_MISSING")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.location.path == str(paths.cache)
    assert finding.metadata["artifact_kind"] == "cache"


def test_provenance_reports_stale_cache(
    tmp_path: Path,
) -> None:
    """Cache files older than their source artifacts should be warnings."""
    paths = _write_lineage(tmp_path)
    os.utime(paths.cache, (1_700_000_000, 1_700_000_000))
    os.utime(paths.zip, (1_700_000_050, 1_700_000_050))
    os.utime(paths.csv, (1_700_000_100, 1_700_000_100))

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_CACHE_STALE")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.location.path == str(paths.cache)
    assert finding.metadata["source_path"] == str(paths.csv)


def test_provenance_reports_checksum_mismatch(
    tmp_path: Path,
) -> None:
    """Stored checksums should be compared to the current artifact bytes."""
    _write_lineage(tmp_path, cache_sha256="0" * 64)

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_ARTIFACT_CHECKSUM_MISMATCH")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["expected_sha256"] == "0" * 64


def test_provenance_reports_cache_metadata_mismatch(
    tmp_path: Path,
) -> None:
    """Cache lineage should reconcile row counts and timestamp bounds."""
    _write_lineage(tmp_path, cache_line_count="4")

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_CACHE_METADATA_MISMATCH")
    assert report.status is QualityStatus.FAILED
    assert finding.severity is QualitySeverity.ERROR
    assert finding.metadata["field"] == "line_count"
    assert finding.metadata["artifact_value"] == "4"
    assert finding.metadata["work_item_value"] == "3"


def test_provenance_reports_orphan_discovered_file(
    tmp_path: Path,
) -> None:
    """Discovered files absent from sidecar artifacts should be warnings."""
    _write_lineage(tmp_path)
    orphan = tmp_path / "DAT_ASCII_GBPUSD_M1_201202.csv"
    orphan.write_text(CLEAN_M1_CASE.text, encoding="utf-8")

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_ORPHAN_TARGET")
    assert report.status is QualityStatus.WARNING
    assert finding.severity is QualitySeverity.WARNING
    assert finding.location.path == str(orphan.resolve())
    assert finding.metadata["symbol"] == "GBPUSD"


def test_explicit_provenance_without_store_is_clean_info(
    tmp_path: Path,
) -> None:
    """File-only quality runs should not fail when no sidecar store exists."""
    write_ascii_case(tmp_path, CLEAN_M1_CASE)

    report = _provenance_report(tmp_path)

    finding = _finding(report, "PROVENANCE_STORE_UNAVAILABLE")
    manifest = report.metadata[PROVENANCE_METADATA_KEY]
    assert report.status is QualityStatus.CLEAN
    assert finding.severity is QualitySeverity.INFO
    assert manifest["available"] is False
    assert manifest["reason"] == "no sidecar manifest/status store found"


@dataclass(frozen=True, slots=True)
class _LineagePaths:
    csv: Path
    zip: Path
    cache: Path


def _write_lineage(
    tmp_path: Path,
    *,
    cache_sha256: str = "",
    cache_line_count: str = "3",
) -> _LineagePaths:
    csv = write_ascii_case(tmp_path, CLEAN_M1_CASE).resolve()
    archive = write_zip_case(tmp_path, CLEAN_M1_CASE).resolve()
    cache = (
        tmp_path / "ASCII" / "M1" / "eurusd" / "2012" / "02" / CACHE_FILENAME
    ).resolve()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(b"canonical-polars-cache-placeholder")

    item = WorkItem(
        work_id="work-eurusd-m1-201202",
        status=WorkStatus.CACHE_READY,
        data_dir=str(tmp_path),
        data_format="ascii",
        data_timeframe="M1",
        data_fxpair="EURUSD",
        data_datemonth="201202",
        zip_filename=archive.name,
        csv_filename=csv.name,
        cache_filename=CACHE_FILENAME,
        cache_line_count="3",
        cache_start="1328072400000",
        cache_end="1328072520000",
    )
    store = ManifestStatusStore(tmp_path)
    store.write_work_item(item, source="test", message="test work item")
    store.write_stage_result(
        StageResult(
            work_id=item.work_id,
            stage="build_cache",
            status=WorkStatus.CACHE_READY,
            artifacts=(
                _artifact("csv", csv),
                _artifact("zip", archive),
                _artifact(
                    "cache",
                    cache,
                    sha256=cache_sha256,
                    metadata={
                        "filename": CACHE_FILENAME,
                        "line_count": cache_line_count,
                        "start": "1328072400000",
                        "end": "1328072520000",
                        "work_id": item.work_id,
                    },
                ),
            ),
        )
    )
    return _LineagePaths(csv=csv, zip=archive, cache=cache)


def _artifact(
    kind: str,
    path: Path,
    *,
    sha256: str = "",
    metadata: dict | None = None,
) -> ArtifactRef:
    import hashlib

    encoded = path.read_bytes()
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=path.stat().st_size,
        sha256=sha256 or hashlib.sha256(encoded).hexdigest(),
        metadata=dict(metadata or {"filename": path.name}),
    )


def _provenance_report(path: Path):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("provenance",)),
        run_rules=quality_run_rules_for_groups(("provenance",)),
        metadata={
            PROVENANCE_METADATA_KEY: provenance_manifest_metadata(
                roots=discovery.roots,
            )
        },
    )


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]
