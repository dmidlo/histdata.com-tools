"""Tests for offline data-quality target discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.data_quality import (
    QualityDiscoveryError,
    QualityDiscoveryResult,
    QualityTargetKind,
    discover_quality_targets,
    normalize_quality_check_groups,
    quality_target_from_path,
)
from histdatacom.histdata_ascii import CACHE_FILENAME, M1
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    write_ascii_case,
    write_zip_case,
)


def test_quality_discovery_recursively_finds_zip_csv_and_cache_targets(
    tmp_path: Path,
) -> None:
    """Directory discovery should find supported local targets recursively."""
    nested = tmp_path / "nested"
    csv_path = write_ascii_case(nested, CLEAN_M1_CASE)
    zip_path = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )
    cache_path = nested / CACHE_FILENAME
    cache_path.write_bytes(b"placeholder cache bytes")
    (nested / "README.txt").write_text("not a target", encoding="utf-8")

    result = discover_quality_targets((tmp_path,))

    assert result.roots == (str(tmp_path),)
    assert result.target_count == 3
    assert {
        (Path(target.path).name, target.kind) for target in result.targets
    } == {
        (csv_path.name, QualityTargetKind.CSV),
        (zip_path.name, QualityTargetKind.ZIP),
        (cache_path.name, QualityTargetKind.CACHE),
    }
    csv_target = next(
        target
        for target in result.targets
        if target.kind == QualityTargetKind.CSV
    )
    zip_target = next(
        target
        for target in result.targets
        if target.kind == QualityTargetKind.ZIP
    )
    assert csv_target.data_format == "ascii"
    assert csv_target.symbol == "EURUSD"
    assert csv_target.timeframe == M1
    assert csv_target.period == "201202"
    assert zip_target.data_format == "ascii"
    assert zip_target.symbol == "EURUSD"
    assert zip_target.timeframe == M1
    assert zip_target.period == "201202"
    assert (
        QualityDiscoveryResult.from_dict(result.to_dict()).to_dict()
        == result.to_dict()
    )


@pytest.mark.parametrize(
    ("filename", "kind"),
    (
        ("DAT_ASCII_EURUSD_M1_201202.csv", QualityTargetKind.CSV),
        ("DAT_ASCII_EURUSD_M1_201202.zip", QualityTargetKind.ZIP),
        (CACHE_FILENAME, QualityTargetKind.CACHE),
    ),
)
def test_quality_target_from_path_classifies_supported_files(
    tmp_path: Path,
    filename: str,
    kind: QualityTargetKind,
) -> None:
    """Supported files should map to QualityTarget objects."""
    path = tmp_path / filename
    path.write_bytes(b"")

    target = quality_target_from_path(path)

    assert target is not None
    assert target.kind == kind
    assert target.path == str(path.resolve())


def test_quality_discovery_rejects_missing_and_unsupported_paths(
    tmp_path: Path,
) -> None:
    """Operators should get clear setup errors for invalid roots."""
    with pytest.raises(QualityDiscoveryError, match="does not exist"):
        discover_quality_targets((tmp_path / "missing",))

    unsupported = tmp_path / "notes.txt"
    unsupported.write_text("not a quality target", encoding="utf-8")
    with pytest.raises(QualityDiscoveryError, match="unsupported"):
        discover_quality_targets((unsupported,))


def test_quality_discovery_allows_empty_directories(tmp_path: Path) -> None:
    """An empty local directory should produce an empty discovery result."""
    result = discover_quality_targets((tmp_path,))

    assert result.target_count == 0
    assert result.targets == ()


def test_quality_check_groups_normalize_and_validate_operator_selection() -> (
    None
):
    """CLI/API check group selections should be deterministic."""
    assert normalize_quality_check_groups(None) == ("all",)
    assert normalize_quality_check_groups(("inventory", "time", "time")) == (
        "inventory",
        "time",
    )

    with pytest.raises(QualityDiscoveryError, match="cannot be combined"):
        normalize_quality_check_groups(("all", "inventory"))
    with pytest.raises(QualityDiscoveryError, match="unsupported"):
        normalize_quality_check_groups(("bad-group",))
