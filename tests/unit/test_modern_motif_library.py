"""Tests for the installed modern reference-motif library boundary."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.data_analytics import cli as analytics_cli
from histdatacom.data_analytics.cli import build_parser
from histdatacom.synthetic.motif_library import (
    MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION,
    ModernReferenceMotifProfileV1,
    _coverage_axis_rates,
    read_modern_reference_motif_artifact,
)
from histdatacom.synthetic.motifs import reference_session_for_ns


def test_profile_round_trip_is_strict_and_chronological() -> None:
    profile = ModernReferenceMotifProfileV1()

    assert ModernReferenceMotifProfileV1.from_dict(profile.to_dict()) == profile
    canonical_payload = json.loads(
        json.dumps(profile.to_dict(), sort_keys=True, separators=(",", ":"))
    )
    assert ModernReferenceMotifProfileV1.from_dict(canonical_payload) == profile
    assert profile.split_periods["train"][0] == "201901"
    assert profile.split_periods["final_holdout"] == ("202510",)
    assert profile.max_fragments == 256
    assert profile.max_matches == 64

    with pytest.raises(ValueError, match="overlap or regress"):
        ModernReferenceMotifProfileV1(
            split_periods={
                "train": ("202401",),
                "calibration": ("202307",),
                "validation": ("202501",),
                "final_holdout": ("202510",),
            }
        )


@pytest.mark.parametrize(
    ("hour", "expected"),
    (
        (0, "asia"),
        (6, "asia"),
        (7, "london"),
        (11, "london"),
        (12, "new_york"),
        (23, "new_york"),
    ),
)
def test_reference_session_coordinate_matches_frozen_model_windows(
    hour: int, expected: str
) -> None:
    assert reference_session_for_ns(hour * 3_600_000_000_000) == expected


def test_content_addressed_companion_reader_rejects_tampering(
    tmp_path: Path,
) -> None:
    payload = {
        "schema_version": MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION,
        "index_id": "reference-motif-index:sha256:" + "0" * 64,
    }
    content = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    path = tmp_path / f"modern-reference-motif-coverage-{digest}.json"
    path.write_bytes(content)

    assert (
        read_modern_reference_motif_artifact(path, kind="coverage") == payload
    )
    path.write_bytes(content + b" ")
    with pytest.raises(ValueError, match="content hash differs"):
        read_modern_reference_motif_artifact(path, kind="coverage")


def test_coverage_axis_rates_keep_support_refusal_and_backoff_visible() -> None:
    summary = _coverage_axis_rates(
        Counter(
            {
                "query_count": 4,
                "status:matched": 3,
                "status:no_supported_cell": 1,
                "backoff:exact": 1,
                "backoff:symbol_epoch_session": 2,
            }
        )
    )

    assert summary == {
        "query_count": 4,
        "status_counts": {"matched": 3, "no_supported_cell": 1},
        "backoff_counts": {"exact": 1, "symbol_epoch_session": 2},
        "backoff_rates": {"exact": 0.25, "symbol_epoch_session": 0.5},
    }


def test_cli_exposes_installed_modern_motif_library_command() -> None:
    args = build_parser().parse_args(
        [
            "modern-reference-motif-library",
            "--source-root",
            "ticks",
            "--definition",
            "epochs.json",
            "--market-context-corpus",
            "context.json",
            "--cftc-positioning-corpus",
            "positioning.json",
            "--benchmark-manifest",
            "benchmark.json",
            "--artifact-dir",
            "artifacts",
        ]
    )

    assert args.analytics_command == "modern-reference-motif-library"
    assert args.max_fragments == 256
    assert args.max_matches == 64
    assert args.train_periods == (
        "201901",
        "202001",
        "202101",
        "202201",
        "202301",
    )


def test_modern_motif_cli_reports_failed_candidate_without_crashing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact = SimpleNamespace(
        path=tmp_path / "manifest.json",
        to_dict=lambda: {"path": str(tmp_path / "manifest.json")},
    )
    build = SimpleNamespace(
        manifest={"schema_version": "test"},
        library_id="modern-reference-motif:test",
        index=SimpleNamespace(index_id="index:test", fragments=(object(),)),
        qualification={
            "candidate_promotion_eligible": False,
            "real_window_contracts": {"source_replay_verified": True},
        },
    )
    monkeypatch.setattr(
        analytics_cli,
        "build_modern_reference_motif_library",
        lambda *_, **__: build,
    )
    monkeypatch.setattr(
        analytics_cli,
        "write_modern_reference_motif_artifacts",
        lambda *_, **__: {"manifest": artifact, "qualification": artifact},
    )

    result = analytics_cli.main(
        [
            "modern-reference-motif-library",
            "--source-root",
            "ticks",
            "--definition",
            "epochs.json",
            "--market-context-corpus",
            "context.json",
            "--cftc-positioning-corpus",
            "positioning.json",
            "--benchmark-manifest",
            "benchmark.json",
            "--artifact-dir",
            str(tmp_path),
        ]
    )

    assert result == 2
    assert "candidate gate: fail" in capsys.readouterr().out
