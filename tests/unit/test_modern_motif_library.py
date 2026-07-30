"""Tests for the installed modern reference-motif library boundary."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path

import pytest

from histdatacom.data_analytics.cli import build_parser
from histdatacom.synthetic.motif_library import (
    MODERN_REFERENCE_MOTIF_COVERAGE_SCHEMA_VERSION,
    ModernReferenceMotifProfileV1,
    _coverage_axis_rates,
    read_modern_reference_motif_artifact,
)


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
