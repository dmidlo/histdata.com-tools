"""Tests for the row-free release-holdout evidence freezer."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest


def _load_freezer() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "freeze_release_holdout_evidence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "freeze_release_holdout_evidence", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("release-holdout freezer could not be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FREEZER = _load_freezer()


def _declaration_payload() -> dict[str, object]:
    return {
        "schema_version": "histdatacom.release-holdout-declaration.v1",
        "claim_scope": "v2.5-marked-hawkes-release-decision-successor-2",
        "resource_bounds": {
            "max_source_bytes": 4 * 1024**3,
            "max_runtime_seconds": 3_600.0,
            "max_peak_memory_bytes": 2 * 1024**3,
        },
        "split_periods": {
            "calibration": "202512",
            "validation": "202601",
            "final_holdout": "202607",
        },
        "windows": {
            "calibration": [
                {"start_utc": "2025-12-03T00:00:00Z", "session": "asia"},
                {
                    "start_utc": "2025-12-12T07:00:00Z",
                    "session": "london",
                },
                {
                    "start_utc": "2025-12-10T19:00:00Z",
                    "session": "new_york",
                },
                {
                    "start_utc": "2025-12-23T16:00:00Z",
                    "session": "overlap_closure",
                },
            ],
            "validation": [
                {"start_utc": "2026-01-05T00:00:00Z", "session": "asia"},
                {
                    "start_utc": "2026-01-15T07:00:00Z",
                    "session": "london",
                },
                {
                    "start_utc": "2026-01-28T19:00:00Z",
                    "session": "new_york",
                },
                {
                    "start_utc": "2026-01-22T16:00:00Z",
                    "session": "overlap_closure",
                },
            ],
            "final_holdout": [
                {"start_utc": "2026-07-03T00:00:00Z", "session": "asia"},
                {
                    "start_utc": "2026-07-10T08:00:00Z",
                    "session": "london",
                },
                {
                    "start_utc": "2026-07-29T18:00:00Z",
                    "session": "new_york",
                },
                {
                    "start_utc": "2026-07-20T16:00:00Z",
                    "session": "overlap_closure",
                },
            ],
        },
    }


def _write_declaration(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "declaration.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_load_declaration_preserves_issue_512_default() -> None:
    split_periods, windows, claim_scope, resources = FREEZER._load_declaration(
        None
    )

    assert split_periods["final_holdout"] == "202606"
    assert windows["final_holdout"][0] == (
        "2026-06-02T00:00:00Z",
        "asia",
    )
    assert claim_scope.endswith("successor-1")
    assert resources["max_source_bytes"] == 4 * 1024**3


def test_load_declaration_accepts_strict_successor_file(tmp_path: Path) -> None:
    path = _write_declaration(tmp_path, _declaration_payload())

    split_periods, windows, claim_scope, resources = FREEZER._load_declaration(
        path
    )

    assert split_periods["final_holdout"] == "202607"
    assert {session for _, session in windows["final_holdout"]} == {
        "asia",
        "london",
        "new_york",
        "overlap_closure",
    }
    assert claim_scope.endswith("successor-2")
    assert resources == {
        "max_source_bytes": 4 * 1024**3,
        "max_runtime_seconds": 3_600.0,
        "max_peak_memory_bytes": 2 * 1024**3,
    }


def test_load_declaration_rejects_window_outside_split(tmp_path: Path) -> None:
    payload = _declaration_payload()
    windows = payload["windows"]
    assert isinstance(windows, dict)
    final_holdout = windows["final_holdout"]
    assert isinstance(final_holdout, list)
    final_holdout[0] = {
        "start_utc": "2026-06-30T00:00:00Z",
        "session": "asia",
    }

    with pytest.raises(ValueError, match="outside its period"):
        FREEZER._load_declaration(_write_declaration(tmp_path, payload))


def test_load_declaration_rejects_missing_session_axis(tmp_path: Path) -> None:
    payload = _declaration_payload()
    windows = payload["windows"]
    assert isinstance(windows, dict)
    final_holdout = windows["final_holdout"]
    assert isinstance(final_holdout, list)
    final_holdout[-1] = {
        "start_utc": "2026-07-20T16:00:00Z",
        "session": "new_york",
    }

    with pytest.raises(ValueError, match="each required session exactly once"):
        FREEZER._load_declaration(_write_declaration(tmp_path, payload))


def test_development_cutoff_accepts_normalized_profile_periods() -> None:
    cutoff_ns = FREEZER._development_source_cutoff_ns(
        {
            "calibration": ("202512",),
            "validation": ("202601",),
            "final_holdout": ("202607",),
        }
    )

    assert (
        cutoff_ns
        == int(datetime(2026, 2, 1, tzinfo=timezone.utc).timestamp())
        * 1_000_000_000
    )
