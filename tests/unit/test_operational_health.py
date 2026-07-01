"""Tests for bounded operational health payloads."""

from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

from histdatacom.operational_health import (
    OPERATIONAL_HEALTH_METADATA_KEY,
    attach_operational_health_from_snapshot,
)
from histdatacom.runtime_contracts import RunRequest
from histdatacom.orchestration.control import OrchestrationJobSnapshot


class _Handle:
    """Minimal handle shape for snapshot metadata tests."""

    request_id = "run-health"
    workflow_id = "histdatacom-run-health"
    run_id = "run-fake"
    task_queue = "histdatacom.test.orchestration"
    namespace = "default"


def test_operational_health_attachment_is_path_free(
    tmp_path: Path,
) -> None:
    """Request-scoped health should expose counts and states, not roots."""
    data_dir = tmp_path / "private" / "data"
    target_dir = data_dir / "ASCII" / "T" / "eurusd" / "2024" / "1"
    target_dir.mkdir(parents=True)
    (target_dir / ".data").write_bytes(b"cache")
    (target_dir / "DAT_ASCII_EURUSD_T_202401.csv").write_bytes(b"source")
    request = RunRequest(
        request_id="run-health",
        pairs=("eurusd",),
        formats=("ASCII",),
        timeframes=("T",),
        data_directory=str(data_dir),
    )
    snapshot = replace(
        OrchestrationJobSnapshot.from_handle(_Handle()),
        metadata={
            "run_request": request.to_dict(),
            "runtime_health": {
                "state": "running",
                "pid_count": 1,
                "component_count": 1,
                "components": {"server": {"state": "running", "pid": 123}},
            },
        },
    )

    enriched = attach_operational_health_from_snapshot(snapshot)
    health = enriched.metadata[OPERATIONAL_HEALTH_METADATA_KEY]

    assert isinstance(health, dict)
    assert health["status"] == "active"
    assert health["summary"]["cache_count"] == 1
    assert health["summary"]["source_artifact_count"] == 1
    assert health["runtime"]["components"]["server"]["pid"] == 123
    serialized = json.dumps(health, sort_keys=True)
    assert str(tmp_path) not in serialized
