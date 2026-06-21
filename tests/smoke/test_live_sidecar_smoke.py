"""Operator-gated live Temporal sidecar end-to-end smoke test."""

from __future__ import annotations

from pathlib import Path

import pytest

from histdatacom.runtime_contracts import WorkStatus
from histdatacom.sidecar.control import JobLifecycle
from histdatacom.sidecar.live_smoke import (
    LiveSidecarSmokeError,
    diagnostics_json,
    live_sidecar_smoke_skip_reason,
    run_live_sidecar_smoke,
)


@pytest.mark.live_sidecar
def test_live_sidecar_smoke_completes_minimal_non_influx_job(
    tmp_path: Path,
) -> None:
    """Run only when an operator provides or packages Temporal."""
    skip_reason = live_sidecar_smoke_skip_reason()
    if skip_reason:
        pytest.skip(skip_reason)

    try:
        result = run_live_sidecar_smoke(
            workspace=tmp_path / "workspace",
            runtime_home=tmp_path / "runtime",
            data_directory=tmp_path / "data",
        )
    except LiveSidecarSmokeError as err:
        pytest.fail(diagnostics_json(err.diagnostics))

    assert result.snapshot.lifecycle == JobLifecycle.SUCCEEDED
    assert result.snapshot.status == WorkStatus.COMPLETED
    assert result.snapshot.artifacts
    assert result.started_status.running
    assert result.stopped_status is not None
    assert result.stopped_status.state in {"stopped", "stopping"}
