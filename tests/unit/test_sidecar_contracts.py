"""Tests for the public sidecar contract import surface."""

from __future__ import annotations

import importlib
import sys

import pytest

from histdatacom import runtime_contracts

_IMPLEMENTATION_MODULES = (
    "histdatacom.sidecar.activities",
    "histdatacom.sidecar.client",
    "histdatacom.sidecar.workflows",
    "histdatacom.sidecar.worker",
)


@pytest.fixture
def unloaded_sidecar_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear sidecar modules whose accidental import would hide regressions."""
    for module_name in (
        "histdatacom.sidecar.contracts",
        "histdatacom.sidecar",
        *_IMPLEMENTATION_MODULES,
    ):
        monkeypatch.delitem(sys.modules, module_name, raising=False)


def test_sidecar_contracts_reexport_runtime_contracts_without_worker_imports(
    unloaded_sidecar_modules: None,
) -> None:
    """Documented sidecar contract imports should stay lightweight."""
    contracts = importlib.import_module("histdatacom.sidecar.contracts")

    assert contracts.ArtifactRef is runtime_contracts.ArtifactRef
    assert contracts.FailureInfo is runtime_contracts.FailureInfo
    assert contracts.JSONScalar is runtime_contracts.JSONScalar
    assert contracts.JSONValue is runtime_contracts.JSONValue
    assert contracts.RunRequest is runtime_contracts.RunRequest
    assert contracts.StageResult is runtime_contracts.StageResult
    assert contracts.StatusEvent is runtime_contracts.StatusEvent
    assert contracts.WorkItem is runtime_contracts.WorkItem
    assert contracts.WorkStatus is runtime_contracts.WorkStatus
    assert contracts.derive_work_id is runtime_contracts.derive_work_id
    assert contracts.new_request_id is runtime_contracts.new_request_id
    assert (
        contracts.status_has_csv_artifact
        is runtime_contracts.status_has_csv_artifact
    )
    assert not set(_IMPLEMENTATION_MODULES).intersection(sys.modules)


def test_sidecar_contracts_run_request_round_trip(
    unloaded_sidecar_modules: None,
) -> None:
    """The public contract path should preserve RunRequest serialization."""
    contracts = importlib.import_module("histdatacom.sidecar.contracts")
    request = contracts.RunRequest(
        request_id="run-contract",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("1-minute-bar-quotes",),
        start_yearmonth="202201",
        end_yearmonth="202202",
        data_directory="data",
        api_return_type="polars",
        validate_urls=True,
        metadata={"source": "test"},
    )

    restored = contracts.RunRequest.from_dict(request.to_dict())

    assert restored == request
    assert restored.pairs == ("eurusd",)
    assert restored.metadata == {"source": "test"}


def test_sidecar_package_lazy_exports_contracts_without_worker_imports(
    unloaded_sidecar_modules: None,
) -> None:
    """The sidecar package should expose contracts without worker imports."""
    sidecar = importlib.import_module("histdatacom.sidecar")

    assert sidecar.RunRequest is runtime_contracts.RunRequest
    assert sidecar.WorkStatus is runtime_contracts.WorkStatus
    assert "RunRequest" in sidecar.__all__
    assert not set(_IMPLEMENTATION_MODULES).intersection(sys.modules)
