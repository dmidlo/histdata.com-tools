"""Tests for the public orchestration contract import surface."""

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
def unloaded_orchestration_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear modules whose accidental import would hide regressions."""
    for module_name in (
        "histdatacom.orchestration.contracts",
        "histdatacom.orchestration",
        *_IMPLEMENTATION_MODULES,
    ):
        monkeypatch.delitem(sys.modules, module_name, raising=False)


def test_orchestration_contracts_reexport_runtime_contracts_without_worker_imports(
    unloaded_orchestration_modules: None,
) -> None:
    """Documented orchestration contract imports should stay lightweight."""
    contracts = importlib.import_module("histdatacom.orchestration.contracts")

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


def test_orchestration_contracts_run_request_round_trip(
    unloaded_orchestration_modules: None,
) -> None:
    """The public contract path should preserve RunRequest serialization."""
    contracts = importlib.import_module("histdatacom.orchestration.contracts")
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


def test_orchestration_package_exports_contracts_without_worker_imports(
    unloaded_orchestration_modules: None,
) -> None:
    """The orchestration package should expose contracts without worker imports."""
    orchestration = importlib.import_module("histdatacom.orchestration")

    assert orchestration.RunRequest is runtime_contracts.RunRequest
    assert orchestration.WorkStatus is runtime_contracts.WorkStatus
    assert "RunRequest" in orchestration.__all__
    assert not set(_IMPLEMENTATION_MODULES).intersection(sys.modules)


def test_orchestration_client_facade_exposes_public_job_helpers() -> None:
    """The public client facade should not require sidecar-named imports."""
    orchestration_client = importlib.import_module(
        "histdatacom.orchestration.client"
    )
    implementation_client = importlib.import_module(
        "histdatacom.sidecar.client"
    )

    assert (
        orchestration_client.JobHandle is implementation_client.SidecarJobHandle
    )
    assert (
        orchestration_client.JobResult is implementation_client.SidecarJobResult
    )
    assert (
        orchestration_client.OrchestrationUnavailableError
        is implementation_client.SidecarUnavailableError
    )
    assert (
        orchestration_client.submit_run_request_and_observe_sync
        is implementation_client.submit_run_request_and_observe_sync
    )
    assert "SidecarJobHandle" not in orchestration_client.__all__
    assert "SidecarUnavailableError" not in orchestration_client.__all__


def test_orchestration_resources_facade_exposes_runtime_resolver() -> None:
    """Runtime provisioning should be available from orchestration resources."""
    orchestration_resources = importlib.import_module(
        "histdatacom.orchestration.resources"
    )
    implementation_resources = importlib.import_module(
        "histdatacom.sidecar.resources"
    )

    assert (
        orchestration_resources.load_runtime_manifest
        is implementation_resources.load_sidecar_manifest
    )
    assert (
        orchestration_resources.runtime_asset
        is implementation_resources.sidecar_asset
    )
    assert (
        orchestration_resources.TemporalExecutableUnavailable
        is implementation_resources.SidecarExecutableUnavailable
    )
    assert "sidecar_asset" not in orchestration_resources.__all__
