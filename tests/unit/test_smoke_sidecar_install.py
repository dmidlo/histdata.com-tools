"""Tests for installed-package runtime smoke helpers."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from histdatacom.orchestration.resources import TemporalExecutableUnavailable
from histdatacom.sidecar import live_smoke


def _module():
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "smoke_runtime_install.py"
    )
    spec = importlib.util.spec_from_file_location(
        "smoke_runtime_install",
        script_path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_install_wheel_installs_temporal_extra_with_direct_reference(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Runtime smoke should install the Temporal extra when requested."""
    module = _module()
    wheel = tmp_path / "histdatacom-0.0.0-py3-none-any.whl"
    wheel.touch()
    calls: list[list[str]] = []

    monkeypatch.setattr(
        module.subprocess,
        "check_call",
        lambda command: calls.append(command),
    )

    installed = module.install_wheel(
        wheel_path=wheel,
        install_temporal_extra=True,
    )

    assert installed == wheel
    assert calls == [
        [
            module.sys.executable,
            "-m",
            "pip",
            "install",
            f"histdatacom[temporal] @ {wheel.resolve().as_uri()}",
        ]
    ]


def test_check_live_runtime_smoke_returns_live_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Install smoke should expose the operator-gated live runtime check."""
    module = _module()
    captured: dict[str, Any] = {}

    class _Result:
        def to_dict(self) -> dict[str, str]:
            return {"status": "completed"}

    def fake_run_live_sidecar_smoke(**kwargs: Any) -> _Result:
        captured.update(kwargs)
        return _Result()

    monkeypatch.setattr(
        live_smoke,
        "run_live_sidecar_smoke",
        fake_run_live_sidecar_smoke,
    )

    report = module.check_live_runtime_smoke(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        temporal_executable=tmp_path / "temporal",
        startup_timeout=3.0,
        completion_timeout=4.0,
        stop_timeout=5.0,
    )

    assert report == {"status": "completed"}
    assert captured["workspace"] == tmp_path / "workspace"
    assert captured["runtime_home"] == tmp_path / "runtime"
    assert captured["data_directory"] == tmp_path / "data"
    assert captured["temporal_executable"] == tmp_path / "temporal"
    assert captured["startup_timeout"] == 3.0
    assert captured["completion_timeout"] == 4.0
    assert captured["stop_timeout"] == 5.0


def test_check_hermetic_runtime_smoke_returns_hermetic_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Install smoke should expose the release-gating hermetic runtime check."""
    module = _module()
    captured: dict[str, Any] = {}

    class _Result:
        def to_dict(self) -> dict[str, str]:
            return {"status": "completed"}

    def fake_run_hermetic_sidecar_smoke(**kwargs: Any) -> _Result:
        captured.update(kwargs)
        return _Result()

    monkeypatch.setattr(
        live_smoke,
        "run_hermetic_sidecar_smoke",
        fake_run_hermetic_sidecar_smoke,
    )

    report = module.check_hermetic_runtime_smoke(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        temporal_executable=tmp_path / "temporal",
        startup_timeout=3.0,
        completion_timeout=4.0,
        stop_timeout=5.0,
    )

    assert report == {"status": "completed"}
    assert captured["workspace"] == tmp_path / "workspace"
    assert captured["runtime_home"] == tmp_path / "runtime"
    assert captured["data_directory"] == tmp_path / "data"
    assert captured["temporal_executable"] == tmp_path / "temporal"
    assert captured["startup_timeout"] == 3.0
    assert captured["completion_timeout"] == 4.0
    assert captured["stop_timeout"] == 5.0


def test_check_default_routing_runtime_smoke_returns_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Install smoke should expose the default client-routing smoke."""
    module = _module()
    captured: dict[str, Any] = {}

    class _Result:
        def to_dict(self) -> dict[str, str]:
            return {"client_routing": "default_client_routing"}

    def fake_run_default_client_routing_sidecar_smoke(
        **kwargs: Any,
    ) -> _Result:
        captured.update(kwargs)
        return _Result()

    monkeypatch.setattr(
        live_smoke,
        "run_default_client_routing_sidecar_smoke",
        fake_run_default_client_routing_sidecar_smoke,
    )

    report = module.check_default_routing_runtime_smoke(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        temporal_executable=tmp_path / "temporal",
        startup_timeout=3.0,
        completion_timeout=4.0,
        stop_timeout=5.0,
    )

    assert report == {"client_routing": "default_client_routing"}
    assert captured["workspace"] == tmp_path / "workspace"
    assert captured["runtime_home"] == tmp_path / "runtime"
    assert captured["data_directory"] == tmp_path / "data"
    assert captured["temporal_executable"] == tmp_path / "temporal"
    assert captured["startup_timeout"] == 3.0
    assert captured["completion_timeout"] == 4.0
    assert captured["stop_timeout"] == 5.0


def test_check_quality_runtime_smoke_runs_installed_quality_cli(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Install smoke should run quality mode through installed scripts."""
    module = _module()
    commands: list[list[str]] = []
    run_envs: list[dict[str, str]] = []

    def fake_script_path(name: str) -> str:
        return f"/venv/bin/{name}"

    def fake_run(
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        expected_returncodes: tuple[int, ...] = (0,),
    ) -> SimpleNamespace:
        commands.append(command)
        run_envs.append(dict(env or {}))
        report_path = Path(command[command.index("--quality-report") + 1])
        target_path = Path(command[command.index("--quality-target") + 1])
        dirty = "BAD_NUMERIC" in target_path.name
        payload = _quality_report_payload(status="failed" if dirty else "clean")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload), encoding="utf-8")
        returncode = 1 if dirty else 0
        assert returncode in expected_returncodes
        return SimpleNamespace(
            returncode=returncode,
            stdout="Data quality assessment\n",
            stderr="",
        )

    def fake_run_json(
        command: list[str],
        *,
        env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        commands.append(command)
        run_envs.append(dict(env or {}))
        if "start" in command:
            return {"state": "running", "pids": {"server": 123}}
        if "stop" in command:
            return {"state": "stopped", "pids": {}}
        if "jobs" in command and "list" in command:
            return {
                "jobs": [
                    _quality_job(
                        status="COMPLETED",
                        target=tmp_path
                        / "data"
                        / "quality-smoke-fixtures"
                        / "DAT_ASCII_EURUSD_M1_201202.csv",
                        report=tmp_path
                        / "data"
                        / "quality-smoke-reports"
                        / "quality-clean.json",
                    ),
                    _quality_job(
                        status="FAILED",
                        target=tmp_path
                        / "data"
                        / "quality-smoke-fixtures"
                        / "DAT_ASCII_EURUSD_M1_201202_BAD_NUMERIC.csv",
                        report=tmp_path
                        / "data"
                        / "quality-smoke-reports"
                        / "quality-dirty.json",
                    ),
                ]
            }
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(module, "_script_path", fake_script_path)
    monkeypatch.setattr(module, "_run", fake_run)
    monkeypatch.setattr(module, "_run_json", fake_run_json)

    report = module.check_quality_runtime_smoke(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
        data_directory=tmp_path / "data",
        temporal_executable=tmp_path / "temporal",
        startup_timeout=3.0,
        stop_timeout=5.0,
    )

    assert report["start_state"] == "running"
    assert report["stop_state"] == "stopped"
    assert report["clean"]["returncode"] == 0
    assert report["clean"]["status"] == "clean"
    assert report["dirty"]["returncode"] == 1
    assert report["dirty"]["status"] == "failed"
    assert report["jobs"]["clean_status"] == "completed"
    assert report["jobs"]["dirty_status"] == "failed"
    assert any(command[0].endswith("histdatacom") for command in commands)
    assert any(
        command[:3] == ["/venv/bin/histdatacom", "runtime", "--workspace"]
        and "start" in command
        and "--executable" in command
        for command in commands
    )
    assert any("--no-orchestration-start" in command for command in commands)
    assert all(
        env.get("HISTDATACOM_RUNTIME_WORKSPACE") == str(tmp_path / "workspace")
        for env in run_envs
    )
    assert all(
        env.get("HISTDATACOM_RUNTIME_HOME") == str(tmp_path / "runtime")
        for env in run_envs
    )


def test_quality_runtime_smoke_rejects_shutdown_leaks() -> None:
    """Quality smoke should fail if runtime stop leaves live PIDs."""
    module = _module()

    try:
        module._validate_quality_runtime_stop(
            {"state": "stopped", "pids": {"worker": 123}}
        )
    except SystemExit as err:
        assert "left running processes" in str(err)
    else:  # pragma: no cover - defensive assertion shape
        raise AssertionError("expected shutdown leak to fail")


def test_check_runtime_resources_reports_external_runtime_resolution(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Release smoke should prove first-run Temporal resolver behavior."""
    import histdatacom.orchestration.resources as orchestration_resources

    module = _module()
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\necho temporal\n", encoding="utf-8")
    executable.chmod(0o755)
    resource = SimpleNamespace(bundled=False)
    artifact = SimpleNamespace(
        archive_name="temporal.tar.gz",
        archive_sha256="a" * 64,
        archive_size_bytes=1024,
    )
    manifest = SimpleNamespace(
        runtime="temporal",
        distribution_strategy="metadata-only",
        embedded_binary=False,
        platforms={"linux-x86_64": resource},
    )
    runtime_index = SimpleNamespace(
        version="1.7.2",
        platforms={"linux-x86_64": artifact},
    )
    resolution_payload = {
        "executable": str(executable),
        "source": "download",
        "platform": "linux-x86_64",
        "version": "1.7.2",
        "network_fetch": True,
    }
    resolution = SimpleNamespace(
        executable=executable,
        source="download",
        network_fetch=True,
        to_dict=lambda: resolution_payload,
    )
    cache_entry = SimpleNamespace(
        platform_key="linux-x86_64",
        to_dict=lambda: {"path": str(tmp_path / "cache"), "valid": True},
    )

    @contextmanager
    def missing_bundled_executable(*_: Any, **__: Any):
        raise TemporalExecutableUnavailable("not bundled in this distribution")
        yield  # pragma: no cover

    @contextmanager
    def resolved_runtime(**_: Any):
        yield resolution

    monkeypatch.setattr(
        orchestration_resources, "current_platform_key", lambda: "linux-x86_64"
    )
    monkeypatch.setattr(
        orchestration_resources, "load_runtime_manifest", lambda: manifest
    )
    monkeypatch.setattr(
        orchestration_resources,
        "load_temporal_runtime_index",
        lambda _manifest: runtime_index,
    )
    monkeypatch.setattr(
        orchestration_resources,
        "runtime_asset",
        lambda _asset: executable,
    )
    monkeypatch.setattr(
        orchestration_resources,
        "packaged_temporal_executable_path",
        missing_bundled_executable,
    )
    monkeypatch.setattr(
        orchestration_resources,
        "temporal_runtime_executable_path",
        resolved_runtime,
    )
    monkeypatch.setattr(
        orchestration_resources,
        "inspect_temporal_runtime_cache",
        lambda: (cache_entry,),
    )
    monkeypatch.setattr(
        module,
        "_run",
        lambda command: SimpleNamespace(
            returncode=0,
            stdout="temporal version 1.7.2\n",
            stderr="",
        ),
    )

    report = module.check_runtime_resources(
        require_external_runtime_provisioning=True,
        check_executable_version=True,
    )

    assert report["platform_bundled"] is False
    assert report["runtime_resolution"] == resolution_payload
    assert report["resolver_source"] == "download"
    assert report["resolver_network_fetch"] is True
    assert report["runtime_cache_entries"] == [
        {"path": str(tmp_path / "cache"), "valid": True}
    ]


def test_check_runtime_resources_rejects_explicit_runtime_for_external_preflight(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Production preflight should not pass through an explicit override."""
    import histdatacom.orchestration.resources as orchestration_resources

    module = _module()
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\necho temporal\n", encoding="utf-8")
    executable.chmod(0o755)
    manifest = SimpleNamespace(
        runtime="temporal",
        distribution_strategy="metadata-only",
        embedded_binary=False,
        platforms={"linux-x86_64": SimpleNamespace(bundled=False)},
    )
    runtime_index = SimpleNamespace(
        version="1.7.2",
        platforms={
            "linux-x86_64": SimpleNamespace(
                archive_name="temporal.tar.gz",
                archive_sha256="a" * 64,
                archive_size_bytes=1024,
            )
        },
    )
    resolution = SimpleNamespace(
        executable=executable,
        source="explicit",
        network_fetch=False,
        to_dict=lambda: {"source": "explicit"},
    )

    @contextmanager
    def missing_bundled_executable(*_: Any, **__: Any):
        raise TemporalExecutableUnavailable("not bundled in this distribution")
        yield  # pragma: no cover

    @contextmanager
    def resolved_runtime(**_: Any):
        yield resolution

    monkeypatch.setattr(
        orchestration_resources, "current_platform_key", lambda: "linux-x86_64"
    )
    monkeypatch.setattr(
        orchestration_resources, "load_runtime_manifest", lambda: manifest
    )
    monkeypatch.setattr(
        orchestration_resources,
        "load_temporal_runtime_index",
        lambda _manifest: runtime_index,
    )
    monkeypatch.setattr(
        orchestration_resources, "runtime_asset", lambda _asset: executable
    )
    monkeypatch.setattr(
        orchestration_resources,
        "packaged_temporal_executable_path",
        missing_bundled_executable,
    )
    monkeypatch.setattr(
        orchestration_resources,
        "temporal_runtime_executable_path",
        resolved_runtime,
    )
    monkeypatch.setattr(
        orchestration_resources, "inspect_temporal_runtime_cache", lambda: ()
    )
    monkeypatch.setattr(
        module,
        "_run",
        lambda command: SimpleNamespace(
            returncode=0,
            stdout="temporal version 1.7.2\n",
            stderr="",
        ),
    )

    try:
        module.check_runtime_resources(
            require_external_runtime_provisioning=True,
            check_executable_version=True,
            temporal_executable=executable,
        )
    except SystemExit as err:
        assert (
            "must resolve from the pinned cache or first-run download"
            in str(err)
        )
    else:  # pragma: no cover - defensive assertion shape
        raise AssertionError("expected explicit release runtime to fail")


def _quality_report_payload(*, status: str) -> dict[str, Any]:
    errors = 1 if status == "failed" else 0
    return {
        "schema_version": "histdatacom.quality-report.v1",
        "targets": [{"path": "/tmp/target.csv", "kind": "csv"}],
        "rule_results": [],
        "target_summaries": [
            {
                "target": {"path": "/tmp/target.csv", "kind": "csv"},
                "rule_count": 1,
                "finding_count": 1,
                "info_count": 0 if errors else 1,
                "warning_count": 0,
                "error_count": errors,
                "status": status,
                "max_severity": "error" if errors else "info",
            }
        ],
        "summary": {
            "target_count": 1,
            "rule_count": 1,
            "finding_count": 1,
            "info_count": 0 if errors else 1,
            "warning_count": 0,
            "error_count": errors,
            "status": status,
            "max_severity": "error" if errors else "info",
        },
        "metadata": {
            "operation": "data-quality",
            "check_groups": ["ingestion"],
        },
    }


def _quality_job(
    *,
    status: str,
    target: Path,
    report: Path,
) -> dict[str, Any]:
    return {
        "workflow_id": f"histdatacom-{target.stem}",
        "status": status,
        "artifacts": [{"kind": "quality-report", "path": str(report)}],
        "metadata": {
            "run_request": {
                "data_quality": True,
                "quality_paths": [str(target)],
                "quality_report_path": str(report),
            }
        },
    }


def test_check_package_metadata_requires_core_temporal_dependency(
    monkeypatch,
) -> None:
    """Base-install smoke should require temporalio as a core dependency."""
    import histdatacom

    module = _module()

    class _Metadata(dict):
        def get_all(
            self,
            key: str,
            default: list[str] | None = None,
        ) -> list[str]:
            value = self.get(key, default or [])
            return list(value) if isinstance(value, list) else [str(value)]

    class _Distribution:
        metadata = _Metadata(
            {
                "Name": "histdatacom",
                "Provides-Extra": ["temporal"],
            }
        )

    class _EntryPoints(list):
        def select(self, *, group: str) -> "_EntryPoints":
            return self

    monkeypatch.setattr(module, "_script_path", lambda name: name)
    monkeypatch.setattr(
        module.metadata,
        "distribution",
        lambda name: _Distribution(),
    )
    monkeypatch.setattr(
        module.metadata,
        "entry_points",
        lambda: _EntryPoints(
            SimpleNamespace(name=name, value=value)
            for name, value in module.EXPECTED_CONSOLE_SCRIPTS.items()
        ),
    )
    monkeypatch.setattr(
        module.metadata,
        "version",
        lambda name: (
            histdatacom.__version__ if name == "histdatacom" else "1.10.0"
        ),
    )
    monkeypatch.setattr(
        module.importlib.util, "find_spec", lambda name: object()
    )

    report = module.check_package_metadata(expect_temporal_extra=False)

    assert report["orchestration_contracts"] == ["RunRequest"]
    assert report["temporalio_version"] == "1.10.0"
