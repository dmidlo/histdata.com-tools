"""Tests for installed-package sidecar smoke helpers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from histdatacom.sidecar import live_smoke


def _module():
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "smoke_sidecar_install.py"
    )
    spec = importlib.util.spec_from_file_location(
        "smoke_sidecar_install",
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
    """Starting the sidecar smoke should install the Temporal extra."""
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


def test_check_live_sidecar_smoke_returns_live_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Install smoke should expose the operator-gated live sidecar check."""
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

    report = module.check_live_sidecar_smoke(
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


def test_check_missing_temporal_extra_failure_requires_clean_json_error(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Base-install smoke should exercise missing temporalio startup behavior."""
    module = _module()
    captured: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any) -> object:
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(
            returncode=1,
            stdout=json.dumps(
                {
                    "state": "error",
                    "message": (
                        "Temporal worker support requires "
                        "histdatacom[temporal]. Install the Temporal extra."
                    ),
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(module, "_script_path", lambda name: name)
    monkeypatch.setattr(module.subprocess, "run", fake_run)

    payload = module.check_missing_temporal_extra_failure(tmp_path / "state")

    assert payload["state"] == "error"
    assert payload["exit_code"] == 1
    assert "histdatacom[temporal]" in payload["message"]
    assert captured["command"][:5] == [
        "histdatacom-sidecar",
        "--state-dir",
        str(tmp_path / "state"),
        "--json",
        "start",
    ]
