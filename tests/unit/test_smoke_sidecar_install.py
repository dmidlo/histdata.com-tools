"""Tests for installed-package sidecar smoke helpers."""

from __future__ import annotations

import importlib.util
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

    assert report["temporalio_version"] == "1.10.0"
