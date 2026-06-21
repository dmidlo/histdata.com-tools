"""Tests for installed-package sidecar smoke helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path


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
