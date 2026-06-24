"""Tests for the TestPyPI installed-package parity harness."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


def _module() -> ModuleType:
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "verify_testpypi_install.py"
    )
    spec = importlib.util.spec_from_file_location(
        "verify_testpypi_install",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_download_testpypi_wheel_uses_no_deps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The harness should prove the artifact came from TestPyPI only."""
    module = _module()
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> SimpleNamespace:
        commands.append(command)
        download_dir = Path(command[command.index("--dest") + 1])
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / "histdatacom-0.79.0-py3-none-any.whl").touch()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module, "_run", fake_run)

    wheel = module._download_testpypi_wheel(
        venv_python=tmp_path / "venv" / "bin" / "python",
        download_dir=tmp_path / "downloads",
        version="0.79.0",
        index_url="https://test.pypi.org/simple/",
        timeout=30.0,
    )

    assert wheel.name == "histdatacom-0.79.0-py3-none-any.whl"
    assert commands == [
        [
            str(tmp_path / "venv" / "bin" / "python"),
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--no-deps",
            "--index-url",
            "https://test.pypi.org/simple/",
            "--dest",
            str(tmp_path / "downloads"),
            "histdatacom==0.79.0",
        ]
    ]


def test_install_wheel_resolves_dependencies_from_pypi(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dependencies should not be resolved from TestPyPI shadow packages."""
    module = _module()
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> SimpleNamespace:
        commands.append(command)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(module, "_run", fake_run)

    module._install_wheel(
        venv_python=tmp_path / "venv" / "bin" / "python",
        wheel=tmp_path / "downloads" / "histdatacom-0.79.0-py3-none-any.whl",
        dependency_index_url="https://pypi.org/simple/",
        timeout=30.0,
    )

    assert commands == [
        [
            str(tmp_path / "venv" / "bin" / "python"),
            "-m",
            "pip",
            "install",
            "--index-url",
            "https://pypi.org/simple/",
            str(tmp_path / "downloads" / "histdatacom-0.79.0-py3-none-any.whl"),
        ]
    ]


def test_download_smoke_uses_bounded_historical_m1_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Download parity should not depend on current-month tick volume."""
    module = _module()
    commands: list[list[str]] = []
    json_commands: list[list[str]] = []
    captured_envs: list[dict[str, str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> SimpleNamespace:
        commands.append(command)
        captured_envs.append(kwargs["env"])
        data_dir = Path(command[command.index("--data-directory") + 1])
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "HISTDATA_COM_ASCII_EURUSD_M1202201.zip").touch()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_run_json(command: list[str], **kwargs: Any) -> dict[str, str]:
        json_commands.append(command)
        captured_envs.append(kwargs["env"])
        return {"state": "stopped"}

    monkeypatch.setattr(module, "_run", fake_run)
    monkeypatch.setattr(module, "_run_json", fake_run_json)

    report = module._download_smoke_probe(
        venv_dir=tmp_path / "venv",
        root=tmp_path,
        timeout=30.0,
    )

    command = commands[0]
    assert "-t" in command
    assert command[command.index("-t") + 1] == "1-minute-bar-quotes"
    assert command[command.index("-s") + 1] == "202201"
    assert command[command.index("-e") + 1] == "202202"
    assert "tick-data-quotes" not in command
    assert "now" not in command
    assert json_commands == [
        [
            str(tmp_path / "venv" / "bin" / "histdatacom"),
            "runtime",
            "--json",
            "stop",
        ]
    ]
    assert captured_envs[0]["VIRTUAL_ENV"] == str(tmp_path / "venv")
    assert captured_envs[0]["HISTDATACOM_RUNTIME_HOME"] == str(
        tmp_path / "download-smoke-runtime"
    )
    assert captured_envs[0]["HISTDATACOM_RUNTIME_WORKSPACE"] == str(
        tmp_path / "download-smoke-runtime-workspace"
    )
    assert captured_envs[0]["HISTDATACOM_TEMPORAL_CACHE_DIR"] == str(
        tmp_path / "temporal-runtime-cache"
    )
    assert captured_envs[1] == captured_envs[0]
    assert report["files"] == ["HISTDATA_COM_ASCII_EURUSD_M1202201.zip"]
    assert report["runtime_stop"] == {"state": "stopped"}


def test_release_verification_environment_isolates_runtime_overrides(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Release parity should not be satisfied by host runtime overrides."""
    module = _module()
    monkeypatch.setenv("HISTDATACOM_TEMPORAL_EXECUTABLE", "/tmp/temporal")
    monkeypatch.setenv("HISTDATACOM_TEMPORAL_OFFLINE", "1")
    monkeypatch.setenv("HISTDATACOM_RUNTIME_HOME", "/tmp/runtime")
    monkeypatch.setenv("HISTDATACOM_RUNTIME_WORKSPACE", "/tmp/workspace")

    env = module._release_verification_environment(
        venv_dir=tmp_path / "venv",
        root=tmp_path,
    )

    assert "HISTDATACOM_TEMPORAL_EXECUTABLE" not in env
    assert "HISTDATACOM_TEMPORAL_OFFLINE" not in env
    assert "HISTDATACOM_RUNTIME_HOME" not in env
    assert "HISTDATACOM_RUNTIME_WORKSPACE" not in env
    assert env["HISTDATACOM_TEMPORAL_CACHE_DIR"] == str(
        tmp_path / "temporal-runtime-cache"
    )


def test_cli_parity_probe_requires_current_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The verifier should reject stale artifacts with old CLI help."""
    module = _module()

    def fake_run(command: list[str], **_: Any) -> SimpleNamespace:
        executable = Path(command[0]).name
        if executable == "histdatacom" and "--version" in command:
            return SimpleNamespace(returncode=0, stdout="0.79.0\n", stderr="")
        if executable == "histdatacom" and "-h" in command:
            return SimpleNamespace(
                returncode=0,
                stdout=" ".join(module.EXPECTED_HELP_TOKENS),
                stderr="",
            )
        if executable == "histdatacom" and "runtime" in command:
            return SimpleNamespace(
                returncode=0,
                stdout=" ".join(module.EXPECTED_RUNTIME_COMMANDS),
                stderr="",
            )
        return SimpleNamespace(returncode=0, stdout="worker help", stderr="")

    monkeypatch.setattr(module, "_run", fake_run)

    report = module._cli_parity_probe(
        venv_dir=tmp_path / "venv",
        version="0.79.0",
        timeout=30.0,
    )

    assert report["version"] == "0.79.0"
    assert "--quality" in report["required_help_tokens"]
    assert "doctor" in report["runtime_commands"]


def test_cli_parity_probe_fails_on_stale_help(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A TestPyPI wheel missing orchestration/data-quality flags should fail."""
    module = _module()

    def fake_run(command: list[str], **_: Any) -> SimpleNamespace:
        executable = Path(command[0]).name
        if executable == "histdatacom" and "--version" in command:
            return SimpleNamespace(returncode=0, stdout="0.79.0\n", stderr="")
        if executable == "histdatacom" and "-h" in command:
            return SimpleNamespace(returncode=0, stdout="old help", stderr="")
        return SimpleNamespace(returncode=0, stdout="status doctor", stderr="")

    monkeypatch.setattr(module, "_run", fake_run)

    with pytest.raises(SystemExit, match="missing current flags"):
        module._cli_parity_probe(
            venv_dir=tmp_path / "venv",
            version="0.79.0",
            timeout=30.0,
        )


def test_smoke_runtime_install_probe_passes_strong_flags(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The harness should be able to require full installed orchestration parity."""
    module = _module()
    commands: list[list[str]] = []
    args = SimpleNamespace(
        timeout=30.0,
        live_startup_timeout=3.0,
        live_completion_timeout=4.0,
        live_stop_timeout=5.0,
        require_bundled_current_platform=True,
        require_external_runtime_provisioning=True,
        check_executable_version=True,
        temporal_executable=None,
        start_runtime=True,
        hermetic_runtime_smoke=True,
        default_routing_runtime_smoke=True,
        quality_runtime_smoke=True,
        live_runtime_smoke=True,
    )

    def fake_run_json(command: list[str], **_: Any) -> dict[str, str]:
        commands.append(command)
        return {"ok": "true"}

    monkeypatch.setattr(module, "_run_json", fake_run_json)

    report = module._smoke_runtime_install_probe(
        venv_python=tmp_path / "venv" / "bin" / "python",
        venv_dir=tmp_path / "venv",
        root=tmp_path,
        args=args,
    )

    assert report == {"ok": "true"}
    command = commands[0]
    assert "--require-bundled-current-platform" in command
    assert "--require-external-runtime-provisioning" in command
    assert "--check-executable-version" in command
    assert "--start-runtime" in command
    assert "--hermetic-runtime-smoke" in command
    assert "--default-routing-runtime-smoke" in command
    assert "--quality-runtime-smoke" in command
    assert "--live-runtime-smoke" in command


def test_smoke_runtime_install_probe_exposes_venv_scripts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The smoke script should see console entry points without activation."""
    module = _module()
    captured_env: dict[str, str] = {}
    args = SimpleNamespace(
        timeout=30.0,
        live_startup_timeout=3.0,
        live_completion_timeout=4.0,
        live_stop_timeout=5.0,
        require_bundled_current_platform=False,
        require_external_runtime_provisioning=False,
        check_executable_version=False,
        temporal_executable=tmp_path / "temporal",
        start_runtime=False,
        hermetic_runtime_smoke=False,
        default_routing_runtime_smoke=False,
        quality_runtime_smoke=False,
        live_runtime_smoke=False,
    )

    def fake_run_json(command: list[str], **kwargs: Any) -> dict[str, str]:
        captured_env.update(kwargs["env"])
        return {"ok": "true"}

    monkeypatch.setattr(module, "_run_json", fake_run_json)

    module._smoke_runtime_install_probe(
        venv_python=tmp_path / "venv" / "bin" / "python",
        venv_dir=tmp_path / "venv",
        root=tmp_path,
        args=args,
    )

    assert captured_env["VIRTUAL_ENV"] == str(tmp_path / "venv")
    assert captured_env["PATH"].split(":")[0] == str(tmp_path / "venv" / "bin")


def test_smoke_runtime_install_probe_accepts_explicit_developer_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Developers should be able to pass a local Temporal executable explicitly."""
    module = _module()
    commands: list[list[str]] = []
    args = SimpleNamespace(
        timeout=30.0,
        live_startup_timeout=3.0,
        live_completion_timeout=4.0,
        live_stop_timeout=5.0,
        require_bundled_current_platform=False,
        require_external_runtime_provisioning=False,
        check_executable_version=True,
        temporal_executable=tmp_path / "temporal",
        start_runtime=False,
        hermetic_runtime_smoke=False,
        default_routing_runtime_smoke=False,
        quality_runtime_smoke=False,
        live_runtime_smoke=False,
    )

    def fake_run_json(command: list[str], **_: Any) -> dict[str, str]:
        commands.append(command)
        return {"ok": "true"}

    monkeypatch.setattr(module, "_run_json", fake_run_json)

    module._smoke_runtime_install_probe(
        venv_python=tmp_path / "venv" / "bin" / "python",
        venv_dir=tmp_path / "venv",
        root=tmp_path,
        args=args,
    )

    assert "--temporal-executable" in commands[0]
    assert commands[0][commands[0].index("--temporal-executable") + 1] == str(
        tmp_path / "temporal"
    )


def test_parse_args_defines_live_timeout_defaults() -> None:
    """The CLI should provide every timeout passed to the smoke script."""
    module = _module()

    args = module.parse_args([])

    assert args.live_startup_timeout == 30.0
    assert args.live_completion_timeout == 180.0
    assert args.live_stop_timeout == 90.0
    assert args.require_external_runtime_provisioning is False
    assert args.temporal_executable is None
