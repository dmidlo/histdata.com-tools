"""Tests for the histdatacom application-image smoke helper."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


def _module() -> Any:
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts/smoke_container.py"
    )
    spec = importlib.util.spec_from_file_location(
        "smoke_container",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _completed(
    module: Any,
    command: list[str],
    *,
    stdout: str = "",
    stderr: str = "",
    returncode: int = 0,
) -> Any:
    return module.subprocess.CompletedProcess(
        args=command,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _inspect_payload() -> str:
    env = [
        "HISTDATACOM_RUNTIME_HOME=/workspace/runtime",
        "HISTDATACOM_RUNTIME_WORKSPACE=/workspace",
        "HISTDATACOM_TEMPORAL_CACHE_DIR=/workspace/cache/temporal-cli",
    ]
    labels = {
        name: "value"
        for name in (
            "org.opencontainers.image.created",
            "org.opencontainers.image.description",
            "org.opencontainers.image.documentation",
            "org.opencontainers.image.licenses",
            "org.opencontainers.image.revision",
            "org.opencontainers.image.source",
            "org.opencontainers.image.title",
            "org.opencontainers.image.version",
        )
    }
    return json.dumps(
        [
            {
                "Architecture": "arm64",
                "Config": {
                    "Cmd": ["--help"],
                    "Entrypoint": ["/usr/bin/tini", "--", "histdatacom"],
                    "Env": env,
                    "Healthcheck": None,
                    "Labels": labels,
                    "User": "10001:10001",
                    "Volumes": None,
                    "WorkingDir": "/workspace",
                },
                "Id": "sha256:image",
                "Os": "linux",
            }
        ]
    )


def test_smoke_builds_inspects_executes_and_cleans_image() -> None:
    """The default smoke should prove the image contract without residue."""
    module = _module()
    calls: list[tuple[list[str], bool]] = []

    def fake_run(command: list[str], *, check: bool = True) -> Any:
        calls.append((list(command), check))
        if command[:2] == ["docker", "build"]:
            return _completed(module, command)
        if command[:3] == ["docker", "image", "inspect"]:
            return _completed(module, command, stdout=_inspect_payload())
        if command[-1:] == ["--version"]:
            return _completed(module, command, stdout="histdatacom 2.0.0\n")
        if command[-1:] == ["--help"]:
            return _completed(
                module,
                command,
                stdout="usage: histdatacom [-h]\n",
            )
        if "--entrypoint" in command and "python" in command:
            return _completed(
                module,
                command,
                stdout=json.dumps(
                    {
                        "gid": 10001,
                        "paths": ["/workspace/data"],
                        "uid": 10001,
                    }
                ),
            )
        if command[:4] == ["docker", "image", "rm", "--force"]:
            return _completed(module, command)
        raise AssertionError(command)

    report = module.run_container_smoke(
        image="histdatacom:test",
        context=Path("."),
        created="2026-07-14T00:00:00Z",
        run_command=fake_run,
    )

    assert report["status"] == "passed"
    assert report["platform"] == {"architecture": "arm64", "os": "linux"}
    build = calls[0][0]
    assert "IMAGE_CREATED=2026-07-14T00:00:00Z" in build
    assert "IMAGE_VERSION=local-smoke" in build
    assert (
        ["docker", "image", "rm", "--force", "histdatacom:test"],
        False,
    ) in calls


def test_deep_smoke_proves_temporal_cache_across_containers() -> None:
    """Connected mode should start/stop Temporal and reuse its named cache."""
    module = _module()
    calls: list[tuple[list[str], bool]] = []

    def fake_run(command: list[str], *, check: bool = True) -> Any:
        calls.append((list(command), check))
        if command[:2] == ["docker", "build"]:
            return _completed(module, command)
        if command[:3] == ["docker", "image", "inspect"]:
            return _completed(module, command, stdout=_inspect_payload())
        if command[-1:] == ["--version"]:
            return _completed(module, command, stdout="histdatacom 2.0.0\n")
        if command[-1:] == ["--help"]:
            return _completed(
                module,
                command,
                stdout="usage: histdatacom [-h]\n",
            )
        if "--entrypoint" in command and "python" in command:
            if "/usr/bin/tini" in command:
                return _completed(
                    module,
                    command,
                    stdout=json.dumps(
                        {
                            "doctor": {},
                            "start": {"state": "running"},
                            "stop": {"state": "stopped"},
                        }
                    ),
                )
            return _completed(
                module,
                command,
                stdout=json.dumps({"gid": 10001, "uid": 10001}),
            )
        if command[:3] == ["docker", "volume", "create"]:
            return _completed(module, command, stdout=f"{command[-1]}\n")
        if command[-3:] == ["runtime", "doctor", "--json"]:
            return _completed(
                module,
                command,
                stdout=json.dumps(
                    {
                        "platform": {"key": "linux-arm64"},
                        "runtime_provisioning": {
                            "cache_available": True,
                            "cache_entries": [{"valid": True}],
                        },
                    }
                ),
            )
        if command[:4] in (
            ["docker", "volume", "rm", "--force"],
            ["docker", "image", "rm", "--force"],
        ):
            return _completed(module, command)
        raise AssertionError(command)

    report = module.run_container_smoke(
        image="histdatacom:test",
        deep_runtime=True,
        created="2026-07-14T00:00:00Z",
        run_command=fake_run,
    )

    assert report["runtime"]["start_state"] == "running"
    assert report["runtime"]["stop_state"] == "stopped"
    assert report["runtime"]["runtime_provisioning"]["cache_available"]
    assert any(call[:3] == ["docker", "volume", "create"] for call, _ in calls)
    assert any(
        call[:4] == ["docker", "volume", "rm", "--force"] and check is False
        for call, check in calls
    )
