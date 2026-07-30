"""Build and verify the distributable histdatacom container image."""

from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_IMAGE = "histdatacom:local-smoke"
DEFAULT_CONTEXT = Path(__file__).resolve().parents[1]
CONTAINER_UID = 10001
CONTAINER_GID = 10001
WORKSPACE = "/workspace"
REQUIRED_ENV = {
    "HISTDATACOM_RUNTIME_HOME": "/workspace/runtime",
    "HISTDATACOM_RUNTIME_WORKSPACE": WORKSPACE,
    "HISTDATACOM_TEMPORAL_CACHE_DIR": "/workspace/cache/temporal-cli",
}
REQUIRED_LABELS = {
    "org.opencontainers.image.created",
    "org.opencontainers.image.description",
    "org.opencontainers.image.documentation",
    "org.opencontainers.image.licenses",
    "org.opencontainers.image.revision",
    "org.opencontainers.image.source",
    "org.opencontainers.image.title",
    "org.opencontainers.image.version",
}

RunCommand = Callable[..., subprocess.CompletedProcess[str]]


class ContainerSmokeError(RuntimeError):
    """Raised when a container distribution contract is not satisfied."""

    def __init__(self, message: str, diagnostics: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


def _run(
    command: Sequence[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run one command and retain bounded diagnostics on failure."""
    completed = subprocess.run(
        list(command),
        capture_output=True,
        check=False,
        text=True,
    )
    if check and completed.returncode != 0:
        raise ContainerSmokeError(
            f"command failed with exit {completed.returncode}: " f"{' '.join(command)}",
            {
                "command": list(command),
                "returncode": completed.returncode,
                "stdout": completed.stdout[-8000:],
                "stderr": completed.stderr[-8000:],
            },
        )
    return completed


def _json_output(completed: subprocess.CompletedProcess[str]) -> Any:
    """Decode a command's JSON stdout with useful failure evidence."""
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise ContainerSmokeError(
            "command did not emit valid JSON",
            {
                "command": list(completed.args),
                "stdout": completed.stdout[-8000:],
                "stderr": completed.stderr[-8000:],
            },
        ) from err


def build_image(
    image: str,
    *,
    context: Path = DEFAULT_CONTEXT,
    version: str = "local-smoke",
    revision: str = "local",
    created: str | None = None,
    run_command: RunCommand = _run,
) -> None:
    """Build the native-platform smoke image with explicit OCI metadata."""
    timestamp = (
        created
        or datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
    )
    run_command(
        [
            "docker",
            "build",
            "--tag",
            image,
            "--build-arg",
            f"IMAGE_CREATED={timestamp.replace('+00:00', 'Z')}",
            "--build-arg",
            f"IMAGE_REVISION={revision}",
            "--build-arg",
            f"IMAGE_VERSION={version}",
            str(context),
        ]
    )


def inspect_image(
    image: str,
    *,
    run_command: RunCommand = _run,
) -> dict[str, Any]:
    """Return and validate the image configuration contract."""
    payload = _json_output(run_command(["docker", "image", "inspect", image]))
    if not isinstance(payload, list) or len(payload) != 1:
        raise ContainerSmokeError(
            "docker image inspect returned an unexpected payload",
            {"image": image, "payload": payload},
        )
    inspected = payload[0]
    if not isinstance(inspected, dict):
        raise ContainerSmokeError(
            "docker image inspect entry is not an object",
            {"image": image, "payload": inspected},
        )
    config = inspected.get("Config")
    if not isinstance(config, dict):
        raise ContainerSmokeError(
            "docker image inspect is missing Config",
            {"image": image, "payload": inspected},
        )

    env = {}
    for item in config.get("Env", []):
        key, separator, value = str(item).partition("=")
        if separator:
            env[key] = value
    labels = config.get("Labels") or {}
    failures: list[str] = []
    if config.get("User") != f"{CONTAINER_UID}:{CONTAINER_GID}":
        failures.append("fixed non-root user is not configured")
    if config.get("WorkingDir") != WORKSPACE:
        failures.append("working directory is not /workspace")
    if config.get("Entrypoint") != ["/usr/bin/tini", "--", "histdatacom"]:
        failures.append("tini-backed histdatacom entry point is not configured")
    if config.get("Cmd") != ["--help"]:
        failures.append("bounded help is not the default command")
    if config.get("Healthcheck") is not None:
        failures.append("one-shot CLI image declares a service health check")
    if config.get("Volumes") is not None:
        failures.append("image declares hidden anonymous storage")
    for key, expected in REQUIRED_ENV.items():
        if env.get(key) != expected:
            failures.append(f"{key} does not resolve beneath /workspace")
    missing_labels = sorted(REQUIRED_LABELS.difference(labels))
    if missing_labels:
        failures.append("missing OCI labels: " + ", ".join(missing_labels))
    if failures:
        raise ContainerSmokeError(
            "container image configuration contract failed",
            {"image": image, "failures": failures, "config": config},
        )
    return inspected


_WRITE_PROBE = """
import json
import os
from pathlib import Path

paths = [
    Path('/workspace/data'),
    Path('/workspace/runtime'),
    Path('/workspace/cache/temporal-cli'),
]
for path in paths:
    marker = path / '.histdatacom-container-write-probe'
    marker.write_text('ok', encoding='utf-8')
    marker.unlink()
print(json.dumps({
    'gid': os.getgid(),
    'paths': [str(path) for path in paths],
    'uid': os.getuid(),
}))
"""


_RUNTIME_PROBE = """
import json
import subprocess

def run(command):
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(json.dumps({
            'command': command,
            'returncode': completed.returncode,
            'stdout': completed.stdout[-8000:],
            'stderr': completed.stderr[-8000:],
        }, sort_keys=True))
    return json.loads(completed.stdout)

start = None
doctor = None
stop = None
try:
    start = run([
        'histdatacom', 'runtime', 'start', '--json',
        '--startup-timeout', '60',
    ])
    doctor = run(['histdatacom', 'runtime', 'doctor', '--json'])
finally:
    if start is not None:
        stop = run(['histdatacom', 'runtime', 'stop', '--json'])

print(json.dumps({
    'doctor': doctor,
    'start': start,
    'stop': stop,
}, sort_keys=True))
"""


def _run_cli_smoke(
    image: str,
    *,
    run_command: RunCommand,
) -> dict[str, Any]:
    version = run_command(["docker", "run", "--rm", image, "--version"])
    help_result = run_command(["docker", "run", "--rm", image, "--help"])
    write_probe = _json_output(
        run_command(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "python",
                image,
                "-c",
                _WRITE_PROBE,
            ]
        )
    )
    expected_identity = {"uid": CONTAINER_UID, "gid": CONTAINER_GID}
    actual_identity = {
        "uid": write_probe.get("uid"),
        "gid": write_probe.get("gid"),
    }
    if actual_identity != expected_identity:
        raise ContainerSmokeError(
            "container process did not run with the expected identity",
            {"expected": expected_identity, "actual": actual_identity},
        )
    if not version.stdout.strip() or "usage: histdatacom" not in help_result.stdout:
        raise ContainerSmokeError(
            "container CLI version/help smoke failed",
            {
                "version": version.stdout[-2000:],
                "help": help_result.stdout[-4000:],
            },
        )
    return {
        "help_line_count": len(help_result.stdout.splitlines()),
        "version": version.stdout.strip(),
        "write_probe": write_probe,
    }


def _run_runtime_smoke(
    image: str,
    volume: str,
    *,
    run_command: RunCommand,
) -> dict[str, Any]:
    mount = f"type=volume,source={volume},target={WORKSPACE}"
    first_pass = _json_output(
        run_command(
            [
                "docker",
                "run",
                "--rm",
                "--mount",
                mount,
                "--entrypoint",
                "/usr/bin/tini",
                image,
                "--",
                "python",
                "-c",
                _RUNTIME_PROBE,
            ]
        )
    )
    persisted_doctor = _json_output(
        run_command(
            [
                "docker",
                "run",
                "--rm",
                "--mount",
                mount,
                image,
                "runtime",
                "doctor",
                "--json",
            ]
        )
    )
    provisioning = persisted_doctor.get("runtime_provisioning", {})
    start = first_pass.get("start") or {}
    stop = first_pass.get("stop") or {}
    failures: list[str] = []
    if start.get("state") != "running":
        failures.append("runtime did not reach running state")
    if stop.get("state") != "stopped":
        failures.append("runtime did not stop cleanly")
    if not provisioning.get("cache_available"):
        failures.append("verified Temporal cache did not persist")
    if not provisioning.get("cache_entries"):
        failures.append("Temporal cache provenance is missing")
    if failures:
        raise ContainerSmokeError(
            "connected runtime container smoke failed",
            {
                "failures": failures,
                "first_pass": first_pass,
                "persisted_doctor": persisted_doctor,
            },
        )
    return {
        "platform": persisted_doctor.get("platform"),
        "runtime_provisioning": provisioning,
        "start_state": start.get("state"),
        "stop_state": stop.get("state"),
        "workspace_volume": volume,
    }


def run_container_smoke(
    *,
    image: str = DEFAULT_IMAGE,
    context: Path = DEFAULT_CONTEXT,
    deep_runtime: bool = False,
    keep_image: bool = False,
    keep_workspace_volume: bool = False,
    version: str = "local-smoke",
    revision: str = "local",
    created: str | None = None,
    run_command: RunCommand = _run,
) -> dict[str, Any]:
    """Build, inspect, execute, and normally clean the application image."""
    volume = f"histdatacom-smoke-{secrets.token_hex(6)}"
    image_built = False
    volume_created = False
    report: dict[str, Any] = {
        "deep_runtime": deep_runtime,
        "image": image,
        "status": "running",
    }
    try:
        build_image(
            image,
            context=context,
            version=version,
            revision=revision,
            created=created,
            run_command=run_command,
        )
        image_built = True
        inspected = inspect_image(image, run_command=run_command)
        report["image_id"] = inspected.get("Id")
        report["platform"] = {
            "architecture": inspected.get("Architecture"),
            "os": inspected.get("Os"),
        }
        report["cli"] = _run_cli_smoke(image, run_command=run_command)
        if deep_runtime:
            run_command(["docker", "volume", "create", volume])
            volume_created = True
            report["runtime"] = _run_runtime_smoke(
                image,
                volume,
                run_command=run_command,
            )
        report["status"] = "passed"
        return report
    finally:
        if volume_created and not keep_workspace_volume:
            run_command(
                ["docker", "volume", "rm", "--force", volume],
                check=False,
            )
        if image_built and not keep_image:
            run_command(
                ["docker", "image", "rm", "--force", image],
                check=False,
            )


def build_parser() -> argparse.ArgumentParser:
    """Build the container smoke CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--context", type=Path, default=DEFAULT_CONTEXT)
    parser.add_argument(
        "--deep-runtime",
        action="store_true",
        help=(
            "download the pinned Temporal runtime and exercise "
            "start/doctor/stop with a persistent named volume"
        ),
    )
    parser.add_argument("--keep-image", action="store_true")
    parser.add_argument("--keep-workspace-volume", action="store_true")
    parser.add_argument("--version", default="local-smoke")
    parser.add_argument("--revision", default="local")
    parser.add_argument("--created")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the image smoke and emit one machine-readable report."""
    args = build_parser().parse_args(argv)
    try:
        report = run_container_smoke(
            image=args.image,
            context=args.context,
            deep_runtime=args.deep_runtime,
            keep_image=args.keep_image,
            keep_workspace_volume=args.keep_workspace_volume,
            version=args.version,
            revision=args.revision,
            created=args.created,
        )
    except ContainerSmokeError as err:
        print(  # noqa: T201
            json.dumps(
                {
                    "diagnostics": err.diagnostics,
                    "error": str(err),
                    "status": "failed",
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
