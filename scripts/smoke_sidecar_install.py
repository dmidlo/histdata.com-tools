"""Smoke-test an installed histdatacom package and its sidecar resources."""

from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from importlib import metadata
from pathlib import Path
from typing import Any, Sequence

EXPECTED_ASSETS = ("README.md", "manifest.json", "runtime-defaults.json")
EXPECTED_CONSOLE_SCRIPTS = {
    "histdatacom": "histdatacom.histdata_com:main",
    "histdatacom-sidecar": "histdatacom.sidecar.cli:main",
    "histdatacom-sidecar-worker": "histdatacom.sidecar.worker:main",
}


def _single_wheel(wheel_dir: Path) -> Path:
    """Return the only built histdatacom wheel in a directory."""
    wheels = sorted(wheel_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def _script_path(name: str) -> str:
    """Return an installed console script path."""
    script_path = shutil.which(name)
    if script_path is None:
        raise SystemExit(f"console script is not on PATH: {name}")
    return script_path


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run a smoke command and fail with useful output when it breaks."""
    completed = subprocess.run(
        command,
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"command failed with exit {completed.returncode}: "
            f"{' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def _run_json(command: Sequence[str]) -> dict[str, Any]:
    """Run a command that emits JSON and return the decoded payload."""
    completed = _run(command)
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise SystemExit(
            f"command did not emit valid JSON: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}"
        ) from err
    if not isinstance(payload, dict):
        raise SystemExit(
            f"command emitted non-object JSON: {' '.join(command)}"
        )
    return payload


def install_wheel(
    *,
    wheel_dir: Path | None = None,
    wheel_path: Path | None = None,
) -> Path:
    """Install the built wheel into the active Python environment."""
    resolved_wheel = wheel_path or _single_wheel(wheel_dir or Path("dist"))
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", str(resolved_wheel)]
    )
    return resolved_wheel


def check_package_metadata(*, expect_temporal_extra: bool) -> dict[str, Any]:
    """Validate installed package metadata and console entry points."""
    import histdatacom

    dist = metadata.distribution("histdatacom")
    scripts = {
        entry.name: entry.value
        for entry in metadata.entry_points().select(group="console_scripts")
    }
    for script_name, expected_target in EXPECTED_CONSOLE_SCRIPTS.items():
        actual_target = scripts.get(script_name)
        if actual_target != expected_target:
            raise SystemExit(
                f"{script_name} entry point expected {expected_target!r}, "
                f"found {actual_target!r}"
            )
        _script_path(script_name)

    installed_version = metadata.version("histdatacom")
    if installed_version != histdatacom.__version__:
        raise SystemExit(
            "installed package metadata version does not match imported "
            f"package: {installed_version!r} != {histdatacom.__version__!r}"
        )
    if dist.metadata["Name"] != "histdatacom":
        raise SystemExit(f"unexpected installed name: {dist.metadata['Name']}")

    temporalio_version = ""
    if expect_temporal_extra:
        temporalio_version = metadata.version("temporalio")
        if importlib.util.find_spec("temporalio") is None:
            raise SystemExit("temporalio distribution is installed but missing")

    return {
        "name": dist.metadata["Name"],
        "version": installed_version,
        "console_scripts": sorted(EXPECTED_CONSOLE_SCRIPTS),
        "temporalio_version": temporalio_version,
    }


def check_sidecar_resources(
    *,
    require_bundled_current_platform: bool = False,
    check_executable_version: bool = False,
) -> dict[str, Any]:
    """Validate installed sidecar resources for the current platform."""
    from histdatacom.sidecar import (
        SidecarExecutableUnavailable,
        current_platform_key,
        load_sidecar_manifest,
        sidecar_asset,
        sidecar_executable_path,
    )

    manifest = load_sidecar_manifest()
    for asset in EXPECTED_ASSETS:
        if not sidecar_asset(asset).is_file():
            raise SystemExit(f"sidecar asset is not a file: {asset}")

    platform_key = current_platform_key()
    platform_resource = manifest.platforms.get(platform_key)
    if platform_resource is None:
        supported = ", ".join(sorted(manifest.platforms))
        raise SystemExit(
            f"current platform {platform_key!r} is not declared in sidecar "
            f"manifest. Supported platforms: {supported}"
        )
    executable_version = ""
    if platform_resource.bundled:
        with sidecar_executable_path(platform_key) as executable_path:
            if not executable_path.is_file():
                raise SystemExit(
                    f"bundled sidecar executable is missing: {executable_path}"
                )
            if check_executable_version:
                completed = _run([str(executable_path), "--version"])
                executable_version = (
                    completed.stdout.strip() or completed.stderr.strip()
                )
    else:
        if require_bundled_current_platform:
            raise SystemExit(
                f"current platform {platform_key!r} is not bundled in this wheel"
            )
        try:
            with sidecar_executable_path(platform_key):
                raise SystemExit(
                    "metadata-only sidecar resource exposed an executable"
                )
        except SidecarExecutableUnavailable as err:
            if "not bundled in this distribution" not in str(err):
                raise

    return {
        "sidecar": manifest.sidecar,
        "distribution_strategy": manifest.distribution_strategy,
        "embedded_binary": manifest.embedded_binary,
        "platform": platform_key,
        "platform_bundled": platform_resource.bundled,
        "executable_version": executable_version,
    }


def check_cli_smoke(
    state_dir: Path,
    *,
    require_bundled_current_platform: bool = False,
    start_sidecar: bool = False,
) -> dict[str, Any]:
    """Run offline CLI smoke checks against a temporary sidecar state dir."""
    state_dir.mkdir(parents=True, exist_ok=True)
    _run([_script_path("histdatacom"), "--version"])
    _run([_script_path("histdatacom-sidecar-worker"), "--help"])

    sidecar_script = _script_path("histdatacom-sidecar")
    status = _run_json(
        [
            sidecar_script,
            "--state-dir",
            str(state_dir),
            "--json",
            "status",
        ]
    )
    if status.get("state") not in {"running", "stopped"}:
        raise SystemExit(f"unexpected sidecar status payload: {status}")

    doctor = _run_json(
        [
            sidecar_script,
            "--state-dir",
            str(state_dir),
            "--json",
            "doctor",
        ]
    )
    platform = doctor.get("platform", {})
    if not isinstance(platform, dict) or not platform.get("supported"):
        raise SystemExit(f"unexpected sidecar doctor payload: {doctor}")
    if (
        require_bundled_current_platform
        and platform.get("executable_bundled") is not True
    ):
        raise SystemExit(
            "sidecar doctor did not report a bundled current-platform "
            f"executable: {doctor}"
        )

    start_state = ""
    stop_state = ""
    if start_sidecar:
        start = _run_json(
            [
                sidecar_script,
                "--state-dir",
                str(state_dir),
                "--json",
                "start",
                "--startup-timeout",
                "20",
            ]
        )
        start_state = str(start.get("state", ""))
        if start_state != "running":
            raise SystemExit(f"unexpected sidecar start payload: {start}")
        stop = _run_json(
            [
                sidecar_script,
                "--state-dir",
                str(state_dir),
                "--json",
                "stop",
            ]
        )
        stop_state = str(stop.get("state", ""))
    return {
        "status_state": status["state"],
        "doctor_supported": platform["supported"],
        "doctor_executable_bundled": platform.get("executable_bundled"),
        "start_state": start_state,
        "stop_state": stop_state,
    }


def main() -> None:
    """Run install-time sidecar smoke checks."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--wheel-dir",
        type=Path,
        help="install the only histdatacom wheel from this directory first",
    )
    parser.add_argument(
        "--wheel",
        type=Path,
        help="install this exact histdatacom wheel first",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="state directory used for offline sidecar CLI checks",
    )
    parser.add_argument(
        "--expect-temporal-extra",
        action="store_true",
        help="require the temporalio optional dependency to be installed",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="skip console command execution and validate import metadata only",
    )
    parser.add_argument(
        "--require-bundled-current-platform",
        action="store_true",
        help="require the installed wheel to bundle this platform executable",
    )
    parser.add_argument(
        "--check-executable-version",
        action="store_true",
        help="run the packaged Temporal executable with --version",
    )
    parser.add_argument(
        "--start-sidecar",
        action="store_true",
        help="start the sidecar without --executable and then stop it",
    )
    args = parser.parse_args()
    if args.wheel is not None and args.wheel_dir is not None:
        parser.error("--wheel and --wheel-dir are mutually exclusive")

    wheel_name = ""
    if args.wheel_dir is not None or args.wheel is not None:
        wheel_name = install_wheel(
            wheel_dir=args.wheel_dir,
            wheel_path=args.wheel,
        ).name

    with tempfile.TemporaryDirectory() as temporary_dir:
        state_dir = args.state_dir or Path(temporary_dir) / "sidecar-state"
        report = {
            "wheel": wheel_name,
            "package": check_package_metadata(
                expect_temporal_extra=args.expect_temporal_extra
            ),
            "sidecar": check_sidecar_resources(
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                check_executable_version=args.check_executable_version,
            ),
            "cli": None,
        }
        if not args.skip_cli:
            report["cli"] = check_cli_smoke(
                state_dir,
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                start_sidecar=args.start_sidecar,
            )

    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201


if __name__ == "__main__":
    main()
