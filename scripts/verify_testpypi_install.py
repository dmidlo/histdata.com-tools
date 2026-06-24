"""Verify a TestPyPI histdatacom artifact from a fresh virtual environment."""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any, ContextManager, Iterator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TESTPYPI_INDEX = "https://test.pypi.org/simple/"
DEFAULT_DEPENDENCY_INDEX = "https://pypi.org/simple/"
EXPECTED_HELP_TOKENS = (
    "--sidecar",
    "--sidecar-start",
    "--no-sidecar-start",
    "--sidecar-submit-only",
    "--quality",
    "--repo-quality",
    "--repo-quality-columns",
    "--quality-target",
    "--quality-checks",
    "--quality-report",
    "--quality-profile",
    "--quality-fail-on",
    "--quality-max-errors",
    "--quality-max-warnings",
)
EXPECTED_SIDECAR_COMMANDS = (
    "status",
    "doctor",
    "start",
    "stop",
)
METADATA_PROBE = r"""
import json
import sys
from importlib import metadata

import histdatacom
from histdatacom.runtime_contracts import RunRequest as RuntimeRunRequest
from histdatacom.sidecar.contracts import RunRequest as SidecarRunRequest
from histdatacom.sidecar.resources import (
    current_platform_key,
    load_sidecar_manifest,
    sidecar_asset,
    sidecar_executable_path,
    sidecar_platform_resource,
)

expected_version = sys.argv[1]
require_bundled = sys.argv[2] == "1"
dist = metadata.distribution("histdatacom")
installed_version = metadata.version("histdatacom")
if installed_version != expected_version:
    raise SystemExit(
        f"installed version {installed_version!r} != {expected_version!r}"
    )
if histdatacom.__version__ != expected_version:
    raise SystemExit(
        f"imported version {histdatacom.__version__!r} != {expected_version!r}"
    )
if dist.metadata["Name"] != "histdatacom":
    raise SystemExit(f"unexpected distribution name: {dist.metadata['Name']!r}")

entry_points = {
    entry.name: entry.value
    for entry in metadata.entry_points().select(group="console_scripts")
}
expected_scripts = {
    "histdatacom": "histdatacom.histdata_com:main",
    "histdatacom-sidecar": "histdatacom.sidecar.cli:main",
    "histdatacom-sidecar-worker": "histdatacom.sidecar.worker:main",
}
for name, target in expected_scripts.items():
    if entry_points.get(name) != target:
        raise SystemExit(
            f"entry point {name!r} expected {target!r}, "
            f"found {entry_points.get(name)!r}"
        )

requires_dist = dist.metadata.get_all("Requires-Dist") or []
for dependency in ("polars", "rich", "requests", "temporalio"):
    if not any(
        requirement.lower().startswith(dependency)
        and "extra ==" not in requirement.lower()
        for requirement in requires_dist
    ):
        raise SystemExit(f"core dependency missing from metadata: {dependency}")

expected_assets = (
    "README.md",
    "manifest.json",
    "runtime-defaults.json",
    "third-party/temporal-cli/LICENSE",
    "third-party/temporal-cli/NOTICE.md",
)
for asset in expected_assets:
    if not sidecar_asset(asset).is_file():
        raise SystemExit(f"sidecar asset is not packaged: {asset}")

manifest = load_sidecar_manifest()
platform_key = current_platform_key()
resource = sidecar_platform_resource(platform_key, manifest)
executable = ""
if require_bundled:
    if not resource.bundled:
        raise SystemExit(
            f"current platform {platform_key!r} is not bundled in TestPyPI wheel"
        )
    with sidecar_executable_path(platform_key, manifest) as executable_path:
        executable = str(executable_path)
else:
    if resource.bundled:
        with sidecar_executable_path(platform_key, manifest) as executable_path:
            executable = str(executable_path)

if SidecarRunRequest is not RuntimeRunRequest:
    raise SystemExit("sidecar RunRequest is not the runtime contract RunRequest")

print(json.dumps({
    "distribution_name": dist.metadata["Name"],
    "version": installed_version,
    "requires_dist": sorted(requires_dist),
    "console_scripts": sorted(expected_scripts),
    "sidecar": {
        "platform": platform_key,
        "distribution_strategy": manifest.distribution_strategy,
        "embedded_binary": manifest.embedded_binary,
        "platform_bundled": resource.bundled,
        "executable": executable,
    },
}, sort_keys=True))
"""


def _run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 300.0,
) -> subprocess.CompletedProcess[str]:
    """Run a command and fail with captured output on non-zero exit."""
    completed = subprocess.run(
        list(command),
        capture_output=True,
        check=False,
        cwd=cwd,
        env=env,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"command failed with exit {completed.returncode}: "
            f"{' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def _run_json(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 300.0,
) -> dict[str, Any]:
    """Run a JSON-emitting command and return the decoded object."""
    completed = _run(command, cwd=cwd, env=env, timeout=timeout)
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as err:
        raise SystemExit(
            f"command did not emit JSON: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        ) from err
    if not isinstance(payload, dict):
        raise SystemExit(f"command emitted non-object JSON: {' '.join(command)}")
    return payload


def _project_version(project_root: Path = PROJECT_ROOT) -> str:
    """Return the package version declared by the source tree."""
    init_path = project_root / "src" / "histdatacom" / "__init__.py"
    match = re.search(
        r'^__version__\s*=\s*["\']([^"\']+)["\']',
        init_path.read_text(encoding="utf-8"),
        flags=re.MULTILINE,
    )
    if match is None:
        raise SystemExit(f"could not read __version__ from {init_path}")
    return match.group(1)


def _script_path(venv_dir: Path, script_name: str) -> Path:
    """Return the path to an executable inside a virtual environment."""
    bin_dir = "Scripts" if os.name == "nt" else "bin"
    suffix = ".exe" if os.name == "nt" else ""
    return venv_dir / bin_dir / f"{script_name}{suffix}"


def _venv_python(venv_dir: Path) -> Path:
    return _script_path(venv_dir, "python")


def _venv_environment(venv_dir: Path) -> dict[str, str]:
    """Return an environment that exposes installed console scripts."""
    env = os.environ.copy()
    script_dir = _venv_python(venv_dir).parent
    env["PATH"] = str(script_dir) + os.pathsep + env.get("PATH", "")
    env["VIRTUAL_ENV"] = str(venv_dir)
    env.pop("PYTHONHOME", None)
    return env


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _create_environment(
    *,
    python: str,
    venv_dir: Path,
    timeout: float,
) -> Path:
    """Create a fresh virtual environment and return its Python executable."""
    _run([python, "-m", "venv", str(venv_dir)], timeout=timeout)
    venv_python = _venv_python(venv_dir)
    _run(
        [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"],
        timeout=timeout,
    )
    return venv_python


def _download_testpypi_wheel(
    *,
    venv_python: Path,
    download_dir: Path,
    version: str,
    index_url: str,
    timeout: float,
) -> Path:
    """Download exactly one compatible histdatacom wheel from TestPyPI."""
    download_dir.mkdir(parents=True, exist_ok=True)
    _run(
        [
            str(venv_python),
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--no-deps",
            "--index-url",
            index_url,
            "--dest",
            str(download_dir),
            f"histdatacom=={version}",
        ],
        timeout=timeout,
    )
    wheels = sorted(download_dir.glob(f"histdatacom-{version}-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(
            f"expected exactly one TestPyPI wheel for {version}, found {wheels}"
        )
    return wheels[0]


def _install_wheel(
    *,
    venv_python: Path,
    wheel: Path,
    dependency_index_url: str,
    timeout: float,
) -> None:
    """Install a downloaded TestPyPI wheel with dependencies from PyPI."""
    _run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--index-url",
            dependency_index_url,
            str(wheel),
        ],
        timeout=timeout,
    )


def _metadata_probe(
    *,
    venv_python: Path,
    version: str,
    require_bundled_current_platform: bool,
    timeout: float,
) -> dict[str, Any]:
    """Validate installed metadata, dependencies, entry points, and resources."""
    return _run_json(
        [
            str(venv_python),
            "-c",
            METADATA_PROBE,
            version,
            "1" if require_bundled_current_platform else "0",
        ],
        timeout=timeout,
    )


def _cli_parity_probe(
    *,
    venv_dir: Path,
    version: str,
    timeout: float,
) -> dict[str, Any]:
    """Validate current installed CLI surface and entry-point behavior."""
    histdatacom = _script_path(venv_dir, "histdatacom")
    sidecar = _script_path(venv_dir, "histdatacom-sidecar")
    worker = _script_path(venv_dir, "histdatacom-sidecar-worker")

    version_output = _run([str(histdatacom), "--version"], timeout=timeout)
    actual_version = version_output.stdout.strip()
    if actual_version != version:
        raise SystemExit(
            f"histdatacom --version returned {actual_version!r}, "
            f"expected {version!r}"
        )

    help_output = _run([str(histdatacom), "-h"], timeout=timeout).stdout
    missing_tokens = [
        token for token in EXPECTED_HELP_TOKENS if token not in help_output
    ]
    if missing_tokens:
        raise SystemExit(
            "installed histdatacom CLI help is missing current flags: "
            + ", ".join(missing_tokens)
        )

    sidecar_help = _run([str(sidecar), "--help"], timeout=timeout).stdout
    missing_commands = [
        command
        for command in EXPECTED_SIDECAR_COMMANDS
        if command not in sidecar_help
    ]
    if missing_commands:
        raise SystemExit(
            "installed sidecar CLI help is missing commands: "
            + ", ".join(missing_commands)
        )
    _run([str(worker), "--help"], timeout=timeout)

    return {
        "version": actual_version,
        "required_help_tokens": sorted(EXPECTED_HELP_TOKENS),
        "sidecar_commands": sorted(EXPECTED_SIDECAR_COMMANDS),
    }


def _smoke_sidecar_install_probe(
    *,
    venv_python: Path,
    venv_dir: Path,
    root: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Run the installed-package sidecar smoke suite from the fresh venv."""
    command = [
        str(venv_python),
        str(PROJECT_ROOT / "scripts" / "smoke_sidecar_install.py"),
        "--state-dir",
        str(root / "sidecar-state"),
        "--live-workspace",
        str(root / "sidecar-workspace"),
        "--live-runtime-home",
        str(root / "sidecar-runtime"),
        "--live-data-dir",
        str(root / "sidecar-data"),
        "--live-startup-timeout",
        str(args.live_startup_timeout),
        "--live-completion-timeout",
        str(args.live_completion_timeout),
        "--live-stop-timeout",
        str(args.live_stop_timeout),
    ]
    if args.require_bundled_current_platform:
        command.append("--require-bundled-current-platform")
    if args.check_executable_version:
        command.append("--check-executable-version")
    if args.start_sidecar:
        command.append("--start-sidecar")
    if args.hermetic_sidecar_smoke:
        command.append("--hermetic-sidecar-smoke")
    if args.default_routing_sidecar_smoke:
        command.append("--default-routing-sidecar-smoke")
    if args.quality_sidecar_smoke:
        command.append("--quality-sidecar-smoke")
    if args.live_sidecar_smoke:
        command.append("--live-sidecar-smoke")
    return _run_json(
        command,
        env=_venv_environment(venv_dir),
        timeout=args.timeout,
    )


def _download_smoke_probe(
    *,
    venv_dir: Path,
    root: Path,
    timeout: float,
) -> dict[str, Any]:
    """Run a small live download/extract smoke through installed CLI defaults."""
    data_dir = root / "download-smoke-data"
    env = _venv_environment(venv_dir)
    env["HISTDATACOM_SIDECAR_HOME"] = str(root / "download-smoke-sidecar-runtime")
    env["HISTDATACOM_SIDECAR_WORKSPACE"] = str(
        root / "download-smoke-sidecar-workspace"
    )
    command = [
        str(_script_path(venv_dir, "histdatacom")),
        "-p",
        "eurusd",
        "-f",
        "ascii",
        "-t",
        "1-minute-bar-quotes",
        "-s",
        "202201",
        "-e",
        "202202",
        "--data-directory",
        str(data_dir),
        "-D",
    ]
    try:
        completed = _run(command, env=env, timeout=timeout)
    except BaseException:
        try:
            _run_json(
                [str(_script_path(venv_dir, "histdatacom-sidecar")), "--json", "stop"],
                env=env,
                timeout=90.0,
            )
        except SystemExit:
            pass
        raise
    sidecar_stop = _run_json(
        [str(_script_path(venv_dir, "histdatacom-sidecar")), "--json", "stop"],
        env=env,
        timeout=90.0,
    )
    files = sorted(path.name for path in data_dir.rglob("*") if path.is_file())
    if not files:
        raise SystemExit("download smoke did not create any data files")
    return {
        "returncode": completed.returncode,
        "data_directory": str(data_dir),
        "files": files,
        "sidecar_stop": sidecar_stop,
    }


@contextmanager
def _temporary_root() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="histdatacom-testpypi-") as root:
        yield Path(root)


def _root_context(work_dir: Path | None) -> ContextManager[Path]:
    if work_dir is not None:
        work_dir.mkdir(parents=True, exist_ok=True)
        return nullcontext(work_dir)
    return _temporary_root()


def verify(args: argparse.Namespace) -> dict[str, Any]:
    """Run the full TestPyPI installed-package verification harness."""
    version = args.version or _project_version()
    with _root_context(args.work_dir) as root_value:
        root = Path(root_value)
        venv_dir = root / "venv"
        venv_python = _create_environment(
            python=args.python,
            venv_dir=venv_dir,
            timeout=args.timeout,
        )
        wheel = _download_testpypi_wheel(
            venv_python=venv_python,
            download_dir=root / "downloads",
            version=version,
            index_url=args.index_url,
            timeout=args.timeout,
        )
        _install_wheel(
            venv_python=venv_python,
            wheel=wheel,
            dependency_index_url=args.dependency_index_url,
            timeout=args.timeout,
        )
        report: dict[str, Any] = {
            "version": version,
            "root": str(root),
            "testpypi": {
                "index_url": args.index_url,
                "wheel": str(wheel),
                "wheel_sha256": _sha256(wheel),
                "wheel_size": wheel.stat().st_size,
            },
            "metadata": _metadata_probe(
                venv_python=venv_python,
                version=version,
                require_bundled_current_platform=(
                    args.require_bundled_current_platform
                ),
                timeout=args.timeout,
            ),
            "cli": _cli_parity_probe(
                venv_dir=venv_dir,
                version=version,
                timeout=args.timeout,
            ),
            "installed_smoke": _smoke_sidecar_install_probe(
                venv_python=venv_python,
                venv_dir=venv_dir,
                root=root,
                args=args,
            ),
            "download_smoke": None,
        }
        if args.download_smoke:
            report["download_smoke"] = _download_smoke_probe(
                venv_dir=venv_dir,
                root=root,
                timeout=args.download_timeout,
            )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if args.keep_env:
            print(f"kept TestPyPI verification environment: {root}", file=sys.stderr)
        return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download histdatacom from TestPyPI, install it into a fresh "
            "virtual environment, and verify parity with the current package "
            "surface."
        )
    )
    parser.add_argument("--version", help="histdatacom version to verify")
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used to create the fresh virtual environment",
    )
    parser.add_argument(
        "--index-url",
        default=DEFAULT_TESTPYPI_INDEX,
        help="package index used only to download the histdatacom artifact",
    )
    parser.add_argument(
        "--dependency-index-url",
        default=DEFAULT_DEPENDENCY_INDEX,
        help="package index used to resolve dependencies after wheel download",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="reuse or create a specific verification working directory",
    )
    parser.add_argument(
        "--keep-env",
        action="store_true",
        help="keep the verification environment instead of deleting it",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="write a JSON verification report",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="default command timeout in seconds",
    )
    parser.add_argument(
        "--download-timeout",
        type=float,
        default=300.0,
        help="live download smoke timeout in seconds",
    )
    parser.add_argument(
        "--live-startup-timeout",
        type=float,
        default=30.0,
        help="seconds to wait for the live Temporal frontend to start",
    )
    parser.add_argument(
        "--live-completion-timeout",
        type=float,
        default=180.0,
        help="seconds to wait for the live sidecar smoke job to complete",
    )
    parser.add_argument(
        "--live-stop-timeout",
        type=float,
        default=90.0,
        help="seconds to wait for live sidecar processes to stop",
    )
    parser.add_argument(
        "--require-bundled-current-platform",
        action="store_true",
        help="require TestPyPI to install a bundled wheel for this platform",
    )
    parser.add_argument(
        "--check-executable-version",
        action="store_true",
        help="run the packaged Temporal executable with --version",
    )
    parser.add_argument(
        "--start-sidecar",
        action="store_true",
        help="start and stop the installed sidecar through its CLI",
    )
    parser.add_argument(
        "--hermetic-sidecar-smoke",
        action="store_true",
        help="run deterministic installed sidecar workflow smoke",
    )
    parser.add_argument(
        "--default-routing-sidecar-smoke",
        action="store_true",
        help="run deterministic default client-routing sidecar smoke",
    )
    parser.add_argument(
        "--quality-sidecar-smoke",
        action="store_true",
        help="run clean and dirty quality checks through the installed sidecar",
    )
    parser.add_argument(
        "--live-sidecar-smoke",
        action="store_true",
        help="run operator-gated live HistData sidecar smoke",
    )
    parser.add_argument(
        "--download-smoke",
        action="store_true",
        help="run a small live download/extract CLI smoke",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    report = verify(args)
    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201


if __name__ == "__main__":
    main()
