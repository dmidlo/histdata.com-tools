"""Build platform wheels with a bundled Temporal sidecar executable."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

ASSET_ROOT = Path("src/histdatacom/sidecar/assets")
MANIFEST_FILENAME = "manifest.json"
DEFAULT_PYTHON_TAG = "py3"
DEFAULT_ABI_TAG = "none"
IGNORED_COPY_NAMES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    "build",
    "dist",
    "htmlcov",
    "venv",
    "__pycache__",
}


def _current_platform_key(
    system: str | None = None,
    machine: str | None = None,
) -> str:
    """Return the sidecar manifest platform key for this machine."""
    normalized_system = (system or platform.system()).strip().lower()
    normalized_machine = (machine or platform.machine()).strip().lower()
    system_aliases = {
        "darwin": "macos",
        "mac": "macos",
        "macos": "macos",
        "linux": "linux",
        "windows": "windows",
    }
    machine_aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "x86-64": "x86_64",
        "aarch64": "arm64",
    }
    return (
        f"{system_aliases.get(normalized_system, normalized_system)}-"
        f"{machine_aliases.get(normalized_machine, normalized_machine)}"
    )


def _manifest_path(source_root: Path) -> Path:
    """Return the sidecar manifest path inside a source tree."""
    return source_root / ASSET_ROOT / MANIFEST_FILENAME


def _load_manifest(source_root: Path) -> dict[str, Any]:
    """Load a sidecar manifest from a source tree."""
    manifest_path = _manifest_path(source_root)
    try:
        loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise SystemExit(f"sidecar manifest not found: {manifest_path}") from err
    if not isinstance(loaded, dict):
        raise SystemExit(f"sidecar manifest must contain an object: {manifest_path}")
    return loaded


def _write_manifest(source_root: Path, manifest: dict[str, Any]) -> None:
    """Write a normalized sidecar manifest into a source tree."""
    _manifest_path(source_root).write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _resource_path(relative_path: str) -> PurePosixPath:
    """Validate and return a manifest resource path."""
    resource_path = PurePosixPath(relative_path)
    if resource_path.is_absolute() or ".." in resource_path.parts:
        raise SystemExit(f"sidecar resource path must be relative: {relative_path}")
    return resource_path


def _asset_path(source_root: Path, relative_path: str) -> Path:
    """Return a filesystem path for a POSIX-style sidecar asset path."""
    resource_path = _resource_path(relative_path)
    return source_root / ASSET_ROOT / Path(*resource_path.parts)


def _platform_resource(
    manifest: dict[str, Any],
    platform_key: str,
) -> dict[str, Any]:
    """Return a mutable platform resource entry from a manifest."""
    platforms = manifest.get("platforms")
    if not isinstance(platforms, dict):
        raise SystemExit("sidecar manifest must define a platforms object")
    resource = platforms.get(platform_key)
    if not isinstance(resource, dict):
        supported = ", ".join(sorted(str(key) for key in platforms))
        raise SystemExit(
            f"platform {platform_key!r} is not declared. "
            f"Supported platform keys: {supported}"
        )
    return resource


def _is_windows_platform(platform_key: str) -> bool:
    """Return whether a sidecar platform key targets Windows."""
    return platform_key.startswith("windows-")


def _run_version_check(executable: Path) -> str:
    """Run a best-effort Temporal executable version probe."""
    completed = subprocess.run(
        [str(executable), "--version"],
        capture_output=True,
        check=False,
        text=True,
        timeout=20,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"Temporal executable version probe failed for {executable} "
            f"with exit {completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed.stdout.strip() or completed.stderr.strip()


def prepare_sidecar_binary(
    *,
    source_root: Path,
    platform_key: str,
    executable: Path,
    check_version: bool = False,
) -> dict[str, Any]:
    """Copy a Temporal executable into a source tree and patch the manifest."""
    resolved_source = source_root.resolve()
    resolved_executable = executable.expanduser().resolve()
    if not resolved_executable.is_file():
        raise SystemExit(f"Temporal executable is not a file: {resolved_executable}")

    manifest = _load_manifest(resolved_source)
    resource = _platform_resource(manifest, platform_key)
    relative_executable = str(resource.get("executable", ""))
    if not relative_executable:
        raise SystemExit(f"platform {platform_key!r} has no executable resource path")
    _resource_path(relative_executable)

    if not _is_windows_platform(platform_key) and not os.access(
        resolved_executable, os.X_OK
    ):
        raise SystemExit(
            f"Temporal executable is not executable: {resolved_executable}"
        )

    version = _run_version_check(resolved_executable) if check_version else ""
    target = _asset_path(resolved_source, relative_executable)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(resolved_executable, target)
    if not _is_windows_platform(platform_key):
        target.chmod(target.stat().st_mode | 0o755)

    resource["bundled"] = True
    resource["notes"] = (
        "Bundled by scripts/sidecar_platform_wheel.py for a platform wheel."
    )
    resource_files = [str(item) for item in manifest.get("resource_files", [])]
    if relative_executable not in resource_files:
        resource_files.append(relative_executable)
    manifest["resource_files"] = resource_files
    platforms = manifest.get("platforms", {})
    manifest["embedded_binary"] = any(
        bool(entry.get("bundled"))
        for entry in platforms.values()
        if isinstance(entry, dict)
    )
    _write_manifest(resolved_source, manifest)

    return {
        "platform": platform_key,
        "executable": str(target.relative_to(resolved_source / ASSET_ROOT)),
        "source_executable": str(resolved_executable),
        "size_bytes": target.stat().st_size,
        "version": version,
        "wheel_tags": list(resource.get("wheel_tags", [])),
    }


def _ignore_copy_names(directory: str, names: list[str]) -> set[str]:
    """Return top-level and generated names to exclude from the build copy."""
    ignored = {name for name in names if name in IGNORED_COPY_NAMES}
    ignored.update(name for name in names if name.endswith(".egg-info"))
    return ignored


def _copy_source_tree(source_root: Path, work_root: Path) -> Path:
    """Copy the source tree to an isolated build work directory."""
    staged_source = work_root / "source"
    if staged_source.exists():
        shutil.rmtree(staged_source)
    shutil.copytree(
        source_root,
        staged_source,
        ignore=_ignore_copy_names,
        symlinks=False,
    )
    return staged_source


def _single_wheel(dist_dir: Path) -> Path:
    """Return the only histdatacom wheel in a distribution directory."""
    wheels = sorted(dist_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def _retag_wheel(
    wheel_path: Path,
    *,
    platform_tag: str,
) -> Path:
    """Retag a pure build artifact as a platform wheel."""
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "wheel",
            "tags",
            "--remove",
            "--python-tag",
            DEFAULT_PYTHON_TAG,
            "--abi-tag",
            DEFAULT_ABI_TAG,
            "--platform-tag",
            platform_tag,
            str(wheel_path),
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"wheel retag failed for {wheel_path} with exit "
            f"{completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    outputs = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if outputs:
        candidate = Path(outputs[-1])
        return candidate if candidate.is_absolute() else wheel_path.parent / candidate

    retagged = sorted(wheel_path.parent.glob("histdatacom-*.whl"))
    if len(retagged) != 1:
        raise SystemExit(f"could not identify retagged wheel in {wheel_path.parent}")
    return retagged[0]


def _run(command: Sequence[str], *, cwd: Path) -> None:
    """Run a subprocess and fail with a useful command line."""
    completed = subprocess.run(command, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise SystemExit(
            f"command failed with exit {completed.returncode}: "
            f"{' '.join(command)}"
        )


def build_platform_wheel(
    *,
    source_root: Path,
    work_root: Path,
    dist_dir: Path,
    platform_key: str,
    executable: Path,
    platform_tag: str | None = None,
    check_version: bool = False,
    isolated: bool = False,
) -> dict[str, Any]:
    """Build a platform wheel from an explicit Temporal executable artifact."""
    staged_source = _copy_source_tree(source_root.resolve(), work_root)
    prepare_report = prepare_sidecar_binary(
        source_root=staged_source,
        platform_key=platform_key,
        executable=executable,
        check_version=check_version,
    )
    wheel_tags = [str(tag) for tag in prepare_report["wheel_tags"]]
    resolved_platform_tag = platform_tag or (wheel_tags[0] if wheel_tags else "")
    if not resolved_platform_tag:
        raise SystemExit(
            f"platform {platform_key!r} does not declare any wheel tags"
        )

    work_dist = work_root / "dist"
    work_dist.mkdir(parents=True, exist_ok=True)
    build_command = [
        sys.executable,
        "-m",
        "build",
        "--wheel",
        "--outdir",
        str(work_dist),
    ]
    if not isolated:
        build_command.append("--no-isolation")
    _run(build_command, cwd=staged_source)

    retagged = _retag_wheel(_single_wheel(work_dist), platform_tag=resolved_platform_tag)
    dist_dir.mkdir(parents=True, exist_ok=True)
    final_wheel = dist_dir / retagged.name
    shutil.copy2(retagged, final_wheel)
    return {
        "wheel": str(final_wheel),
        "platform": platform_key,
        "platform_tag": resolved_platform_tag,
        "staged_source": str(staged_source),
        "sidecar": prepare_report,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Build a platform-specific sidecar wheel."""
    parser = argparse.ArgumentParser(
        description=(
            "Build a histdatacom platform wheel with a bundled Temporal "
            "sidecar executable. The source tree is copied to a temporary "
            "staging directory before package data and manifest changes are "
            "applied."
        )
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path.cwd(),
        help="source tree root to copy before building",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="optional work directory to keep for inspection",
    )
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist-sidecar"),
        help="directory where the retagged platform wheel is copied",
    )
    parser.add_argument(
        "--platform-key",
        default=_current_platform_key(),
        help="sidecar manifest platform key to bundle",
    )
    parser.add_argument(
        "--platform-tag",
        help="wheel platform tag override; defaults to the manifest first tag",
    )
    parser.add_argument(
        "--executable",
        type=Path,
        required=True,
        help="Temporal executable artifact to bundle",
    )
    parser.add_argument(
        "--check-version",
        action="store_true",
        help="run '<executable> --version' before building",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="let python -m build create an isolated build environment",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="write a JSON report describing the built platform wheel",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.work_dir is None:
        with tempfile.TemporaryDirectory(prefix="histdatacom-sidecar-wheel-") as tmp:
            report = build_platform_wheel(
                source_root=args.source_root,
                work_root=Path(tmp),
                dist_dir=args.dist_dir,
                platform_key=args.platform_key,
                executable=args.executable,
                platform_tag=args.platform_tag,
                check_version=args.check_version,
                isolated=args.isolated,
            )
    else:
        args.work_dir.mkdir(parents=True, exist_ok=True)
        report = build_platform_wheel(
            source_root=args.source_root,
            work_root=args.work_dir,
            dist_dir=args.dist_dir,
            platform_key=args.platform_key,
            executable=args.executable,
            platform_tag=args.platform_tag,
            check_version=args.check_version,
            isolated=args.isolated,
        )

    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
