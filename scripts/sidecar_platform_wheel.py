"""Build platform wheels with a bundled Temporal sidecar executable."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

ASSET_ROOT = Path("src/histdatacom/sidecar/assets")
MANIFEST_FILENAME = "manifest.json"
TEMPORAL_CLI_COMPONENT = "temporal-cli"
TEMPORAL_CLI_REPOSITORY = "https://github.com/temporalio/cli"
TEMPORAL_CLI_LICENSE = "MIT"
TEMPORAL_CLI_LICENSE_URL = "https://github.com/temporalio/cli/blob/main/LICENSE"
TEMPORAL_CLI_LICENSE_RESOURCE = "third-party/temporal-cli/LICENSE"
TEMPORAL_CLI_NOTICE_RESOURCE = "third-party/temporal-cli/NOTICE.md"
PROVENANCE_FILENAME = "temporal-cli-provenance.json"
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


def _load_fetch_report(report_path: Path) -> dict[str, Any]:
    """Load a Temporal CLI fetch report from release packaging."""
    try:
        loaded = json.loads(report_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise SystemExit(f"Temporal CLI fetch report not found: {report_path}") from err
    if not isinstance(loaded, dict):
        raise SystemExit(f"Temporal CLI fetch report must be an object: {report_path}")
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


def _sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _report_string(report: Mapping[str, Any], key: str) -> str:
    """Return a required non-empty string from a fetch report."""
    value = report.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"Temporal CLI fetch report missing string field: {key}")
    return value


def _validate_sha256(value: str, *, field: str) -> None:
    """Fail if a fetch report checksum is not a SHA-256 hex digest."""
    if len(value) != 64:
        raise SystemExit(
            f"Temporal CLI fetch report field {field!r} is not a SHA-256 "
            f"digest: {value!r}"
        )
    try:
        int(value, 16)
    except ValueError as err:
        raise SystemExit(
            f"Temporal CLI fetch report field {field!r} is not hex: {value!r}"
        ) from err


def _validate_fetch_report(
    fetch_report: Mapping[str, Any],
    *,
    platform_key: str,
) -> dict[str, str]:
    """Normalize and validate a Temporal CLI fetch report for packaging."""
    normalized = {
        "platform": _report_string(fetch_report, "platform"),
        "version": _report_string(fetch_report, "version"),
        "asset": _report_string(fetch_report, "asset"),
        "url": _report_string(fetch_report, "url"),
        "sha256": _report_string(fetch_report, "sha256"),
        "expected_sha256": _report_string(fetch_report, "expected_sha256"),
        "upstream_repository": str(
            fetch_report.get("upstream_repository") or TEMPORAL_CLI_REPOSITORY
        ),
        "license": str(fetch_report.get("license") or TEMPORAL_CLI_LICENSE),
        "license_url": str(
            fetch_report.get("license_url") or TEMPORAL_CLI_LICENSE_URL
        ),
    }
    if normalized["platform"] != platform_key:
        raise SystemExit(
            "Temporal CLI fetch report platform does not match requested "
            f"platform: {normalized['platform']!r} != {platform_key!r}"
        )
    _validate_sha256(normalized["sha256"], field="sha256")
    _validate_sha256(normalized["expected_sha256"], field="expected_sha256")
    if normalized["sha256"].lower() != normalized["expected_sha256"].lower():
        raise SystemExit(
            "Temporal CLI fetch report does not describe a verified archive: "
            f"{normalized['sha256']} != {normalized['expected_sha256']}"
        )
    return normalized


def _ensure_asset_file(source_root: Path, relative_path: str) -> None:
    """Fail if a required sidecar asset file is missing from the source tree."""
    asset_path = _asset_path(source_root, relative_path)
    if not asset_path.is_file():
        raise SystemExit(f"sidecar asset is required for bundled wheels: {relative_path}")


def _merge_resource_files(
    existing: Sequence[Any],
    additional: Sequence[str],
) -> list[str]:
    """Return manifest resource files with stable order and no duplicates."""
    merged: list[str] = []
    seen: set[str] = set()
    for item in [*(str(value) for value in existing), *additional]:
        if item not in seen:
            merged.append(item)
            seen.add(item)
    return merged


def _write_temporal_cli_provenance(
    *,
    source_root: Path,
    platform_key: str,
    relative_executable: str,
    target: Path,
    fetch_report: Mapping[str, str],
    version_probe: str,
) -> dict[str, Any]:
    """Write packaged provenance for a bundled Temporal CLI executable."""
    stat_result = target.stat()
    provenance = {
        "schema_version": 1,
        "component": TEMPORAL_CLI_COMPONENT,
        "bundled": True,
        "platform": platform_key,
        "version": fetch_report["version"],
        "upstream": {
            "repository": fetch_report["upstream_repository"],
            "license": fetch_report["license"],
            "license_url": fetch_report["license_url"],
            "license_file": TEMPORAL_CLI_LICENSE_RESOURCE,
            "notice_file": TEMPORAL_CLI_NOTICE_RESOURCE,
        },
        "release_asset": {
            "name": fetch_report["asset"],
            "url": fetch_report["url"],
            "sha256_expected": fetch_report["expected_sha256"],
            "sha256_actual": fetch_report["sha256"],
            "sha256_verified": True,
        },
        "executable": {
            "resource_path": relative_executable,
            "sha256": _sha256_file(target),
            "size_bytes": stat_result.st_size,
            "version_probe": version_probe,
        },
        "builder": "scripts/sidecar_platform_wheel.py",
    }
    provenance_path = _asset_path(source_root, PROVENANCE_FILENAME)
    provenance_path.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return provenance


def prepare_sidecar_binary(
    *,
    source_root: Path,
    platform_key: str,
    executable: Path,
    fetch_report: Mapping[str, Any],
    check_version: bool = False,
) -> dict[str, Any]:
    """Copy a Temporal executable into a source tree and patch the manifest."""
    resolved_source = source_root.resolve()
    resolved_executable = executable.expanduser().resolve()
    if not resolved_executable.is_file():
        raise SystemExit(f"Temporal executable is not a file: {resolved_executable}")

    manifest = _load_manifest(resolved_source)
    normalized_fetch_report = _validate_fetch_report(
        fetch_report,
        platform_key=platform_key,
    )
    _ensure_asset_file(resolved_source, TEMPORAL_CLI_LICENSE_RESOURCE)
    _ensure_asset_file(resolved_source, TEMPORAL_CLI_NOTICE_RESOURCE)
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

    provenance = _write_temporal_cli_provenance(
        source_root=resolved_source,
        platform_key=platform_key,
        relative_executable=relative_executable,
        target=target,
        fetch_report=normalized_fetch_report,
        version_probe=version,
    )
    resource["bundled"] = True
    resource["provenance"] = PROVENANCE_FILENAME
    resource["license"] = TEMPORAL_CLI_LICENSE_RESOURCE
    resource["notice"] = TEMPORAL_CLI_NOTICE_RESOURCE
    resource["notes"] = (
        "Bundled by scripts/sidecar_platform_wheel.py for a platform wheel."
    )
    manifest["resource_files"] = _merge_resource_files(
        manifest.get("resource_files", []),
        [
            relative_executable,
            PROVENANCE_FILENAME,
            TEMPORAL_CLI_LICENSE_RESOURCE,
            TEMPORAL_CLI_NOTICE_RESOURCE,
        ],
    )
    platforms = manifest.get("platforms", {})
    manifest["embedded_binary"] = any(
        bool(entry.get("bundled"))
        for entry in platforms.values()
        if isinstance(entry, dict)
    )
    _write_manifest(resolved_source, manifest)

    return {
        "platform": platform_key,
        "executable": relative_executable,
        "source_executable": str(resolved_executable),
        "size_bytes": target.stat().st_size,
        "version": version,
        "provenance": PROVENANCE_FILENAME,
        "license": TEMPORAL_CLI_LICENSE_RESOURCE,
        "notice": TEMPORAL_CLI_NOTICE_RESOURCE,
        "upstream_url": normalized_fetch_report["url"],
        "archive_sha256": normalized_fetch_report["sha256"],
        "executable_sha256": provenance["executable"]["sha256"],
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
    fetch_report: Mapping[str, Any],
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
        fetch_report=fetch_report,
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
        "--fetch-report",
        type=Path,
        required=True,
        help="JSON report from scripts/fetch_temporal_cli.py for this artifact",
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
    fetch_report = _load_fetch_report(args.fetch_report)

    if args.work_dir is None:
        with tempfile.TemporaryDirectory(prefix="histdatacom-sidecar-wheel-") as tmp:
            report = build_platform_wheel(
                source_root=args.source_root,
                work_root=Path(tmp),
                dist_dir=args.dist_dir,
                platform_key=args.platform_key,
                executable=args.executable,
                fetch_report=fetch_report,
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
            fetch_report=fetch_report,
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
