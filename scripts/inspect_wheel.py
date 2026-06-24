"""Inspect built histdatacom wheels for package metadata and runtime assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from email.parser import Parser
from pathlib import Path
from typing import Any
from zipfile import ZipFile

EXPECTED_BASE_RUNTIME_ASSETS = {
    "histdatacom/sidecar/assets/README.md",
    "histdatacom/sidecar/assets/manifest.json",
    "histdatacom/sidecar/assets/runtime-defaults.json",
    "histdatacom/sidecar/assets/temporal-runtime-index.json",
    "histdatacom/sidecar/assets/third-party/temporal-cli/LICENSE",
    "histdatacom/sidecar/assets/third-party/temporal-cli/NOTICE.md",
}
EXPECTED_BASE_RUNTIME_RESOURCE_FILES = {
    "README.md",
    "manifest.json",
    "runtime-defaults.json",
    "temporal-runtime-index.json",
    "third-party/temporal-cli/LICENSE",
    "third-party/temporal-cli/NOTICE.md",
}
TEMPORAL_CLI_PROVENANCE_RESOURCE = "temporal-cli-provenance.json"
TEMPORAL_CLI_LICENSE_RESOURCE = "third-party/temporal-cli/LICENSE"
TEMPORAL_CLI_NOTICE_RESOURCE = "third-party/temporal-cli/NOTICE.md"
EXPECTED_RUNTIME_PLATFORMS = {
    "linux-arm64",
    "linux-x86_64",
    "macos-arm64",
    "macos-x86_64",
    "windows-x86_64",
}
EXPECTED_CONSOLE_SCRIPTS = {
    "histdatacom = histdatacom.histdata_com:main",
}
EXPECTED_METADATA_CLASSIFIERS = {
    "Operating System :: MacOS",
    "Operating System :: Microsoft :: Windows",
    "Operating System :: POSIX",
    "Operating System :: POSIX :: Linux",
}


def _current_platform_key(
    system: str | None = None,
    machine: str | None = None,
) -> str:
    """Return the runtime manifest platform key for this machine."""
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


def _single_wheel(dist_dir: Path) -> Path:
    wheels = sorted(dist_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def _requires_dist_contains(
    requires_dist: list[str],
    *,
    dependency: str,
    extra: str,
) -> bool:
    """Return whether a normalized requirement names a dependency extra."""
    expected_extra = f'extra == "{extra}"'
    return any(
        requirement.startswith(dependency) and expected_extra in requirement
        for requirement in requires_dist
    )


def _requires_dist_core_contains(
    requires_dist: list[str],
    *,
    dependency: str,
) -> bool:
    """Return whether a normalized requirement names a core dependency."""
    return any(
        requirement.startswith(dependency) and "extra ==" not in requirement
        for requirement in requires_dist
    )


def _sha256_bytes(payload: bytes) -> str:
    """Return the SHA-256 digest for in-wheel bytes."""
    return hashlib.sha256(payload).hexdigest()


def _manifest_mapping(
    data: dict[str, Any],
    key: str,
    *,
    context: str,
) -> dict[str, Any]:
    """Return a required nested manifest/provenance mapping."""
    value = data.get(key)
    if not isinstance(value, dict):
        raise SystemExit(f"{context} must define an object field: {key}")
    return value


def _require_string(
    data: dict[str, Any],
    key: str,
    *,
    context: str,
) -> str:
    """Return a required non-empty string field."""
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"{context} must define a string field: {key}")
    return value


def _validate_sha256(value: str, *, context: str, field: str) -> None:
    """Fail if a provenance checksum is not a SHA-256 hex digest."""
    if len(value) != 64:
        raise SystemExit(f"{context} field {field!r} is not a SHA-256 digest")
    try:
        int(value, 16)
    except ValueError as err:
        raise SystemExit(f"{context} field {field!r} is not hex") from err


def _validate_third_party_notice_manifest(manifest: dict[str, Any]) -> None:
    """Validate top-level third-party notice metadata."""
    notices = _manifest_mapping(
        manifest,
        "third_party_notices",
        context="runtime manifest",
    )
    temporal_cli = _manifest_mapping(
        notices,
        "temporal_cli",
        context="runtime manifest third_party_notices",
    )
    if temporal_cli.get("license") != "MIT":
        raise SystemExit("Temporal CLI third-party notice must declare MIT")
    if temporal_cli.get("license_file") != TEMPORAL_CLI_LICENSE_RESOURCE:
        raise SystemExit("Temporal CLI third-party notice has wrong license file")
    if temporal_cli.get("notice_file") != TEMPORAL_CLI_NOTICE_RESOURCE:
        raise SystemExit("Temporal CLI third-party notice has wrong notice file")
    if (
        temporal_cli.get("bundled_provenance_file")
        != TEMPORAL_CLI_PROVENANCE_RESOURCE
    ):
        raise SystemExit(
            "Temporal CLI third-party notice has wrong bundled provenance file"
        )


def _validate_temporal_cli_provenance(
    *,
    wheel: ZipFile,
    names: set[str],
    platform_key: str,
    executable_resource: str,
    executable_path: str,
    provenance_resource: str,
) -> dict[str, Any]:
    """Validate packaged provenance for one bundled platform executable."""
    if provenance_resource != TEMPORAL_CLI_PROVENANCE_RESOURCE:
        raise SystemExit(
            f"runtime platform {platform_key} has unexpected provenance "
            f"resource: {provenance_resource}"
        )
    provenance_path = f"histdatacom/sidecar/assets/{provenance_resource}"
    if provenance_path not in names:
        raise SystemExit(
            f"runtime provenance missing for {platform_key}: {provenance_path}"
        )
    for required_resource in (
        TEMPORAL_CLI_LICENSE_RESOURCE,
        TEMPORAL_CLI_NOTICE_RESOURCE,
    ):
        required_path = f"histdatacom/sidecar/assets/{required_resource}"
        if required_path not in names:
            raise SystemExit(
                f"runtime third-party notice resource missing: {required_path}"
            )

    loaded = json.loads(wheel.read(provenance_path).decode("utf-8"))
    if not isinstance(loaded, dict):
        raise SystemExit(f"runtime provenance must be an object: {provenance_path}")
    provenance: dict[str, Any] = loaded
    if provenance.get("schema_version") != 1:
        raise SystemExit("Temporal CLI provenance schema_version must be 1")
    if provenance.get("component") != "temporal-cli":
        raise SystemExit("Temporal CLI provenance component mismatch")
    if provenance.get("bundled") is not True:
        raise SystemExit("Temporal CLI provenance must declare bundled true")
    if provenance.get("platform") != platform_key:
        raise SystemExit(
            f"Temporal CLI provenance platform mismatch for {platform_key}"
        )
    _require_string(provenance, "version", context="Temporal CLI provenance")

    upstream = _manifest_mapping(
        provenance,
        "upstream",
        context="Temporal CLI provenance",
    )
    if upstream.get("license") != "MIT":
        raise SystemExit("Temporal CLI provenance must declare MIT license")
    if upstream.get("license_file") != TEMPORAL_CLI_LICENSE_RESOURCE:
        raise SystemExit("Temporal CLI provenance has wrong license file")
    if upstream.get("notice_file") != TEMPORAL_CLI_NOTICE_RESOURCE:
        raise SystemExit("Temporal CLI provenance has wrong notice file")
    _require_string(upstream, "repository", context="Temporal CLI provenance")
    _require_string(upstream, "license_url", context="Temporal CLI provenance")

    release_asset = _manifest_mapping(
        provenance,
        "release_asset",
        context="Temporal CLI provenance",
    )
    _require_string(release_asset, "name", context="Temporal CLI provenance")
    _require_string(release_asset, "url", context="Temporal CLI provenance")
    expected_sha = _require_string(
        release_asset,
        "sha256_expected",
        context="Temporal CLI provenance",
    )
    actual_sha = _require_string(
        release_asset,
        "sha256_actual",
        context="Temporal CLI provenance",
    )
    _validate_sha256(
        expected_sha,
        context="Temporal CLI provenance",
        field="release_asset.sha256_expected",
    )
    _validate_sha256(
        actual_sha,
        context="Temporal CLI provenance",
        field="release_asset.sha256_actual",
    )
    if expected_sha.lower() != actual_sha.lower():
        raise SystemExit("Temporal CLI release asset checksum was not verified")
    if release_asset.get("sha256_verified") is not True:
        raise SystemExit("Temporal CLI release asset must be marked verified")

    executable = _manifest_mapping(
        provenance,
        "executable",
        context="Temporal CLI provenance",
    )
    if executable.get("resource_path") != executable_resource:
        raise SystemExit(
            f"Temporal CLI provenance executable path mismatch for {platform_key}"
        )
    executable_bytes = wheel.read(executable_path)
    executable_sha = _require_string(
        executable,
        "sha256",
        context="Temporal CLI provenance",
    )
    _validate_sha256(
        executable_sha,
        context="Temporal CLI provenance",
        field="executable.sha256",
    )
    if executable_sha.lower() != _sha256_bytes(executable_bytes):
        raise SystemExit(
            f"Temporal CLI provenance executable checksum mismatch for {platform_key}"
        )
    if executable.get("size_bytes") != len(executable_bytes):
        raise SystemExit(
            f"Temporal CLI provenance executable size mismatch for {platform_key}"
        )
    return provenance


def inspect_wheel(
    wheel_path: Path,
    *,
    require_bundled_platforms: set[str] | None = None,
    require_current_platform_bundled: bool = False,
) -> dict[str, Any]:
    """Validate wheel metadata, entry points, and runtime resource payloads."""
    required_bundled_platforms = set(require_bundled_platforms or set())
    if require_current_platform_bundled:
        required_bundled_platforms.add(_current_platform_key())

    with ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        metadata_path = next(
            name for name in names if name.endswith(".dist-info/METADATA")
        )
        wheel_metadata_path = next(
            name for name in names if name.endswith(".dist-info/WHEEL")
        )
        entry_points_path = next(
            name
            for name in names
            if name.endswith(".dist-info/entry_points.txt")
        )
        missing = sorted(EXPECTED_BASE_RUNTIME_ASSETS - names)
        if missing:
            raise SystemExit(f"wheel missing runtime assets: {missing}")

        wheel_metadata = Parser().parsestr(
            wheel.read(metadata_path).decode("utf-8")
        )
        wheel_file_metadata = Parser().parsestr(
            wheel.read(wheel_metadata_path).decode("utf-8")
        )
        entry_points = wheel.read(entry_points_path).decode("utf-8")
        manifest = json.loads(
            wheel.read("histdatacom/sidecar/assets/manifest.json").decode(
                "utf-8"
            )
        )
        _validate_third_party_notice_manifest(manifest)
        manifest_platforms = set(dict(manifest["platforms"]))
        missing_platforms = sorted(
            EXPECTED_RUNTIME_PLATFORMS - manifest_platforms
        )
        if missing_platforms:
            raise SystemExit(
                "runtime manifest is missing platform declarations: "
                f"{missing_platforms}"
            )
        bundled_platforms: set[str] = set()
        provenance_reports: dict[str, Any] = {}
        expected_resource_files = set(EXPECTED_BASE_RUNTIME_RESOURCE_FILES)
        for key, resource in dict(manifest["platforms"]).items():
            executable = resource.get("executable")
            if not executable:
                raise SystemExit(f"runtime platform {key} has no executable")
            executable_path = f"histdatacom/sidecar/assets/{executable}"
            if resource.get("bundled"):
                bundled_platforms.add(str(key))
                expected_resource_files.add(str(executable))
                expected_resource_files.add(TEMPORAL_CLI_PROVENANCE_RESOURCE)
                if resource.get("license") != TEMPORAL_CLI_LICENSE_RESOURCE:
                    raise SystemExit(
                        f"runtime platform {key} has wrong license resource"
                    )
                if resource.get("notice") != TEMPORAL_CLI_NOTICE_RESOURCE:
                    raise SystemExit(
                        f"runtime platform {key} has wrong notice resource"
                    )
                provenance = resource.get("provenance")
                if not isinstance(provenance, str) or not provenance:
                    raise SystemExit(
                        f"runtime platform {key} is bundled without provenance"
                    )
                if executable_path not in names:
                    raise SystemExit(
                        f"runtime executable missing for {key}: "
                        f"{executable_path}"
                    )
                info = wheel.getinfo(executable_path)
                mode = (info.external_attr >> 16) & 0o777
                if mode and mode & 0o111 == 0:
                    raise SystemExit(
                        f"runtime executable is not executable for {key}: "
                        f"{executable_path}"
                    )
                provenance_reports[str(key)] = _validate_temporal_cli_provenance(
                    wheel=wheel,
                    names=names,
                    platform_key=str(key),
                    executable_resource=str(executable),
                    executable_path=executable_path,
                    provenance_resource=provenance,
                )
            elif executable_path in names:
                raise SystemExit(
                    f"runtime executable is packaged but not declared bundled "
                    f"for {key}: {executable_path}"
                )
            elif resource.get("provenance"):
                raise SystemExit(
                    f"runtime platform {key} declares provenance but is not bundled"
                )
        provenance_asset = (
            f"histdatacom/sidecar/assets/{TEMPORAL_CLI_PROVENANCE_RESOURCE}"
        )
        if not bundled_platforms and provenance_asset in names:
            raise SystemExit(
                "metadata-only runtime wheel must not package bundled provenance"
            )
        unexpected_resource_files = sorted(
            expected_resource_files ^ set(manifest.get("resource_files", []))
        )
        if unexpected_resource_files:
            raise SystemExit(
                "runtime manifest resource_files drifted from packaged "
                f"assets: {unexpected_resource_files}"
            )

    if wheel_metadata["Name"] != "histdatacom":
        raise SystemExit(f"unexpected wheel name: {wheel_metadata['Name']}")
    if wheel_metadata["Requires-Python"] != ">=3.10.0":
        raise SystemExit(
            f"unexpected Python requirement: {wheel_metadata['Requires-Python']}"
        )
    for console_script in sorted(EXPECTED_CONSOLE_SCRIPTS):
        if console_script not in entry_points:
            raise SystemExit(
                f"console script missing from wheel metadata: {console_script}"
            )
    provides_extra = set(wheel_metadata.get_all("Provides-Extra", []))
    if "temporal" not in provides_extra:
        raise SystemExit("temporal optional extra missing from wheel metadata")
    classifiers = set(wheel_metadata.get_all("Classifier", []))
    missing_classifiers = sorted(
        EXPECTED_METADATA_CLASSIFIERS - classifiers
    )
    if missing_classifiers:
        raise SystemExit(
            "wheel metadata missing platform classifiers: "
            f"{missing_classifiers}"
        )
    requires_dist = [
        requirement.lower()
        for requirement in wheel_metadata.get_all("Requires-Dist", [])
    ]
    if not _requires_dist_core_contains(
        requires_dist,
        dependency="temporalio",
    ):
        raise SystemExit("temporalio dependency missing from core metadata")
    if not _requires_dist_contains(
        requires_dist,
        dependency="temporalio",
        extra="temporal",
    ):
        raise SystemExit("temporalio dependency missing from temporal extra")
    if not _requires_dist_contains(
        requires_dist,
        dependency="temporalio",
        extra="all",
    ):
        raise SystemExit("temporalio dependency missing from all extra")
    if manifest["runtime"] != "temporal":
        raise SystemExit("runtime manifest does not describe Temporal")
    if manifest["distribution_strategy"] != (
        "metadata-wheel-with-verified-runtime-provisioning"
    ):
        raise SystemExit("unexpected runtime distribution strategy")
    embedded_binary = bool(manifest["embedded_binary"])
    if embedded_binary != bool(bundled_platforms):
        raise SystemExit(
            "runtime manifest embedded_binary does not match bundled "
            f"platforms: {sorted(bundled_platforms)}"
        )
    missing_required = sorted(required_bundled_platforms - bundled_platforms)
    if missing_required:
        raise SystemExit(
            "wheel is missing required bundled runtime platforms: "
            f"{missing_required}"
        )
    wheel_tags = list(wheel_file_metadata.get_all("Tag", []))
    if embedded_binary and all(tag.endswith("-any") for tag in wheel_tags):
        raise SystemExit(
            "bundled runtime executable wheels must use platform wheel tags"
        )
    return {
        "wheel": wheel_path.name,
        "classifiers": sorted(classifiers),
        "name": wheel_metadata["Name"],
        "requires_python": wheel_metadata["Requires-Python"],
        "provides_extra": sorted(provides_extra),
        "runtime": {
            "assets": sorted(EXPECTED_BASE_RUNTIME_ASSETS),
            "bundled_platforms": sorted(bundled_platforms),
            "distribution_strategy": manifest["distribution_strategy"],
            "embedded_binary": manifest["embedded_binary"],
            "platforms": sorted(manifest_platforms),
            "provenance": provenance_reports,
            "resource_files": list(manifest["resource_files"]),
        },
        "console_scripts": sorted(EXPECTED_CONSOLE_SCRIPTS),
        "wheel_tags": wheel_tags,
    }


def main() -> None:
    """Inspect the wheel in a distribution directory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-dir", default="dist")
    parser.add_argument(
        "--wheel",
        type=Path,
        help="inspect an explicit wheel instead of the only wheel in dist-dir",
    )
    parser.add_argument(
        "--require-bundled-current-platform",
        action="store_true",
        help="require the current platform to have a bundled executable",
    )
    parser.add_argument(
        "--require-bundled-platform",
        action="append",
        default=[],
        help="require a specific manifest platform key to be bundled",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="write a JSON report describing inspected wheel metadata",
    )
    args = parser.parse_args()
    wheel_path = args.wheel or _single_wheel(Path(args.dist_dir))
    report = inspect_wheel(
        wheel_path,
        require_bundled_platforms=set(args.require_bundled_platform),
        require_current_platform_bundled=args.require_bundled_current_platform,
    )
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
