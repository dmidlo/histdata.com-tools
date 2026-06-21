"""Inspect built histdatacom wheels for package metadata and sidecar assets."""

from __future__ import annotations

import argparse
import json
from email.parser import Parser
from pathlib import Path
from typing import Any
from zipfile import ZipFile

EXPECTED_SIDECAR_ASSETS = {
    "histdatacom/sidecar/assets/README.md",
    "histdatacom/sidecar/assets/manifest.json",
    "histdatacom/sidecar/assets/runtime-defaults.json",
}
EXPECTED_SIDECAR_RESOURCE_FILES = {
    "README.md",
    "manifest.json",
    "runtime-defaults.json",
}
EXPECTED_SIDECAR_PLATFORMS = {
    "linux-arm64",
    "linux-x86_64",
    "macos-arm64",
    "macos-x86_64",
    "windows-x86_64",
}
EXPECTED_CONSOLE_SCRIPTS = {
    "histdatacom = histdatacom.histdata_com:main",
    "histdatacom-sidecar = histdatacom.sidecar.cli:main",
    "histdatacom-sidecar-worker = histdatacom.sidecar.worker:main",
}


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


def inspect_wheel(wheel_path: Path) -> dict[str, Any]:
    """Validate wheel metadata, entry points, and sidecar resource payloads."""
    with ZipFile(wheel_path) as wheel:
        names = set(wheel.namelist())
        metadata_path = next(
            name for name in names if name.endswith(".dist-info/METADATA")
        )
        entry_points_path = next(
            name
            for name in names
            if name.endswith(".dist-info/entry_points.txt")
        )
        missing = sorted(EXPECTED_SIDECAR_ASSETS - names)
        if missing:
            raise SystemExit(f"wheel missing sidecar assets: {missing}")

        wheel_metadata = Parser().parsestr(
            wheel.read(metadata_path).decode("utf-8")
        )
        entry_points = wheel.read(entry_points_path).decode("utf-8")
        manifest = json.loads(
            wheel.read("histdatacom/sidecar/assets/manifest.json").decode(
                "utf-8"
            )
        )
        manifest_platforms = set(dict(manifest["platforms"]))
        missing_platforms = sorted(
            EXPECTED_SIDECAR_PLATFORMS - manifest_platforms
        )
        if missing_platforms:
            raise SystemExit(
                "sidecar manifest is missing platform declarations: "
                f"{missing_platforms}"
            )
        unexpected_resource_files = sorted(
            EXPECTED_SIDECAR_RESOURCE_FILES
            ^ set(manifest.get("resource_files", []))
        )
        if unexpected_resource_files:
            raise SystemExit(
                "sidecar manifest resource_files drifted from packaged "
                f"assets: {unexpected_resource_files}"
            )
        for key, resource in dict(manifest["platforms"]).items():
            executable = resource.get("executable")
            if not executable:
                raise SystemExit(f"sidecar platform {key} has no executable")
            executable_path = f"histdatacom/sidecar/assets/{executable}"
            if resource.get("bundled"):
                if executable_path not in names:
                    raise SystemExit(
                        f"sidecar executable missing for {key}: "
                        f"{executable_path}"
                    )
                info = wheel.getinfo(executable_path)
                mode = (info.external_attr >> 16) & 0o777
                if mode and mode & 0o111 == 0:
                    raise SystemExit(
                        f"sidecar executable is not executable for {key}: "
                        f"{executable_path}"
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
    requires_dist = [
        requirement.lower()
        for requirement in wheel_metadata.get_all("Requires-Dist", [])
    ]
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
    if manifest["sidecar"] != "temporal":
        raise SystemExit("sidecar manifest does not describe Temporal")
    if manifest["distribution_strategy"] != (
        "platform-wheel-with-sdist-metadata-fallback"
    ):
        raise SystemExit("unexpected sidecar distribution strategy")
    if manifest["embedded_binary"]:
        raise SystemExit(
            "metadata-only wheel should not claim bundled binaries"
        )
    return {
        "wheel": wheel_path.name,
        "name": wheel_metadata["Name"],
        "requires_python": wheel_metadata["Requires-Python"],
        "provides_extra": sorted(provides_extra),
        "sidecar": {
            "assets": sorted(EXPECTED_SIDECAR_ASSETS),
            "distribution_strategy": manifest["distribution_strategy"],
            "embedded_binary": manifest["embedded_binary"],
            "platforms": sorted(manifest_platforms),
            "resource_files": list(manifest["resource_files"]),
        },
        "console_scripts": sorted(EXPECTED_CONSOLE_SCRIPTS),
    }


def main() -> None:
    """Inspect the wheel in a distribution directory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-dir", default="dist")
    parser.add_argument(
        "--report",
        type=Path,
        help="write a JSON report describing inspected wheel metadata",
    )
    args = parser.parse_args()
    report = inspect_wheel(_single_wheel(Path(args.dist_dir)))
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
