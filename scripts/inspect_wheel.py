"""Inspect built histdatacom wheels for package metadata and sidecar assets."""

from __future__ import annotations

import argparse
import json
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

EXPECTED_SIDECAR_ASSETS = {
    "histdatacom/sidecar/assets/README.md",
    "histdatacom/sidecar/assets/manifest.json",
    "histdatacom/sidecar/assets/runtime-defaults.json",
}


def _single_wheel(dist_dir: Path) -> Path:
    wheels = sorted(dist_dir.glob("histdatacom-*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"expected exactly one wheel, found {wheels}")
    return wheels[0]


def inspect_wheel(wheel_path: Path) -> None:
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
    if "histdatacom = histdatacom.histdata_com:main" not in entry_points:
        raise SystemExit(
            "histdatacom console script missing from wheel metadata"
        )
    if "histdatacom-sidecar = histdatacom.sidecar.cli:main" not in entry_points:
        raise SystemExit(
            "histdatacom-sidecar console script missing from wheel metadata"
        )
    if (
        "histdatacom-sidecar-worker = histdatacom.sidecar.worker:main"
        not in entry_points
    ):
        raise SystemExit(
            "histdatacom-sidecar-worker console script missing from "
            "wheel metadata"
        )
    if "temporal" not in set(wheel_metadata.get_all("Provides-Extra", [])):
        raise SystemExit("temporal optional extra missing from wheel metadata")
    requires_dist = [
        requirement.lower()
        for requirement in wheel_metadata.get_all("Requires-Dist", [])
    ]
    if not any(
        requirement.startswith("temporalio")
        and 'extra == "temporal"' in requirement
        for requirement in requires_dist
    ):
        raise SystemExit("temporalio dependency missing from temporal extra")
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
    if "linux-x86_64" not in manifest["platforms"]:
        raise SystemExit("sidecar manifest is missing linux-x86_64")


def main() -> None:
    """Inspect the wheel in a distribution directory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-dir", default="dist")
    args = parser.parse_args()
    inspect_wheel(_single_wheel(Path(args.dist_dir)))


if __name__ == "__main__":
    main()
