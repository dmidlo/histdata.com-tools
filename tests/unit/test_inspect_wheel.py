"""Tests for wheel sidecar resource inspection."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from zipfile import ZipFile, ZipInfo


def _load_script() -> ModuleType:
    """Load the wheel inspector as a test module."""
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts/inspect_wheel.py"
    )
    spec = importlib.util.spec_from_file_location("inspect_wheel", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wheel_info(path: str, mode: int = 0o644) -> ZipInfo:
    """Create a zip member with Unix permission metadata."""
    info = ZipInfo(path)
    info.external_attr = mode << 16
    return info


def test_inspect_wheel_accepts_bundled_platform_executable(
    tmp_path: Path,
) -> None:
    """Bundled platform wheels should pass resource and tag checks."""
    module = _load_script()
    manifest_path = (
        Path(__file__).resolve().parents[2]
        / "src/histdatacom/sidecar/assets/manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["embedded_binary"] = True
    manifest["resource_files"].append("bin/macos-arm64/temporal")
    manifest["platforms"]["macos-arm64"]["bundled"] = True
    manifest["platforms"]["macos-arm64"]["notes"] = "test bundled executable"
    wheel_path = tmp_path / "histdatacom-1.0.0-py3-none-macosx_11_0_arm64.whl"

    with ZipFile(wheel_path, "w") as wheel:
        wheel.writestr(
            "histdatacom/sidecar/assets/README.md",
            "sidecar assets",
        )
        wheel.writestr(
            "histdatacom/sidecar/assets/runtime-defaults.json",
            "{}",
        )
        wheel.writestr(
            "histdatacom/sidecar/assets/manifest.json",
            json.dumps(manifest),
        )
        wheel.writestr(
            _wheel_info(
                "histdatacom/sidecar/assets/bin/macos-arm64/temporal",
                mode=0o755,
            ),
            b"#!/bin/sh\n",
        )
        wheel.writestr(
            "histdatacom-1.0.0.dist-info/METADATA",
            "\n".join(
                [
                    "Metadata-Version: 2.4",
                    "Name: histdatacom",
                    "Version: 1.0.0",
                    "Requires-Python: >=3.10.0",
                    "Provides-Extra: temporal",
                    "Provides-Extra: all",
                    'Requires-Dist: temporalio>=1.10,<2; extra == "temporal"',
                    'Requires-Dist: temporalio>=1.10,<2; extra == "all"',
                    "",
                ]
            ),
        )
        wheel.writestr(
            "histdatacom-1.0.0.dist-info/entry_points.txt",
            "\n".join(
                [
                    "[console_scripts]",
                    "histdatacom = histdatacom.histdata_com:main",
                    "histdatacom-sidecar = histdatacom.sidecar.cli:main",
                    (
                        "histdatacom-sidecar-worker = "
                        "histdatacom.sidecar.worker:main"
                    ),
                    "",
                ]
            ),
        )
        wheel.writestr(
            "histdatacom-1.0.0.dist-info/WHEEL",
            "\n".join(
                [
                    "Wheel-Version: 1.0",
                    "Generator: test",
                    "Root-Is-Purelib: false",
                    "Tag: py3-none-macosx_11_0_arm64",
                    "",
                ]
            ),
        )
        wheel.writestr("histdatacom-1.0.0.dist-info/RECORD", "")

    report = module.inspect_wheel(
        wheel_path,
        require_bundled_platforms={"macos-arm64"},
    )

    assert report["sidecar"]["embedded_binary"] is True
    assert report["sidecar"]["bundled_platforms"] == ["macos-arm64"]
    assert report["wheel_tags"] == ["py3-none-macosx_11_0_arm64"]
