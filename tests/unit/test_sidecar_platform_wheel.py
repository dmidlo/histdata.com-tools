"""Tests for Temporal sidecar platform-wheel build helpers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType


def _load_script() -> ModuleType:
    """Load the platform wheel helper as a test module."""
    script_path = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "sidecar_platform_wheel.py"
    )
    spec = importlib.util.spec_from_file_location(
        "sidecar_platform_wheel", script_path
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_manifest(source_root: Path) -> None:
    """Write a minimal sidecar manifest into a temporary source tree."""
    asset_root = source_root / "src/histdatacom/sidecar/assets"
    asset_root.mkdir(parents=True)
    (asset_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sidecar": "temporal",
                "distribution_strategy": (
                    "platform-wheel-with-sdist-metadata-fallback"
                ),
                "embedded_binary": False,
                "resource_files": [
                    "README.md",
                    "manifest.json",
                    "runtime-defaults.json",
                ],
                "sdist_fallback": "metadata-only",
                "platforms": {
                    "macos-arm64": {
                        "bundled": False,
                        "executable": "bin/macos-arm64/temporal",
                        "wheel_tags": ["macosx_11_0_arm64"],
                        "notes": "planned",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_prepare_sidecar_binary_patches_manifest_and_copies_executable(
    tmp_path: Path,
) -> None:
    """A supplied Temporal binary should become a declared package asset."""
    module = _load_script()
    source_root = tmp_path / "source"
    _write_manifest(source_root)
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)

    report = module.prepare_sidecar_binary(
        source_root=source_root,
        platform_key="macos-arm64",
        executable=executable,
    )

    bundled = (
        source_root / "src/histdatacom/sidecar/assets/bin/macos-arm64/temporal"
    )
    manifest = json.loads(
        (
            source_root / "src/histdatacom/sidecar/assets/manifest.json"
        ).read_text(encoding="utf-8")
    )
    assert bundled.is_file()
    assert bundled.stat().st_mode & 0o111
    assert manifest["embedded_binary"] is True
    assert manifest["platforms"]["macos-arm64"]["bundled"] is True
    assert "bin/macos-arm64/temporal" in manifest["resource_files"]
    assert report["platform"] == "macos-arm64"
    assert report["executable"] == "bin/macos-arm64/temporal"
