"""Tests for Temporal sidecar platform-wheel build helpers."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import os
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
    (asset_root / "third-party/temporal-cli").mkdir(parents=True)
    (asset_root / "third-party/temporal-cli/LICENSE").write_text(
        "MIT\n",
        encoding="utf-8",
    )
    (asset_root / "third-party/temporal-cli/NOTICE.md").write_text(
        "Temporal CLI notice\n",
        encoding="utf-8",
    )
    (asset_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sidecar": "temporal",
                "distribution_strategy": (
                    "metadata-wheel-with-verified-runtime-provisioning"
                ),
                "embedded_binary": False,
                "resource_files": [
                    "README.md",
                    "manifest.json",
                    "runtime-defaults.json",
                    "third-party/temporal-cli/LICENSE",
                    "third-party/temporal-cli/NOTICE.md",
                ],
                "third_party_notices": {
                    "temporal_cli": {
                        "name": "Temporal CLI",
                        "license": "MIT",
                        "upstream_repository": "https://github.com/temporalio/cli",
                        "license_file": "third-party/temporal-cli/LICENSE",
                        "notice_file": "third-party/temporal-cli/NOTICE.md",
                        "bundled_provenance_file": (
                            "temporal-cli-provenance.json"
                        ),
                    }
                },
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


def _fetch_report() -> dict[str, str]:
    """Return a verified Temporal CLI fetch report fixture."""
    checksum = "1" * 64
    return {
        "platform": "macos-arm64",
        "version": "1.7.2",
        "asset": "temporal_cli_1.7.2_darwin_arm64.tar.gz",
        "url": (
            "https://github.com/temporalio/cli/releases/download/v1.7.2/"
            "temporal_cli_1.7.2_darwin_arm64.tar.gz"
        ),
        "sha256": checksum,
        "expected_sha256": checksum,
        "upstream_repository": "https://github.com/temporalio/cli",
        "license": "MIT",
        "license_url": "https://github.com/temporalio/cli/blob/main/LICENSE",
    }


def test_copy_source_tree_excludes_local_data_and_release_state(
    tmp_path: Path,
) -> None:
    """Platform-wheel staging should copy package inputs, not local artifacts."""
    module = _load_script()
    source_root = tmp_path / "repo"
    (source_root / "src/histdatacom").mkdir(parents=True)
    (source_root / "src/histdatacom/__init__.py").write_text(
        "__version__ = '0.0.0'\n",
        encoding="utf-8",
    )
    (source_root / "pyproject.toml").write_text(
        "[project]\nname = 'histdatacom'\n",
        encoding="utf-8",
    )
    for path in (
        ".git/config",
        ".pypirc",
        ".temporal-cli/macos-arm64/bin/temporal",
        ".coverage.123",
        "build/temp.txt",
        "data/.repo",
        "data/ASCII/M1/eurusd/2026/DAT_ASCII_EURUSD_M1_202606.csv",
        "dist/histdatacom-0.0.0.tar.gz",
        "src/histdatacom.egg-info/PKG-INFO",
        "venv/pyvenv.cfg",
    ):
        candidate = source_root / path
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_text("local state\n", encoding="utf-8")

    staged = module._copy_source_tree(source_root, tmp_path / "work")

    assert (staged / "pyproject.toml").is_file()
    assert (staged / "src/histdatacom/__init__.py").is_file()
    assert not (staged / ".git").exists()
    assert not (staged / ".pypirc").exists()
    assert not (staged / ".temporal-cli").exists()
    assert not (staged / ".coverage.123").exists()
    assert not (staged / "build").exists()
    assert not (staged / "data").exists()
    assert not (staged / "dist").exists()
    assert not (staged / "src/histdatacom.egg-info").exists()
    assert not (staged / "venv").exists()


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
        fetch_report=_fetch_report(),
    )

    bundled = (
        source_root / "src/histdatacom/sidecar/assets/bin/macos-arm64/temporal"
    )
    provenance_path = (
        source_root
        / "src/histdatacom/sidecar/assets/temporal-cli-provenance.json"
    )
    manifest = json.loads(
        (
            source_root / "src/histdatacom/sidecar/assets/manifest.json"
        ).read_text(encoding="utf-8")
    )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert bundled.is_file()
    if os.name != "nt":
        assert bundled.stat().st_mode & 0o111
    assert provenance_path.is_file()
    assert manifest["embedded_binary"] is True
    assert manifest["platforms"]["macos-arm64"]["bundled"] is True
    assert manifest["platforms"]["macos-arm64"]["provenance"] == (
        "temporal-cli-provenance.json"
    )
    assert manifest["platforms"]["macos-arm64"]["license"] == (
        "third-party/temporal-cli/LICENSE"
    )
    assert manifest["platforms"]["macos-arm64"]["notice"] == (
        "third-party/temporal-cli/NOTICE.md"
    )
    assert "bin/macos-arm64/temporal" in manifest["resource_files"]
    assert "temporal-cli-provenance.json" in manifest["resource_files"]
    assert "third-party/temporal-cli/LICENSE" in manifest["resource_files"]
    assert "third-party/temporal-cli/NOTICE.md" in manifest["resource_files"]
    assert provenance["platform"] == "macos-arm64"
    assert provenance["version"] == "1.7.2"
    assert provenance["release_asset"]["sha256_verified"] is True
    assert (
        provenance["executable"]["resource_path"] == "bin/macos-arm64/temporal"
    )
    assert (
        provenance["executable"]["sha256"]
        == hashlib.sha256(executable.read_bytes()).hexdigest()
    )
    assert report["platform"] == "macos-arm64"
    assert report["executable"] == "bin/macos-arm64/temporal"
    assert report["provenance"] == "temporal-cli-provenance.json"
    assert report["archive_sha256"] == "1" * 64
