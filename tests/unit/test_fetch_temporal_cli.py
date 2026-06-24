"""Tests for pinned Temporal CLI release artifact fetching."""

from __future__ import annotations

import importlib.util
import io
import os
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest

from histdatacom.sidecar.resources import load_temporal_runtime_index


def _load_script() -> ModuleType:
    """Load the Temporal CLI fetch helper as a test module."""
    script_path = (
        Path(__file__).resolve().parents[2] / "scripts/fetch_temporal_cli.py"
    )
    spec = importlib.util.spec_from_file_location(
        "fetch_temporal_cli",
        script_path,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["fetch_temporal_cli"] = module
    spec.loader.exec_module(module)
    return module


def test_temporal_cli_asset_table_covers_declared_platforms() -> None:
    """Pinned release metadata should cover every sidecar wheel target."""
    module = _load_script()

    assert set(module.TEMPORAL_CLI_ASSETS) == {
        "linux-arm64",
        "linux-x86_64",
        "macos-arm64",
        "macos-x86_64",
        "windows-x86_64",
    }
    assert module.TEMPORAL_CLI_REPOSITORY == "https://github.com/temporalio/cli"
    assert module.TEMPORAL_CLI_LICENSE == "MIT"
    assert module.TEMPORAL_CLI_LICENSE_URL.endswith("/LICENSE")
    for platform_key, asset in module.TEMPORAL_CLI_ASSETS.items():
        assert asset.platform_key == platform_key
        assert len(asset.sha256) == 64
        assert asset.url(module.DEFAULT_TEMPORAL_CLI_VERSION).startswith(
            "https://github.com/temporalio/cli/releases/download/v"
        )


def test_temporal_cli_asset_rejects_unpinned_version() -> None:
    """Checksum verification should fail closed for unpinned versions."""
    module = _load_script()

    with pytest.raises(SystemExit, match="checksum table is pinned"):
        module.temporal_cli_asset("linux-x86_64", version="9.9.9")


def test_temporal_cli_asset_table_matches_runtime_index() -> None:
    """The fetch helper and packaged resolver index should not drift apart."""
    module = _load_script()
    index = load_temporal_runtime_index()

    assert index.version == module.DEFAULT_TEMPORAL_CLI_VERSION
    assert set(index.platforms) == set(module.TEMPORAL_CLI_ASSETS)
    for platform_key, artifact in index.platforms.items():
        asset = module.TEMPORAL_CLI_ASSETS[platform_key]
        assert artifact.archive_name == asset.asset_name(index.version)
        assert artifact.archive_url == asset.url(index.version)
        assert artifact.archive_sha256 == asset.sha256
        assert artifact.executable_name == asset.executable_name
        expected_format = (
            "zip" if artifact.archive_name.endswith(".zip") else "tar.gz"
        )
        assert artifact.archive_format == expected_format
        assert artifact.archive_size_bytes > 0


def test_extract_executable_from_tar_archive(tmp_path: Path) -> None:
    """Tar release archives should extract exactly the Temporal executable."""
    module = _load_script()
    archive_path = tmp_path / "temporal.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        payload = b"#!/bin/sh\n"
        info = tarfile.TarInfo("temporal_cli/temporal")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))

    executable = module.extract_executable(
        archive_path,
        executable_name="temporal",
        destination_dir=tmp_path / "bin",
    )

    assert executable.read_bytes() == b"#!/bin/sh\n"
    if os.name != "nt":
        assert executable.stat().st_mode & 0o111


def test_extract_executable_from_zip_archive(tmp_path: Path) -> None:
    """Zip release archives should extract the Windows executable."""
    module = _load_script()
    archive_path = tmp_path / "temporal.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("temporal_cli/temporal.exe", b"MZ")

    executable = module.extract_executable(
        archive_path,
        executable_name="temporal.exe",
        destination_dir=tmp_path / "bin",
    )

    assert executable.read_bytes() == b"MZ"
