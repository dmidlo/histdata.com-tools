"""Tests for Temporal orchestration package-data resources."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

import histdatacom.orchestration.resources as resource_module
from histdatacom.orchestration.resources import (
    OrchestrationManifest,
    OrchestrationExecutableUnavailable,
    UnsupportedOrchestrationPlatform,
    current_platform_key,
    load_orchestration_manifest,
    read_orchestration_asset_text,
    orchestration_asset,
    orchestration_executable_path,
    orchestration_platform_resource,
)


def test_orchestration_manifest_loads_packaged_strategy() -> None:
    """The packaged manifest should define the PyPI orchestration strategy."""
    manifest = load_orchestration_manifest()

    assert manifest.schema_version == 1
    assert manifest.runtime == "temporal"
    assert manifest.distribution_strategy == (
        "metadata-wheel-with-verified-runtime-provisioning"
    )
    assert not manifest.embedded_binary
    assert all(not resource.bundled for resource in manifest.platforms.values())
    assert all(
        not resource_file.startswith("bin/")
        for resource_file in manifest.resource_files
    )
    assert "runtime resolver provisions" in manifest.sdist_fallback
    assert manifest.runtime_artifact_index == "temporal-runtime-index.json"
    assert "runtime-defaults.json" in manifest.resource_files
    assert "temporal-runtime-index.json" in manifest.resource_files
    assert "third-party/temporal-cli/LICENSE" in manifest.resource_files
    assert "third-party/temporal-cli/NOTICE.md" in manifest.resource_files
    assert "linux-x86_64" in manifest.platforms


def test_orchestration_assets_are_readable_from_importlib_resources() -> None:
    """Editable and wheel installs should expose orchestration package data."""
    manifest_text = read_orchestration_asset_text("manifest.json")
    defaults_text = read_orchestration_asset_text("runtime-defaults.json")
    runtime_index_text = read_orchestration_asset_text(
        "temporal-runtime-index.json"
    )
    license_text = read_orchestration_asset_text(
        "third-party/temporal-cli/LICENSE"
    )
    notice_text = read_orchestration_asset_text(
        "third-party/temporal-cli/NOTICE.md"
    )

    assert orchestration_asset("README.md").is_file()
    assert json.loads(manifest_text)["runtime"] == "temporal"
    assert json.loads(defaults_text)["persistence"]["driver"] == "sqlite"
    runtime_index = json.loads(runtime_index_text)
    assert runtime_index["component"] == "temporal-cli"
    assert runtime_index["version"] == "1.7.2"
    assert runtime_index["platforms"]["macos-arm64"]["archive_size_bytes"] > 0
    assert (
        runtime_index["platforms"]["macos-arm64"]["archive_sha256"]
        == "561ac68bdb6c16c8e8cbbd49f12578218ff1776007c3f3ae0d0196c8c9a73e79"
    )
    assert "MIT License" in license_text
    assert "Temporal CLI" in notice_text


def test_platform_key_normalizes_common_targets() -> None:
    """Common Python platform names should map to manifest keys."""
    assert current_platform_key("Darwin", "arm64") == "macos-arm64"
    assert current_platform_key("Darwin", "x86_64") == "macos-x86_64"
    assert current_platform_key("Linux", "aarch64") == "linux-arm64"
    assert current_platform_key("Windows", "AMD64") == "windows-x86_64"


def test_declared_platform_without_binary_fails_clearly() -> None:
    """Metadata-only wheels should not silently use unmanaged binaries."""
    resource = orchestration_platform_resource("linux-x86_64")

    assert not resource.bundled
    with pytest.raises(OrchestrationExecutableUnavailable) as err:
        with orchestration_executable_path("linux-x86_64"):
            pass

    message = str(err.value)
    assert "not bundled in this distribution" in message
    assert "metadata-wheel-with-verified-runtime-provisioning" in message
    assert "bin/linux-x86_64/temporal" in message


def test_bundled_platform_executable_resolves_from_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Bundled platform wheels should expose an executable path."""
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    manifest = OrchestrationManifest.from_dict(
        {
            "schema_version": 1,
            "runtime": "temporal",
            "distribution_strategy": (
                "metadata-wheel-with-verified-runtime-provisioning"
            ),
            "embedded_binary": True,
            "resource_files": [
                "README.md",
                "manifest.json",
                "runtime-defaults.json",
                "bin/macos-arm64/temporal",
            ],
            "sdist_fallback": "metadata-only",
            "platforms": {
                "macos-arm64": {
                    "bundled": True,
                    "executable": "bin/macos-arm64/temporal",
                    "wheel_tags": ["macosx_11_0_arm64"],
                    "notes": "test bundled executable",
                }
            },
        }
    )

    @contextmanager
    def fake_as_file(asset):
        yield asset

    monkeypatch.setattr(
        resource_module,
        "orchestration_asset",
        lambda relative_path: executable,
    )
    monkeypatch.setattr(resource_module.resources, "as_file", fake_as_file)

    with orchestration_executable_path("macos-arm64", manifest) as resolved:
        assert resolved == executable


def test_unsupported_platform_fails_with_supported_keys() -> None:
    """Unknown platforms should fail with a useful support matrix hint."""
    with pytest.raises(UnsupportedOrchestrationPlatform) as err:
        orchestration_platform_resource("solaris-sparc")

    message = str(err.value)
    assert "solaris-sparc" in message
    assert "linux-x86_64" in message
    assert "windows-x86_64" in message
