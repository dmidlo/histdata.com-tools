"""Tests for Temporal sidecar package-data resources."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

import histdatacom.sidecar.resources as resource_module
from histdatacom.sidecar.resources import (
    SidecarManifest,
    SidecarExecutableUnavailable,
    UnsupportedSidecarPlatform,
    current_platform_key,
    load_sidecar_manifest,
    read_sidecar_asset_text,
    sidecar_asset,
    sidecar_executable_path,
    sidecar_platform_resource,
)


def test_sidecar_manifest_loads_packaged_strategy() -> None:
    """The packaged manifest should define the PyPI sidecar strategy."""
    manifest = load_sidecar_manifest()

    assert manifest.schema_version == 1
    assert manifest.sidecar == "temporal"
    assert manifest.distribution_strategy == (
        "platform-wheel-with-sdist-metadata-fallback"
    )
    assert not manifest.embedded_binary
    assert "runtime-defaults.json" in manifest.resource_files
    assert "third-party/temporal-cli/LICENSE" in manifest.resource_files
    assert "third-party/temporal-cli/NOTICE.md" in manifest.resource_files
    assert "linux-x86_64" in manifest.platforms


def test_sidecar_assets_are_readable_from_importlib_resources() -> None:
    """Editable and wheel installs should expose sidecar package data."""
    manifest_text = read_sidecar_asset_text("manifest.json")
    defaults_text = read_sidecar_asset_text("runtime-defaults.json")
    license_text = read_sidecar_asset_text("third-party/temporal-cli/LICENSE")
    notice_text = read_sidecar_asset_text("third-party/temporal-cli/NOTICE.md")

    assert sidecar_asset("README.md").is_file()
    assert json.loads(manifest_text)["sidecar"] == "temporal"
    assert json.loads(defaults_text)["persistence"]["driver"] == "sqlite"
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
    resource = sidecar_platform_resource("linux-x86_64")

    assert not resource.bundled
    with pytest.raises(SidecarExecutableUnavailable) as err:
        with sidecar_executable_path("linux-x86_64"):
            pass

    message = str(err.value)
    assert "not bundled in this distribution" in message
    assert "platform-wheel-with-sdist-metadata-fallback" in message
    assert "bin/linux-x86_64/temporal" in message


def test_bundled_platform_executable_resolves_from_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Bundled platform wheels should expose an executable path."""
    executable = tmp_path / "temporal"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    manifest = SidecarManifest.from_dict(
        {
            "schema_version": 1,
            "sidecar": "temporal",
            "distribution_strategy": (
                "platform-wheel-with-sdist-metadata-fallback"
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
        "sidecar_asset",
        lambda relative_path: executable,
    )
    monkeypatch.setattr(resource_module.resources, "as_file", fake_as_file)

    with sidecar_executable_path("macos-arm64", manifest) as resolved:
        assert resolved == executable


def test_unsupported_platform_fails_with_supported_keys() -> None:
    """Unknown platforms should fail with a useful support matrix hint."""
    with pytest.raises(UnsupportedSidecarPlatform) as err:
        sidecar_platform_resource("solaris-sparc")

    message = str(err.value)
    assert "solaris-sparc" in message
    assert "linux-x86_64" in message
    assert "windows-x86_64" in message
