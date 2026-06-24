"""Tests for wheel runtime resource inspection."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType
from zipfile import ZipFile, ZipInfo

import pytest

EXECUTABLE_BYTES = b"#!/bin/sh\n"
ARCHIVE_SHA256 = "2" * 64


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


def _base_manifest() -> dict[str, object]:
    """Return the repository runtime manifest fixture."""
    manifest_path = (
        Path(__file__).resolve().parents[2]
        / "src/histdatacom/orchestration/assets/manifest.json"
    )
    loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _append_resource(manifest: dict[str, object], resource: str) -> None:
    """Append a manifest resource once."""
    resource_files = manifest["resource_files"]
    assert isinstance(resource_files, list)
    if resource not in resource_files:
        resource_files.append(resource)


def _provenance() -> dict[str, object]:
    """Return bundled Temporal CLI provenance for the test executable."""
    return {
        "schema_version": 1,
        "component": "temporal-cli",
        "bundled": True,
        "platform": "macos-arm64",
        "version": "1.7.2",
        "upstream": {
            "repository": "https://github.com/temporalio/cli",
            "license": "MIT",
            "license_url": "https://github.com/temporalio/cli/blob/main/LICENSE",
            "license_file": "third-party/temporal-cli/LICENSE",
            "notice_file": "third-party/temporal-cli/NOTICE.md",
        },
        "release_asset": {
            "name": "temporal_cli_1.7.2_darwin_arm64.tar.gz",
            "url": (
                "https://github.com/temporalio/cli/releases/download/v1.7.2/"
                "temporal_cli_1.7.2_darwin_arm64.tar.gz"
            ),
            "sha256_expected": ARCHIVE_SHA256,
            "sha256_actual": ARCHIVE_SHA256,
            "sha256_verified": True,
        },
        "executable": {
            "resource_path": "bin/macos-arm64/temporal",
            "sha256": hashlib.sha256(EXECUTABLE_BYTES).hexdigest(),
            "size_bytes": len(EXECUTABLE_BYTES),
            "version_probe": "temporal version 1.7.2",
        },
        "builder": "scripts/runtime_platform_wheel.py",
    }


def _write_common_assets(
    wheel: ZipFile,
    *,
    manifest: dict[str, object],
) -> None:
    """Write common runtime assets to a fake wheel."""
    wheel.writestr(
        "histdatacom/orchestration/assets/README.md",
        "runtime assets",
    )
    wheel.writestr(
        "histdatacom/orchestration/assets/runtime-defaults.json",
        "{}",
    )
    wheel.writestr(
        "histdatacom/orchestration/assets/temporal-runtime-index.json",
        json.dumps(
            {
                "schema_version": 1,
                "component": "temporal-cli",
                "version": "1.7.2",
                "platforms": {
                    "macos-arm64": {
                        "archive_name": (
                            "temporal_cli_1.7.2_darwin_arm64.tar.gz"
                        ),
                        "archive_sha256": ARCHIVE_SHA256,
                        "archive_size_bytes": 123,
                        "archive_url": (
                            "https://github.com/temporalio/cli/releases/"
                            "download/v1.7.2/"
                            "temporal_cli_1.7.2_darwin_arm64.tar.gz"
                        ),
                        "executable_name": "temporal",
                    }
                },
            }
        ),
    )
    wheel.writestr(
        "histdatacom/orchestration/assets/third-party/temporal-cli/LICENSE",
        "MIT\n",
    )
    wheel.writestr(
        "histdatacom/orchestration/assets/third-party/temporal-cli/NOTICE.md",
        "Temporal CLI notice\n",
    )
    wheel.writestr(
        "histdatacom/orchestration/assets/manifest.json",
        json.dumps(manifest),
    )


def _write_dist_info(wheel: ZipFile, *, tag: str) -> None:
    """Write minimal package metadata needed by the wheel inspector."""
    wheel.writestr(
        "histdatacom-1.0.0.dist-info/METADATA",
        "\n".join(
            [
                "Metadata-Version: 2.4",
                "Name: histdatacom",
                "Version: 1.0.0",
                "Requires-Python: >=3.10.0",
                "Classifier: Operating System :: MacOS",
                "Classifier: Operating System :: Microsoft :: Windows",
                "Classifier: Operating System :: POSIX",
                "Classifier: Operating System :: POSIX :: Linux",
                "Provides-Extra: temporal",
                "Provides-Extra: all",
                "Requires-Dist: temporalio>=1.10,<2",
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
                f"Tag: {tag}",
                "",
            ]
        ),
    )
    wheel.writestr("histdatacom-1.0.0.dist-info/RECORD", "")


def test_inspect_wheel_accepts_bundled_platform_executable(
    tmp_path: Path,
) -> None:
    """Bundled platform wheels should pass provenance, resource, and tag checks."""
    module = _load_script()
    manifest = _base_manifest()
    manifest["embedded_binary"] = True
    _append_resource(manifest, "bin/macos-arm64/temporal")
    _append_resource(manifest, "temporal-cli-provenance.json")
    platforms = manifest["platforms"]
    assert isinstance(platforms, dict)
    platform_resource = platforms["macos-arm64"]
    assert isinstance(platform_resource, dict)
    platform_resource["bundled"] = True
    platform_resource["provenance"] = "temporal-cli-provenance.json"
    platform_resource["license"] = "third-party/temporal-cli/LICENSE"
    platform_resource["notice"] = "third-party/temporal-cli/NOTICE.md"
    platform_resource["notes"] = "test bundled executable"
    wheel_path = tmp_path / "histdatacom-1.0.0-py3-none-macosx_11_0_arm64.whl"

    with ZipFile(wheel_path, "w") as wheel:
        _write_common_assets(wheel, manifest=manifest)
        wheel.writestr(
            "histdatacom/orchestration/assets/temporal-cli-provenance.json",
            json.dumps(_provenance()),
        )
        wheel.writestr(
            _wheel_info(
                "histdatacom/orchestration/assets/bin/macos-arm64/temporal",
                mode=0o755,
            ),
            EXECUTABLE_BYTES,
        )
        _write_dist_info(wheel, tag="py3-none-macosx_11_0_arm64")

    report = module.inspect_wheel(
        wheel_path,
        require_bundled_platforms={"macos-arm64"},
    )

    assert report["runtime"]["embedded_binary"] is True
    assert report["runtime"]["bundled_platforms"] == ["macos-arm64"]
    assert report["runtime"]["provenance"]["macos-arm64"]["version"] == "1.7.2"
    assert set(module.EXPECTED_METADATA_CLASSIFIERS) <= set(
        report["classifiers"]
    )
    assert report["wheel_tags"] == ["py3-none-macosx_11_0_arm64"]


def test_inspect_wheel_accepts_metadata_only_fallback(
    tmp_path: Path,
) -> None:
    """Metadata-only fallback wheels should omit executable provenance."""
    module = _load_script()
    manifest = _base_manifest()
    wheel_path = tmp_path / "histdatacom-1.0.0-py3-none-any.whl"

    with ZipFile(wheel_path, "w") as wheel:
        _write_common_assets(wheel, manifest=manifest)
        _write_dist_info(wheel, tag="py3-none-any")

    report = module.inspect_wheel(wheel_path)

    assert report["runtime"]["embedded_binary"] is False
    assert report["runtime"]["bundled_platforms"] == []
    assert report["runtime"]["provenance"] == {}
    assert (
        "temporal-cli-provenance.json"
        not in report["runtime"]["resource_files"]
    )
    assert report["wheel_tags"] == ["py3-none-any"]


def test_inspect_wheel_rejects_bundled_platform_without_provenance(
    tmp_path: Path,
) -> None:
    """Bundled platform wheels must not ship opaque third-party executables."""
    module = _load_script()
    manifest = _base_manifest()
    manifest["embedded_binary"] = True
    _append_resource(manifest, "bin/macos-arm64/temporal")
    platforms = manifest["platforms"]
    assert isinstance(platforms, dict)
    platform_resource = platforms["macos-arm64"]
    assert isinstance(platform_resource, dict)
    platform_resource["bundled"] = True
    platform_resource["license"] = "third-party/temporal-cli/LICENSE"
    platform_resource["notice"] = "third-party/temporal-cli/NOTICE.md"
    wheel_path = tmp_path / "histdatacom-1.0.0-py3-none-macosx_11_0_arm64.whl"

    with ZipFile(wheel_path, "w") as wheel:
        _write_common_assets(wheel, manifest=manifest)
        wheel.writestr(
            _wheel_info(
                "histdatacom/orchestration/assets/bin/macos-arm64/temporal",
                mode=0o755,
            ),
            EXECUTABLE_BYTES,
        )
        _write_dist_info(wheel, tag="py3-none-macosx_11_0_arm64")

    with pytest.raises(SystemExit, match="bundled without provenance"):
        module.inspect_wheel(
            wheel_path,
            require_bundled_platforms={"macos-arm64"},
        )
