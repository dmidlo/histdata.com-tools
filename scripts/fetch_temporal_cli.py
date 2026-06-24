"""Fetch and verify pinned Temporal CLI release artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tarfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Sequence

DEFAULT_TEMPORAL_CLI_VERSION = "1.7.2"
TEMPORAL_CLI_RELEASE_BASE = (
    "https://github.com/temporalio/cli/releases/download"
)
TEMPORAL_CLI_REPOSITORY = "https://github.com/temporalio/cli"
TEMPORAL_CLI_LICENSE = "MIT"
TEMPORAL_CLI_LICENSE_URL = "https://github.com/temporalio/cli/blob/main/LICENSE"


@dataclass(frozen=True, slots=True)
class TemporalCliAsset:
    """Pinned Temporal CLI release artifact metadata."""

    platform_key: str
    asset_template: str
    sha256: str
    executable_name: str

    def asset_name(self, version: str) -> str:
        """Return the release asset name for a Temporal CLI version."""
        return self.asset_template.format(version=version)

    def url(self, version: str) -> str:
        """Return the release asset URL for a Temporal CLI version."""
        return (
            f"{TEMPORAL_CLI_RELEASE_BASE}/v{version}/"
            f"{self.asset_name(version)}"
        )


TEMPORAL_CLI_ASSETS: dict[str, TemporalCliAsset] = {
    "linux-x86_64": TemporalCliAsset(
        platform_key="linux-x86_64",
        asset_template="temporal_cli_{version}_linux_amd64.tar.gz",
        sha256="e2f548af84e820b7d71f25fc6461a4e6a1ab7cdb6a80fde3c375641b0772375b",
        executable_name="temporal",
    ),
    "linux-arm64": TemporalCliAsset(
        platform_key="linux-arm64",
        asset_template="temporal_cli_{version}_linux_arm64.tar.gz",
        sha256="0d60307cf036f5e29cf0d298dabc9dd62f1cce80f72cbfdbdb0e54ad7790a701",
        executable_name="temporal",
    ),
    "macos-x86_64": TemporalCliAsset(
        platform_key="macos-x86_64",
        asset_template="temporal_cli_{version}_darwin_amd64.tar.gz",
        sha256="bc0929b79aa1792d77fe8412ee9dcb05ffcc33c5d6ed3bb5f352d3bc4b8a37a4",
        executable_name="temporal",
    ),
    "macos-arm64": TemporalCliAsset(
        platform_key="macos-arm64",
        asset_template="temporal_cli_{version}_darwin_arm64.tar.gz",
        sha256="561ac68bdb6c16c8e8cbbd49f12578218ff1776007c3f3ae0d0196c8c9a73e79",
        executable_name="temporal",
    ),
    "windows-x86_64": TemporalCliAsset(
        platform_key="windows-x86_64",
        asset_template="temporal_cli_{version}_windows_amd64.zip",
        sha256="5289e7b404ad01cf51c6754bc0f0684daed89186cac5f0f3f15329b52342cc34",
        executable_name="temporal.exe",
    ),
}


def temporal_cli_asset(
    platform_key: str,
    *,
    version: str = DEFAULT_TEMPORAL_CLI_VERSION,
) -> TemporalCliAsset:
    """Return pinned artifact metadata for a supported platform."""
    if version != DEFAULT_TEMPORAL_CLI_VERSION:
        raise SystemExit(
            "Temporal CLI checksum table is pinned to "
            f"{DEFAULT_TEMPORAL_CLI_VERSION}; update the table before using "
            f"{version}."
        )
    try:
        return TEMPORAL_CLI_ASSETS[platform_key]
    except KeyError as err:
        supported = ", ".join(sorted(TEMPORAL_CLI_ASSETS))
        raise SystemExit(
            f"unsupported Temporal CLI platform {platform_key!r}. "
            f"Supported platforms: {supported}"
        ) from err


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(path: Path, expected_sha256: str) -> str:
    """Verify a file digest and return the actual digest."""
    actual_sha256 = sha256_file(path)
    if actual_sha256.lower() != expected_sha256.lower():
        raise SystemExit(
            f"checksum mismatch for {path}: expected {expected_sha256}, "
            f"got {actual_sha256}"
        )
    return actual_sha256


def download_asset(
    asset: TemporalCliAsset,
    *,
    version: str,
    download_dir: Path,
) -> Path:
    """Download a pinned Temporal CLI release artifact."""
    download_dir.mkdir(parents=True, exist_ok=True)
    destination = download_dir / asset.asset_name(version)
    with urllib.request.urlopen(asset.url(version), timeout=60) as response:
        with destination.open("wb") as output:
            shutil.copyfileobj(response, output)
    return destination


def extract_executable(
    archive_path: Path,
    *,
    executable_name: str,
    destination_dir: Path,
) -> Path:
    """Extract a named executable from a tar.gz or zip release artifact."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / executable_name
    if archive_path.name.endswith(".zip"):
        _extract_zip_executable(
            archive_path,
            executable_name=executable_name,
            destination=destination,
        )
    elif archive_path.name.endswith(".tar.gz"):
        _extract_tar_executable(
            archive_path,
            executable_name=executable_name,
            destination=destination,
        )
    else:
        raise SystemExit(f"unsupported Temporal CLI archive: {archive_path}")
    if not executable_name.endswith(".exe"):
        destination.chmod(destination.stat().st_mode | 0o755)
    return destination


def fetch_temporal_cli(
    *,
    platform_key: str,
    version: str,
    download_dir: Path,
    output_dir: Path,
) -> dict[str, str]:
    """Download, verify, and extract a pinned Temporal CLI executable."""
    asset = temporal_cli_asset(platform_key, version=version)
    archive_path = download_asset(
        asset,
        version=version,
        download_dir=download_dir,
    )
    actual_sha256 = verify_sha256(archive_path, asset.sha256)
    executable_path = extract_executable(
        archive_path,
        executable_name=asset.executable_name,
        destination_dir=output_dir,
    )
    return {
        "platform": platform_key,
        "version": version,
        "asset": asset.asset_name(version),
        "url": asset.url(version),
        "upstream_repository": TEMPORAL_CLI_REPOSITORY,
        "license": TEMPORAL_CLI_LICENSE,
        "license_url": TEMPORAL_CLI_LICENSE_URL,
        "archive": str(archive_path),
        "executable": str(executable_path),
        "sha256": actual_sha256,
        "expected_sha256": asset.sha256,
    }


def _extract_zip_executable(
    archive_path: Path,
    *,
    executable_name: str,
    destination: Path,
) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        matches = [
            name
            for name in archive.namelist()
            if PurePosixPath(name).name == executable_name
            and not name.endswith("/")
        ]
        if len(matches) != 1:
            raise SystemExit(
                f"expected exactly one {executable_name} in {archive_path}, "
                f"found {matches}"
            )
        with archive.open(matches[0], "r") as source:
            _copy_executable(source, destination)


def _extract_tar_executable(
    archive_path: Path,
    *,
    executable_name: str,
    destination: Path,
) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        matches = [
            member
            for member in archive.getmembers()
            if member.isfile()
            and PurePosixPath(member.name).name == executable_name
        ]
        if len(matches) != 1:
            names = [member.name for member in matches]
            raise SystemExit(
                f"expected exactly one {executable_name} in {archive_path}, "
                f"found {names}"
            )
        source = archive.extractfile(matches[0])
        if source is None:
            raise SystemExit(
                f"could not extract {matches[0].name} from {archive_path}"
            )
        with source:
            _copy_executable(source, destination)


def _copy_executable(source: BinaryIO, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as output:
        shutil.copyfileobj(source, output)


def _write_github_outputs(report: dict[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as handle:
        for key in (
            "platform",
            "version",
            "asset",
            "archive",
            "executable",
            "sha256",
        ):
            handle.write(f"{key}={report[key]}\n")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for release workflow artifact fetching."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--platform-key",
        required=True,
        choices=sorted(TEMPORAL_CLI_ASSETS),
        help="runtime manifest platform key to fetch",
    )
    parser.add_argument(
        "--version",
        default=DEFAULT_TEMPORAL_CLI_VERSION,
        help="pinned Temporal CLI version",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path(".temporal-cli/downloads"),
        help="directory for downloaded release archives",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(".temporal-cli/bin"),
        help="directory for the extracted executable",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="write a JSON fetch/verification report",
    )
    parser.add_argument(
        "--github-output",
        action="store_true",
        help="write executable and checksum fields to GITHUB_OUTPUT",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = fetch_temporal_cli(
        platform_key=args.platform_key,
        version=args.version,
        download_dir=args.download_dir,
        output_dir=args.output_dir,
    )
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if args.github_output:
        _write_github_outputs(report)
    print(json.dumps(report, indent=2, sort_keys=True))  # noqa:T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
