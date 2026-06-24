"""Tests for first-run Temporal runtime provisioning."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest

from histdatacom.sidecar import resources as resources_module
from histdatacom.sidecar.resources import (
    TEMPORAL_EXECUTABLE_ENV,
    TEMPORAL_OFFLINE_ENV,
    SidecarManifest,
    SidecarPlatformResource,
    TemporalRuntimeArtifact,
    TemporalRuntimeChecksumError,
    TemporalRuntimeIndex,
    TemporalRuntimeOfflineError,
    TemporalRuntimeProvisioningError,
    inspect_temporal_runtime_cache,
    prune_temporal_runtime_cache,
    resolve_temporal_runtime_executable,
    temporal_runtime_artifact,
    temporal_runtime_cache_entry_dir,
    temporal_runtime_executable_path,
)


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _tar_archive_bytes(
    *,
    executable_name: str = "temporal",
    executable_bytes: bytes = b"#!/bin/sh\necho temporal\n",
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo(f"temporal-cli/{executable_name}")
        info.size = len(executable_bytes)
        info.mode = 0o755
        archive.addfile(info, io.BytesIO(executable_bytes))
    return buffer.getvalue()


def _index(
    archive_bytes: bytes, *, sha256: str | None = None
) -> TemporalRuntimeIndex:
    digest = hashlib.sha256(archive_bytes).hexdigest()
    return TemporalRuntimeIndex.from_dict(
        {
            "schema_version": 1,
            "component": "temporal-cli",
            "version": "1.7.2",
            "release_base_url": "https://example.test/temporal",
            "platforms": {
                "linux-x86_64": {
                    "system": "linux",
                    "machine": "x86_64",
                    "archive_name": "temporal_cli_1.7.2_linux_amd64.tar.gz",
                    "archive_format": "tar.gz",
                    "archive_url": "https://example.test/temporal.tar.gz",
                    "archive_sha256": sha256 or digest,
                    "archive_size_bytes": len(archive_bytes),
                    "executable_name": "temporal",
                    "license": "MIT",
                    "upstream_repository": "https://github.com/temporalio/cli",
                }
            },
        }
    )


def _manifest_with_bundled_resource(
    *,
    provenance: str = "temporal-cli-provenance.json",
) -> SidecarManifest:
    return SidecarManifest(
        schema_version=1,
        runtime="temporal",
        distribution_strategy="test",
        embedded_binary=True,
        resource_files=(),
        runtime_artifact_index="temporal-runtime-index.json",
        platforms={
            "linux-x86_64": SidecarPlatformResource(
                key="linux-x86_64",
                bundled=True,
                executable="bin/linux-x86_64/temporal",
                wheel_tags=(),
                provenance=provenance,
            )
        },
        sdist_fallback="",
    )


def _packaged_provenance(
    *,
    artifact: TemporalRuntimeArtifact,
    index: TemporalRuntimeIndex,
    executable: Path,
    resource_path: str = "bin/linux-x86_64/temporal",
    archive_sha256: str | None = None,
) -> str:
    archive_digest = archive_sha256 or artifact.archive_sha256
    return json.dumps(
        {
            "schema_version": 1,
            "component": index.component,
            "bundled": True,
            "platform": artifact.platform_key,
            "version": index.version,
            "release_asset": {
                "name": artifact.archive_name,
                "url": artifact.archive_url,
                "sha256_expected": archive_digest,
                "sha256_actual": archive_digest,
                "sha256_verified": True,
            },
            "executable": {
                "resource_path": resource_path,
                "sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
                "size_bytes": executable.stat().st_size,
            },
        }
    )


def test_explicit_temporal_executable_override_wins(
    tmp_path: Path,
) -> None:
    """Explicit executables should not touch cache or network."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    executable = _executable(tmp_path / "temporal")

    with temporal_runtime_executable_path(
        explicit_executable=executable,
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=lambda *_args: pytest.fail("unexpected download"),
    ) as resolution:
        assert resolution.executable == executable
        assert resolution.source == "explicit"
        assert not resolution.network_fetch


def test_environment_temporal_executable_override_wins(
    tmp_path: Path,
) -> None:
    """The shared HISTDATACOM_TEMPORAL_EXECUTABLE override should be honored."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    executable = _executable(tmp_path / "temporal")

    resolution = resolve_temporal_runtime_executable(
        environ={TEMPORAL_EXECUTABLE_ENV: str(executable)},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=lambda *_args: pytest.fail("unexpected download"),
    )

    assert resolution.executable == executable
    assert resolution.source == "environment"


def test_packaged_bundle_requires_matching_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Offline/private bundled wheels should be checked against the pinned index."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    artifact = temporal_runtime_artifact("linux-x86_64", index)
    executable = _executable(tmp_path / "temporal")
    manifest = _manifest_with_bundled_resource()

    @contextmanager
    def fake_packaged_executable(
        *_args: object,
        **_kwargs: object,
    ) -> Iterator[Path]:
        yield executable

    monkeypatch.setattr(
        resources_module,
        "sidecar_executable_path",
        fake_packaged_executable,
    )
    monkeypatch.setattr(
        resources_module,
        "read_sidecar_asset_text",
        lambda _path: _packaged_provenance(
            artifact=artifact,
            index=index,
            executable=executable,
        ),
    )

    with temporal_runtime_executable_path(
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        manifest=manifest,
        index=index,
        download_archive=lambda *_args: pytest.fail("unexpected download"),
    ) as resolution:
        assert resolution.source == "packaged"
        assert resolution.executable == executable
        assert not resolution.network_fetch


def test_packaged_bundle_rejects_provenance_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Packaged binaries with stale release provenance should fail closed."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    artifact = temporal_runtime_artifact("linux-x86_64", index)
    executable = _executable(tmp_path / "temporal")
    manifest = _manifest_with_bundled_resource()

    @contextmanager
    def fake_packaged_executable(
        *_args: object,
        **_kwargs: object,
    ) -> Iterator[Path]:
        yield executable

    monkeypatch.setattr(
        resources_module,
        "sidecar_executable_path",
        fake_packaged_executable,
    )
    monkeypatch.setattr(
        resources_module,
        "read_sidecar_asset_text",
        lambda _path: _packaged_provenance(
            artifact=artifact,
            index=index,
            executable=executable,
            archive_sha256="0" * 64,
        ),
    )

    with pytest.raises(
        TemporalRuntimeProvisioningError,
        match="provenance does not match",
    ):
        with temporal_runtime_executable_path(
            environ={},
            cache_dir=tmp_path / "cache",
            platform_key="linux-x86_64",
            manifest=manifest,
            index=index,
            download_archive=lambda *_args: pytest.fail("unexpected download"),
        ):
            pass


def test_first_run_downloads_verifies_extracts_and_caches(
    tmp_path: Path,
) -> None:
    """A metadata-only install should provision the pinned runtime on first use."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    downloads: list[str] = []

    def download(
        artifact: TemporalRuntimeArtifact,
        destination: Path,
        timeout: float,
    ) -> None:
        downloads.append(artifact.platform_key)
        destination.write_bytes(archive_bytes)

    with temporal_runtime_executable_path(
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=download,
    ) as first:
        assert first.source == "download"
        assert first.network_fetch
        assert first.executable.is_file()
        assert first.provenance_path is not None
        assert first.provenance_path.is_file()

    with temporal_runtime_executable_path(
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=lambda *_args: pytest.fail("unexpected download"),
    ) as second:
        assert second.source == "cache"
        assert not second.network_fetch
        assert second.executable == first.executable

    assert downloads == ["linux-x86_64"]
    entries = inspect_temporal_runtime_cache(cache_dir=tmp_path / "cache")
    assert len(entries) == 1
    assert entries[0].valid
    assert entries[0].version == "1.7.2"


def test_checksum_mismatch_rejects_downloaded_archive(
    tmp_path: Path,
) -> None:
    """Downloaded archives must match the packaged SHA-256 before extraction."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes, sha256="0" * 64)

    def download(
        _artifact: TemporalRuntimeArtifact,
        destination: Path,
        _timeout: float,
    ) -> None:
        destination.write_bytes(archive_bytes)

    with pytest.raises(TemporalRuntimeChecksumError, match="checksum mismatch"):
        with temporal_runtime_executable_path(
            environ={},
            cache_dir=tmp_path / "cache",
            platform_key="linux-x86_64",
            index=index,
            download_archive=download,
        ):
            pass


def test_partial_download_rejects_archive_size_mismatch(
    tmp_path: Path,
) -> None:
    """Partial downloads should fail before extraction."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)

    def download(
        _artifact: TemporalRuntimeArtifact,
        destination: Path,
        _timeout: float,
    ) -> None:
        destination.write_bytes(archive_bytes[:-8])

    with pytest.raises(TemporalRuntimeChecksumError, match="size mismatch"):
        with temporal_runtime_executable_path(
            environ={},
            cache_dir=tmp_path / "cache",
            platform_key="linux-x86_64",
            index=index,
            download_archive=download,
        ):
            pass


def test_offline_mode_rejects_cache_miss_without_download(
    tmp_path: Path,
) -> None:
    """Offline mode should fail clearly instead of attempting a network fetch."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)

    with pytest.raises(
        TemporalRuntimeOfflineError, match="offline or disabled"
    ):
        with temporal_runtime_executable_path(
            environ={TEMPORAL_OFFLINE_ENV: "1"},
            cache_dir=tmp_path / "cache",
            platform_key="linux-x86_64",
            index=index,
            download_archive=lambda *_args: pytest.fail("unexpected download"),
        ):
            pass


def test_corrupt_cached_executable_is_not_reused(
    tmp_path: Path,
) -> None:
    """Cache hits should verify the extracted executable checksum."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)
    artifact = temporal_runtime_artifact("linux-x86_64", index)

    def download(
        _artifact: TemporalRuntimeArtifact,
        destination: Path,
        _timeout: float,
    ) -> None:
        destination.write_bytes(archive_bytes)

    with temporal_runtime_executable_path(
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=download,
    ) as resolution:
        resolution.executable.write_text("corrupt", encoding="utf-8")

    with pytest.raises(TemporalRuntimeOfflineError):
        with temporal_runtime_executable_path(
            environ={TEMPORAL_OFFLINE_ENV: "1"},
            cache_dir=tmp_path / "cache",
            platform_key="linux-x86_64",
            index=index,
        ):
            pass

    entry_dir = temporal_runtime_cache_entry_dir(
        artifact,
        version=index.version,
        cache_dir=tmp_path / "cache",
    )
    entries = inspect_temporal_runtime_cache(cache_dir=tmp_path / "cache")
    assert len(entries) == 1
    assert entries[0].path == entry_dir
    assert not entries[0].valid
    assert "checksum mismatch" in entries[0].reason


def test_prune_temporal_runtime_cache_keeps_current_entry(
    tmp_path: Path,
) -> None:
    """Cache pruning should expose a safe keep-current hook."""
    archive_bytes = _tar_archive_bytes()
    index = _index(archive_bytes)

    def download(
        _artifact: TemporalRuntimeArtifact,
        destination: Path,
        _timeout: float,
    ) -> None:
        destination.write_bytes(archive_bytes)

    with temporal_runtime_executable_path(
        environ={},
        cache_dir=tmp_path / "cache",
        platform_key="linux-x86_64",
        index=index,
        download_archive=download,
    ):
        pass
    stale = tmp_path / "cache" / "v0.0.0" / "linux-x86_64" / ("1" * 64)
    _executable(stale / "bin" / "temporal")
    (stale / "provenance.json").write_text(
        '{"version":"0.0.0","platform":"linux-x86_64",'
        '"archive_sha256":"111","executable_name":"temporal",'
        '"executable_sha256":"bad"}',
        encoding="utf-8",
    )

    result = prune_temporal_runtime_cache(
        cache_dir=tmp_path / "cache",
        keep_current=True,
        platform_key="linux-x86_64",
        index=index,
    )

    assert len(result["kept"]) == 1
    assert len(result["deleted"]) == 1
    assert not stale.exists()
