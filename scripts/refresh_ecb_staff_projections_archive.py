#!/usr/bin/env python3
"""Refresh or replay the complete official ECB staff-projection archive."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_ecb_foedb_chunk_requests,
    build_ecb_foedb_metadata_request,
    build_ecb_foedb_projection_release_index,
    build_ecb_foedb_versions_request,
    build_ecb_projection_archive_request,
    build_ecb_projection_requests,
    build_ecb_staff_projections_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_staff_projections_manifest_path,
    parse_ecb_foedb_version,
    parse_ecb_projection_archive_index,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 ECB staff projections qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        required=True,
        help="Explicit archive boundary; use the last fully observable day.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="Caller-retained exact FOEDB, index, and projection corpus.",
    )
    parser.add_argument(
        "--asset-directory", type=Path, default=DEFAULT_ASSET_DIRECTORY
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="Fetch official artifacts absent from the retained corpus.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        choices=range(1, 9),
        metavar="1..8",
    )
    return parser.parse_args()


def _fetch(uri: str) -> tuple[bytes, str]:
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            response = requests.get(
                uri,
                headers={"User-Agent": USER_AGENT},
                timeout=(15, 180),
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                time.sleep(min(30, attempt * 3))
                continue
            response.raise_for_status()
            return cast(bytes, response.content), response.url
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 10:
                time.sleep(min(30, attempt * 2))
    raise RuntimeError(f"unable to fetch {uri}") from last_error


def _local_path(
    source_directory: Path, request: OfficialSourceRequestV1
) -> Path:
    name = Path(urlparse(request.uri).path).name
    if name in {"versions.json", "metadata.json"}:
        return source_directory / "foedb" / name
    if name.startswith("chunk_") and name.endswith(".json"):
        return source_directory / "foedb" / "data" / name
    return source_directory / "projections" / name


def _read_or_fetch(
    local_file: Path, uri: str, *, fetch_missing: bool
) -> tuple[bytes, str]:
    if local_file.exists():
        if not local_file.is_file():
            raise ValueError(f"retained ECB path is not a file: {local_file}")
        return local_file.read_bytes(), uri
    if not fetch_missing:
        raise FileNotFoundError(local_file)
    content, resolved_uri = _fetch(uri)
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(content)
    return content, resolved_uri


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    resolved_uri: str,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.JSON: "application/json",
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
    }[request.source_format]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=captured_at_ns,
        completed_at_ns=captured_at_ns,
        status_code=200,
        resolved_uri=resolved_uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def main() -> int:
    args = _arguments()
    source_directory = args.source_directory.expanduser().resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    registry = load_packaged_official_source_registry()
    captured_at_ns = time.time_ns()

    def capture(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
        content, resolved_uri = _read_or_fetch(
            _local_path(source_directory, request),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        return _snapshot(request, content, resolved_uri, captured_at_ns)

    versions_snapshot = capture(build_ecb_foedb_versions_request(registry))
    version, version_hash = parse_ecb_foedb_version(versions_snapshot)
    metadata_snapshot = capture(
        build_ecb_foedb_metadata_request(registry, version, version_hash)
    )
    metadata = json.loads(metadata_snapshot.content)
    if not isinstance(metadata, dict):
        raise TypeError("ECB FOEDB metadata is not an object")
    chunk_requests = build_ecb_foedb_chunk_requests(
        registry,
        version,
        version_hash,
        total_records=int(metadata["total_records"]),
        chunk_size=int(metadata["chunk_size"]),
        chunk_group_size=int(metadata["chunk_group_size"]),
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        chunk_snapshots = tuple(pool.map(capture, chunk_requests))
    archive_snapshot = capture(build_ecb_projection_archive_request(registry))
    archive_index = parse_ecb_projection_archive_index(
        archive_snapshot, as_of_date=args.as_of
    )
    release_index = build_ecb_foedb_projection_release_index(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        archive_index,
        as_of_date=args.as_of,
    )
    report_requests = build_ecb_projection_requests(registry, release_index)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        report_snapshots = tuple(pool.map(capture, report_requests))
    manifest = build_ecb_staff_projections_archive_manifest(
        registry,
        archive_index,
        release_index,
        {item.request.uri: item for item in report_snapshots},
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = asset_directory / (
        packaged_ecb_staff_projections_manifest_path().name
    )
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": manifest.release_index.as_of_date,
                "database_version": manifest.release_index.database_version,
                "database_record_count": (
                    manifest.release_index.database_total_records
                ),
                "projection_count": manifest.projection_count,
                "ecb_staff_count": manifest.ecb_staff_count,
                "eurosystem_staff_count": manifest.eurosystem_staff_count,
                "pdf_artifact_count": manifest.pdf_artifact_count,
                "html_artifact_count": manifest.html_artifact_count,
                "exact_minute_count": manifest.exact_minute_count,
                "date_only_count": manifest.date_only_count,
                "raw_artifact_count": manifest.raw_artifact_count,
                "unique_content_sha256_count": (
                    manifest.unique_content_sha256_count
                ),
                "total_content_bytes": manifest.total_content_bytes,
                "source_directory": str(source_directory),
                "manifest_path": str(manifest_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
