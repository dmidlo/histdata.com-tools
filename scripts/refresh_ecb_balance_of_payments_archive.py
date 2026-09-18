#!/usr/bin/env python3
"""Refresh or replay the official ECB monthly balance-of-payments archive."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlsplit

import requests

from histdatacom.market_context.ecb_balance_of_payments_archive import (
    build_ecb_balance_of_payments_archive_manifest,
    build_ecb_balance_of_payments_foedb_chunk_requests,
    build_ecb_balance_of_payments_foedb_metadata_request,
    build_ecb_balance_of_payments_foedb_versions_request,
    build_ecb_balance_of_payments_index_requests,
    build_ecb_balance_of_payments_release_requests,
    discover_ecb_balance_of_payments_support_requests,
    packaged_ecb_balance_of_payments_manifest_path,
    parse_ecb_balance_of_payments_foedb_version,
    parse_ecb_balance_of_payments_inventory,
    replay_ecb_balance_of_payments_archive,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 ECB balance-of-payments qualification "
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
        help="Caller-retained exact ECB inventory, FOEDB, and release corpus.",
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
        default=6,
        choices=range(1, 9),
        metavar="1..8",
    )
    return parser.parse_args()


def _fetch(uri: str) -> tuple[bytes, str, str | None]:
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
            if urlsplit(response.url).hostname not in {
                "www.ecb.europa.eu",
                "ecb.europa.eu",
            }:
                raise RuntimeError("ECB fetch redirected to another host")
            return (
                cast(bytes, response.content),
                response.url,
                response.headers.get("Content-Type"),
            )
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 10:
                time.sleep(min(30, attempt * 2))
    raise RuntimeError(f"unable to fetch {uri}") from last_error


def _local_path(
    source_directory: Path, request: OfficialSourceRequestV1
) -> Path:
    parsed = urlsplit(request.uri)
    path = str(parsed.path).lstrip("/")
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.ecb.europa.eu"
        or not path
        or ".." in Path(path).parts
    ):
        raise ValueError(f"unsafe ECB corpus path: {request.uri}")
    return source_directory / "www.ecb.europa.eu" / path


def _read_or_fetch(
    local_file: Path,
    uri: str,
    *,
    fetch_missing: bool,
) -> tuple[bytes, str, str | None]:
    if local_file.exists():
        if not local_file.is_file():
            raise ValueError(f"retained ECB path is not a file: {local_file}")
        return local_file.read_bytes(), uri, None
    if not fetch_missing:
        raise FileNotFoundError(local_file)
    content, resolved_uri, content_type = _fetch(uri)
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(content)
    return content, resolved_uri, content_type


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    resolved_uri: str,
    reported_content_type: str | None,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    expected = {
        OfficialSourceFormat.JSON: "application/json",
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
    }[request.source_format]
    if urlsplit(resolved_uri).hostname not in {
        "www.ecb.europa.eu",
        "ecb.europa.eu",
    }:
        raise ValueError("ECB snapshot resolved to another host")
    if (
        reported_content_type is not None
        and reported_content_type.partition(";")[0].strip().lower() != expected
    ):
        raise ValueError("ECB snapshot content type differs")
    content_type = expected
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
        content, resolved_uri, content_type = _read_or_fetch(
            _local_path(source_directory, request),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        return _snapshot(
            request,
            content,
            resolved_uri,
            content_type,
            captured_at_ns,
        )

    versions_snapshot = capture(
        build_ecb_balance_of_payments_foedb_versions_request(registry)
    )
    version, version_hash = parse_ecb_balance_of_payments_foedb_version(
        versions_snapshot
    )
    metadata_snapshot = capture(
        build_ecb_balance_of_payments_foedb_metadata_request(
            registry, version, version_hash
        )
    )
    metadata = json.loads(metadata_snapshot.content)
    if not isinstance(metadata, dict):
        raise TypeError("ECB FOEDB metadata is not an object")
    chunk_requests = build_ecb_balance_of_payments_foedb_chunk_requests(
        registry,
        version,
        version_hash,
        total_records=int(metadata["total_records"]),
        chunk_size=int(metadata["chunk_size"]),
        chunk_group_size=int(metadata["chunk_group_size"]),
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        chunk_snapshots = tuple(pool.map(capture, chunk_requests))

    index_requests = build_ecb_balance_of_payments_index_requests(registry)
    with ThreadPoolExecutor(max_workers=2) as pool:
        index_snapshots = tuple(pool.map(capture, index_requests))
    by_index_uri = {item.request.uri: item for item in index_snapshots}
    release_index = parse_ecb_balance_of_payments_inventory(
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        by_index_uri[index_requests[0].uri],
        by_index_uri[index_requests[1].uri],
        as_of_date=args.as_of,
    )

    release_requests = build_ecb_balance_of_payments_release_requests(
        registry, release_index
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_values = tuple(pool.map(capture, release_requests))
    release_snapshots = {item.request.uri: item for item in release_values}

    support_requests = discover_ecb_balance_of_payments_support_requests(
        registry, release_index, release_snapshots
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        support_values = tuple(pool.map(capture, support_requests))
    support_snapshots = {item.request.uri: item for item in support_values}

    manifest = build_ecb_balance_of_payments_archive_manifest(
        registry,
        release_index,
        release_snapshots,
        support_snapshots,
    )
    replay_ecb_balance_of_payments_archive(
        registry,
        manifest,
        versions_snapshot,
        metadata_snapshot,
        chunk_snapshots,
        by_index_uri[index_requests[0].uri],
        by_index_uri[index_requests[1].uri],
        release_snapshots,
        support_snapshots,
    )
    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = asset_directory / (
        packaged_ecb_balance_of_payments_manifest_path().name
    )
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": manifest.release_index.as_of_date,
                "database_version": manifest.release_index.database_version,
                "database_record_count": manifest.release_index.database_total_records,
                "database_artifact_count": len(
                    manifest.release_index.database_artifacts
                ),
                "release_count": manifest.release_count,
                "legacy_release_count": manifest.legacy_release_count,
                "current_release_count": manifest.current_release_count,
                "exclusion_count": len(manifest.release_index.exclusions),
                "dated_release_count": manifest.dated_release_count,
                "unavailable_date_count": manifest.unavailable_date_count,
                "exact_second_count": manifest.exact_second_count,
                "exact_minute_count": manifest.exact_minute_count,
                "date_only_count": manifest.date_only_count,
                "migration_timestamp_rejection_count": (
                    manifest.migration_timestamp_rejection_count
                ),
                "support_html_count": manifest.support_html_count,
                "support_pdf_count": manifest.support_pdf_count,
                "published_value_count": manifest.published_value_count,
                "component_value_count": manifest.component_value_count,
                "revision_disclosure_count": manifest.revision_disclosure_count,
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
