#!/usr/bin/env python3
"""Refresh or replay the complete official Federal Reserve H.6 archive."""

from __future__ import annotations

import argparse
import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context import (
    H6_INDEX_PAGES_SCHEMA_VERSION,
    H6_INDEX_URI,
    H6_RELEASE_DATES_URI,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_federal_reserve_h6_archive_manifest,
    build_federal_reserve_h6_index_requests,
    build_federal_reserve_h6_release_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    packaged_federal_reserve_h6_indexes_path,
    packaged_federal_reserve_h6_manifest_path,
    parse_federal_reserve_h6_release_index,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-538 H.6 archive qualification "
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
        help="Caller-retained exact index, release, and timing corpus.",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        default=DEFAULT_ASSET_DIRECTORY,
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
                timeout=(15, 120),
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
    if request.uri == H6_INDEX_URI:
        return source_directory / "index.html"
    if request.uri == H6_RELEASE_DATES_URI:
        return source_directory / "releaseDates.json"
    parts = tuple(
        item for item in urlparse(request.uri).path.split("/") if item
    )
    if len(parts) < 3 or parts[:2] != ("releases", "h6"):
        raise ValueError("H.6 request URI path differs")
    release_token = parts[2]
    if request.source_format is OfficialSourceFormat.PDF:
        return source_directory / "pdfs" / f"{release_token}.pdf"
    suffix = (
        ".txt"
        if request.source_format is OfficialSourceFormat.TEXT
        else ".html"
    )
    return source_directory / "releases" / f"{release_token}{suffix}"


def _read_or_fetch(
    path: Path, uri: str, *, fetch_missing: bool
) -> tuple[bytes, str]:
    if path.exists():
        if not path.is_file():
            raise ValueError(f"retained H.6 path is not a file: {path}")
        return path.read_bytes(), uri
    if not fetch_missing:
        raise FileNotFoundError(path)
    content, resolved_uri = _fetch(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content, resolved_uri


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    resolved_uri: str,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.JSON: "application/json",
        OfficialSourceFormat.PDF: "application/pdf",
        OfficialSourceFormat.TEXT: "text/plain",
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
    profile = built_in_united_states_backfill_profile(registry)
    captured_at_ns = time.time_ns()

    def capture(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
        content, resolved_uri = _read_or_fetch(
            _local_path(source_directory, request),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        return _snapshot(request, content, resolved_uri, captured_at_ns)

    index_requests = build_federal_reserve_h6_index_requests(
        registry, as_of_date=args.as_of
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        index_snapshots = tuple(pool.map(capture, index_requests))
    release_index = parse_federal_reserve_h6_release_index(
        index_snapshots, as_of_date=args.as_of
    )
    release_requests = build_federal_reserve_h6_release_requests(
        registry, release_index
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(capture, release_requests))
    snapshots_by_uri = {
        item.request.uri: item
        for item in (*index_snapshots, *release_snapshots)
    }
    manifest = build_federal_reserve_h6_archive_manifest(
        registry,
        profile,
        release_index,
        snapshots_by_uri,
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        asset_directory / packaged_federal_reserve_h6_manifest_path().name
    )
    indexes_path = (
        asset_directory / packaged_federal_reserve_h6_indexes_path().name
    )
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    index_payload = {
        "schema_version": H6_INDEX_PAGES_SCHEMA_VERSION,
        "pages": [
            {
                "uri": item.request.uri,
                "content_base64": base64.b64encode(item.content).decode(
                    "ascii"
                ),
            }
            for item in sorted(
                index_snapshots, key=lambda value: value.request.uri
            )
        ],
    }
    indexes_path.write_text(
        json.dumps(
            index_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": manifest.release_index.as_of_date,
                "publication_count": len(manifest.publications),
                "weekly_count": manifest.weekly_count,
                "monthly_count": manifest.monthly_count,
                "exact_minute_count": manifest.exact_minute_count,
                "scheduled_only_count": manifest.scheduled_only_count,
                "revision_occurrence_count": (
                    manifest.revision_occurrence_count
                ),
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "source_directory": str(source_directory),
                "manifest_path": str(manifest_path),
                "indexes_path": str(indexes_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
