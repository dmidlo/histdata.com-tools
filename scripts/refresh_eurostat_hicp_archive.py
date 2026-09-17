#!/usr/bin/env python3
"""Refresh or replay the complete official Eurostat HICP release archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context.eurostat_hicp_archive import (
    EUROSTAT_HICP_AS_OF_DATE,
    EUROSTAT_HICP_MIGRATED_INDEX_START_DATE,
    EurostatHicpArchiveManifestV1,
    build_eurostat_hicp_archive_manifest,
    build_eurostat_hicp_dataset_requests,
    build_eurostat_hicp_document_requests,
    build_eurostat_hicp_index_requests,
    build_eurostat_hicp_legacy_release_requests,
    build_eurostat_hicp_release_requests,
    load_packaged_eurostat_hicp_archive_manifest,
    packaged_eurostat_hicp_manifest_path,
    parse_eurostat_hicp_publication_index,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 Eurostat HICP qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)
DEFAULT_REQUEST_INTERVAL_SECONDS: Final = 13.0
MAX_FETCH_ATTEMPTS: Final = 10


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        default=EUROSTAT_HICP_AS_OF_DATE,
        help="Explicit archive boundary; defaults to the packaged qualification date.",
    )
    parser.add_argument(
        "--page-count",
        type=int,
        required=True,
        help="Exact contiguous publication-index page count observed at the boundary.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="Caller-retained exact dataset, index, and release corpus.",
    )
    parser.add_argument(
        "--legacy-uri-file",
        type=Path,
        help=(
            "Optional newline-delimited pre-migration release URI inventory; "
            "the packaged manifest is used when this is omitted."
        ),
    )
    parser.add_argument("--asset-directory", type=Path, default=DEFAULT_ASSET_DIRECTORY)
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="Fetch official artifacts absent from the retained corpus.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=DEFAULT_REQUEST_INTERVAL_SECONDS,
        help="Minimum delay between Eurostat network requests.",
    )
    return parser.parse_args()


def _release_slug(uri: str) -> str:
    slug = Path(urlparse(uri).path).name
    if not slug or slug in {"publications", "data"}:
        raise ValueError(f"Eurostat release URI has no stable slug: {uri}")
    return slug


def _local_path(source_directory: Path, request: OfficialSourceRequestV1) -> Path:
    path_name = Path(urlparse(request.uri).path).name
    if path_name in {"prc_hicp_fp", "prc_hicp_fpd"}:
        return source_directory / "datasets" / f"{path_name}.json"
    if request.page_number is not None:
        return source_directory / "index" / f"page_{request.page_number:03d}.html"
    if "/eurostat/documents/" in urlparse(request.uri).path:
        candidates = tuple(
            item
            for item in Path(urlparse(request.uri).path).parts
            if item.casefold().startswith("2-")
            and any(
                f"-{product_code}-en." in item.casefold()
                for product_code in ("ap", "ap1", "bp", "cp")
            )
        )
        if len(candidates) != 1:
            raise ValueError(
                f"Eurostat release document URI has no stable name: {request.uri}"
            )
        suffix = {
            OfficialSourceFormat.HTML: ".html",
            OfficialSourceFormat.PDF: ".pdf",
        }[request.source_format]
        stem = candidates[0].split(".", 1)[0].casefold()
        uri_digest = hashlib.sha256(request.uri.encode("utf-8")).hexdigest()[:16]
        return source_directory / "documents" / f"{stem}-{uri_digest}{suffix}"
    return source_directory / "releases" / f"{_release_slug(request.uri)}.html"


class _PoliteFetcher:
    def __init__(self, interval_seconds: float) -> None:
        if interval_seconds < DEFAULT_REQUEST_INTERVAL_SECONDS:
            raise ValueError("Eurostat request interval must be at least 13 seconds")
        self._interval_seconds = interval_seconds
        self._last_request_monotonic: float | None = None

    def _wait_for_slot(self) -> None:
        if self._last_request_monotonic is None:
            return
        remaining = self._interval_seconds - (
            time.monotonic() - self._last_request_monotonic
        )
        if remaining > 0:
            time.sleep(remaining)

    def fetch(self, uri: str) -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            self._wait_for_slot()
            try:
                response = requests.get(
                    uri,
                    headers={"User-Agent": USER_AGENT},
                    timeout=(15, 180),
                )
                self._last_request_monotonic = time.monotonic()
                if response.status_code == 429:
                    retry_after = response.headers.get(
                        "X-Rate-Limit-Retry-After-Seconds",
                        response.headers.get("Retry-After", "60"),
                    )
                    try:
                        delay = max(self._interval_seconds, float(retry_after))
                    except ValueError:
                        delay = 60.0
                    time.sleep(min(delay, 300.0))
                    continue
                if response.status_code in {408, 425, 500, 502, 503, 504}:
                    time.sleep(min(60.0, attempt * 5.0))
                    continue
                response.raise_for_status()
                if response.url.rstrip("/") != uri.rstrip("/"):
                    raise RuntimeError(
                        f"Eurostat request redirected unexpectedly: {uri} -> "
                        f"{response.url}"
                    )
                return cast(bytes, response.content)
            except requests.RequestException as exc:
                last_error = exc
                if attempt < MAX_FETCH_ATTEMPTS:
                    time.sleep(min(60.0, attempt * 5.0))
        raise RuntimeError(f"unable to fetch {uri}") from last_error


def _read_or_fetch(
    local_file: Path,
    request: OfficialSourceRequestV1,
    *,
    fetch_missing: bool,
    fetcher: _PoliteFetcher,
) -> bytes:
    if local_file.exists():
        if not local_file.is_file():
            raise ValueError(f"retained Eurostat path is not a file: {local_file}")
        return local_file.read_bytes()
    if not fetch_missing:
        raise FileNotFoundError(local_file)
    content = fetcher.fetch(request.uri)
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(content)
    return content


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.JSON_STAT: "application/json",
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
    }[request.source_format]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=captured_at_ns,
        completed_at_ns=captured_at_ns,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def _legacy_uris(path: Path | None) -> tuple[str, ...]:
    if path is not None:
        source = path.expanduser().resolve()
        values = tuple(
            line.strip()
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        if not values:
            raise ValueError("legacy Eurostat URI inventory is empty")
        return values
    try:
        manifest = load_packaged_eurostat_hicp_archive_manifest()
    except FileNotFoundError as exc:
        raise ValueError(
            "bootstrap requires --legacy-uri-file before a packaged manifest exists"
        ) from exc
    values = tuple(
        item.publication.source_uri
        for item in manifest.releases
        if item.publication.release_date < EUROSTAT_HICP_MIGRATED_INDEX_START_DATE
    )
    if not values:
        raise ValueError("packaged manifest contains no legacy Eurostat releases")
    return values


def _summary(manifest: EurostatHicpArchiveManifestV1) -> dict[str, object]:
    return {
        "manifest_id": manifest.manifest_id,
        "as_of_date": manifest.as_of_date,
        "release_count": manifest.release_count,
        "migrated_publication_count": manifest.migrated_publication_count,
        "excluded_index_card_count": len(manifest.index_exclusions),
        "release_date_correction_count": sum(
            not item.release_date_from_uri for item in manifest.publications
        ),
        "legacy_unindexed_count": manifest.legacy_unindexed_count,
        "final_release_count": manifest.final_release_count,
        "flash_release_count": manifest.flash_release_count,
        "page_date_offset_count": manifest.page_date_offset_count,
        "card_date_offset_count": manifest.card_date_offset_count,
        "headline_only_release_count": manifest.headline_only_release_count,
        "headline_dataset_comparison_count": (
            manifest.headline_dataset_comparison_count
        ),
        "headline_dataset_difference_count": (
            manifest.headline_dataset_difference_count
        ),
        "overlap_count": manifest.overlap_count,
        "overlap_difference_count": len(manifest.overlap_differences),
        "final_artifact_gap_count": len(manifest.final_artifact_gap_reference_periods),
        "flash_artifact_gap_count": len(manifest.flash_artifact_gap_reference_periods),
        "numeric_overlap_difference_count": (manifest.numeric_overlap_difference_count),
        "status_overlap_difference_count": (manifest.status_overlap_difference_count),
        "raw_artifact_count": manifest.raw_artifact_count,
        "unique_content_sha256_count": manifest.unique_content_sha256_count,
        "total_content_bytes": manifest.total_content_bytes,
    }


def main() -> int:
    args = _arguments()
    source_directory = args.source_directory.expanduser().resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    registry = load_packaged_official_source_registry()
    fetcher = _PoliteFetcher(args.request_interval_seconds)
    captured_at_ns = time.time_ns()

    def capture(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
        content = _read_or_fetch(
            _local_path(source_directory, request),
            request,
            fetch_missing=args.fetch_missing,
            fetcher=fetcher,
        )
        return _snapshot(request, content, captured_at_ns)

    dataset_snapshots = tuple(
        capture(item) for item in build_eurostat_hicp_dataset_requests(registry)
    )
    index_snapshots = tuple(
        capture(item)
        for item in build_eurostat_hicp_index_requests(
            registry, page_count=args.page_count
        )
    )
    _, indexed_publications, _ = parse_eurostat_hicp_publication_index(
        index_snapshots, as_of_date=args.as_of
    )
    legacy_requests = build_eurostat_hicp_legacy_release_requests(
        registry, _legacy_uris(args.legacy_uri_file)
    )
    indexed_requests = build_eurostat_hicp_release_requests(
        registry, indexed_publications
    )
    primary_snapshots = tuple(
        capture(item) for item in (*legacy_requests, *indexed_requests)
    )
    landing_snapshots = tuple(
        item
        for item in primary_snapshots
        if "/eurostat/documents/" not in urlparse(item.request.uri).path
    )
    document_snapshots = tuple(
        capture(item)
        for item in build_eurostat_hicp_document_requests(registry, landing_snapshots)
    )
    release_snapshots = (*primary_snapshots, *document_snapshots)
    manifest = build_eurostat_hicp_archive_manifest(
        registry,
        dataset_snapshots,
        index_snapshots,
        release_snapshots,
        as_of_date=args.as_of,
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = asset_directory / packaged_eurostat_hicp_manifest_path().name
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                **_summary(manifest),
                "source_directory": str(source_directory),
                "manifest_path": str(manifest_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
