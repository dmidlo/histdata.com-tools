#!/usr/bin/env python3
"""Refresh or replay the qualified official Eurostat GDP release archive."""

from __future__ import annotations

import argparse
import json
import math
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context.eurostat_gdp_archive import (
    EUROSTAT_GDP_AS_OF_DATE,
    EUROSTAT_GDP_SEARCH_PAGE_COUNT,
    EurostatGdpArchiveManifestV1,
    build_eurostat_gdp_archive_manifest,
    build_eurostat_gdp_dataset_request,
    build_eurostat_gdp_document_requests,
    build_eurostat_gdp_release_requests,
    build_eurostat_gdp_search_requests,
    packaged_eurostat_gdp_manifest_path,
    parse_eurostat_gdp_inventory,
)
from histdatacom.market_context.eurostat_gdp_archive import (
    _product_code as _archive_product_code,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 Eurostat GDP qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)
DEFAULT_REQUEST_INTERVAL_SECONDS: Final = 13.0
MAX_REQUEST_INTERVAL_SECONDS: Final = 3_600.0
MAX_FETCH_ATTEMPTS: Final = 10


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        default=EUROSTAT_GDP_AS_OF_DATE,
        choices=(EUROSTAT_GDP_AS_OF_DATE,),
        help="Frozen packaged qualification boundary.",
    )
    parser.add_argument(
        "--page-count",
        type=int,
        default=EUROSTAT_GDP_SEARCH_PAGE_COUNT,
        choices=(EUROSTAT_GDP_SEARCH_PAGE_COUNT,),
        help="Frozen contiguous Atom-search page count at the archive boundary.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="Caller-retained exact Atom, JSON-stat, and release corpus.",
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


def _product_code(uri: str) -> str:
    return _archive_product_code(uri)


def _local_path(source_directory: Path, request: OfficialSourceRequestV1) -> Path:
    if request.page_number is not None:
        return source_directory / "index" / f"page_{request.page_number:03d}.xml"
    if request.source_format is OfficialSourceFormat.JSON_STAT:
        return source_directory / "datasets" / "namq_10_gdp.json"
    if "/eurostat/documents/" in urlparse(request.uri).path.casefold():
        suffix = {
            OfficialSourceFormat.HTML: "html",
            OfficialSourceFormat.PDF: "pdf",
        }.get(request.source_format)
        if suffix is None:
            raise ValueError("Eurostat GDP document format differs")
        return source_directory / "documents" / f"{_product_code(request.uri)}.{suffix}"
    return source_directory / "releases" / f"{_product_code(request.uri)}.html"


class _CanonicalUriParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.canonical_values: list[str] = []
        self.open_graph_values: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.casefold(): value for key, value in attrs}
        if (
            tag.casefold() == "link"
            and "canonical" in (values.get("rel") or "").casefold().split()
            and values.get("href")
        ):
            self.canonical_values.append(cast(str, values["href"]))
        if (
            tag.casefold() == "meta"
            and (values.get("property") or "").casefold() == "og:url"
            and values.get("content")
        ):
            self.open_graph_values.append(cast(str, values["content"]))


def _canonical_product_path(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "https" or parsed.netloc != "ec.europa.eu":
        raise ValueError("retained Eurostat landing URI is not allowlisted")
    if parsed.query or parsed.fragment:
        raise ValueError("retained Eurostat landing URI has query or fragment")
    path = parsed.path
    if path.startswith("/eurostat/en/"):
        path = "/eurostat/" + path.removeprefix("/eurostat/en/")
    return path.rstrip("/")


def _resolved_uri(request: OfficialSourceRequestV1, content: bytes) -> str:
    if (
        request.source_format is not OfficialSourceFormat.HTML
        or urlparse(request.uri).path != "/eurostat/product"
    ):
        return cast(str, request.uri)
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("retained Eurostat landing is not UTF-8") from exc
    parser = _CanonicalUriParser()
    parser.feed(text)
    parser.close()
    canonical_values = tuple(dict.fromkeys(parser.canonical_values))
    open_graph_values = tuple(dict.fromkeys(parser.open_graph_values))
    if (
        len(canonical_values) > 1
        or len(open_graph_values) > 1
        or not (canonical_values or open_graph_values)
    ):
        raise ValueError("retained Eurostat landing has no unique canonical URI")
    selected = canonical_values[0] if canonical_values else open_graph_values[0]
    selected_path = _canonical_product_path(selected)
    if any(
        _canonical_product_path(uri) != selected_path
        for uri in (*canonical_values, *open_graph_values)
    ):
        raise ValueError("retained Eurostat landing URI metadata differs")
    if _product_code(request.uri) not in selected_path.casefold():
        raise ValueError("retained Eurostat landing canonical product differs")
    return selected


class _PoliteFetcher:
    def __init__(self, interval_seconds: float) -> None:
        if (
            not math.isfinite(interval_seconds)
            or not DEFAULT_REQUEST_INTERVAL_SECONDS
            <= interval_seconds
            <= MAX_REQUEST_INTERVAL_SECONDS
        ):
            raise ValueError(
                "Eurostat request interval must be between 13 and 3600 seconds"
            )
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

    def fetch(self, request: OfficialSourceRequestV1) -> tuple[bytes, str, str]:
        last_error: Exception | None = None
        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            self._wait_for_slot()
            self._last_request_monotonic = time.monotonic()
            try:
                response = requests.get(
                    request.uri,
                    headers={"User-Agent": USER_AGENT},
                    timeout=(15, 180),
                )
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
                parsed = urlparse(response.url)
                if parsed.scheme != "https" or parsed.netloc != "ec.europa.eu":
                    raise RuntimeError("Eurostat response left the allowlisted host")
                if request.source_format in {
                    OfficialSourceFormat.HTML,
                    OfficialSourceFormat.PDF,
                } and (_product_code(request.uri) not in parsed.path.casefold()):
                    raise RuntimeError(
                        "Eurostat response redirected to another product"
                    )
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
                return cast(bytes, response.content), response.url, content_type
            except requests.RequestException as exc:
                last_error = exc
                if attempt < MAX_FETCH_ATTEMPTS:
                    time.sleep(min(60.0, attempt * 5.0))
        raise RuntimeError(f"unable to fetch {request.uri}") from last_error


def _read_or_fetch(
    path: Path,
    request: OfficialSourceRequestV1,
    *,
    fetch_missing: bool,
    fetcher: _PoliteFetcher,
) -> tuple[bytes, str, str]:
    if path.exists():
        if not path.is_file():
            raise ValueError(f"retained Eurostat path is not a file: {path}")
        content = path.read_bytes()
        content_type = {
            OfficialSourceFormat.ATOM: "application/atom+xml",
            OfficialSourceFormat.JSON_STAT: "application/json",
            OfficialSourceFormat.HTML: "text/html",
            OfficialSourceFormat.PDF: "application/pdf",
        }[request.source_format]
        try:
            resolved_uri = _resolved_uri(request, content)
        except ValueError as exc:
            raise ValueError(f"retained Eurostat artifact differs: {path}") from exc
        return content, resolved_uri, content_type
    if not fetch_missing:
        raise FileNotFoundError(path)
    content, response_uri, content_type = fetcher.fetch(request)
    resolved_uri = _resolved_uri(request, content)
    if (
        request.source_format is OfficialSourceFormat.HTML
        and urlparse(request.uri).path == "/eurostat/product"
        and _canonical_product_path(response_uri)
        != _canonical_product_path(resolved_uri)
    ):
        raise ValueError("Eurostat response and canonical landing URI differ")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content, resolved_uri, content_type


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    resolved_uri: str,
    content_type: str,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
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


def _summary(manifest: EurostatGdpArchiveManifestV1) -> dict[str, object]:
    return {
        "manifest_id": manifest.manifest_id,
        "as_of_date": manifest.as_of_date,
        "release_count": manifest.release_count,
        "release_document_count": sum(
            item.document_artifact is not None for item in manifest.releases
        ),
        "exact_publication_time_count": manifest.exact_publication_time_count,
        "published_value_count": manifest.published_value_count,
        "revision_comparison_count": manifest.revision_comparison_count,
        "changed_revision_count": manifest.changed_revision_count,
        "search_result_count": manifest.inventory.query_result_count,
        "explicit_exclusion_count": len(manifest.inventory.exclusions),
        "dataset_observation_count": len(manifest.current_dataset.observations),
        "dataset_time_start": manifest.current_dataset.time_start,
        "dataset_time_end": manifest.current_dataset.time_end,
        "searchable_release_gap_count": len(
            manifest.searchable_release_gap_reference_periods
        ),
        "preliminary_flash_count": manifest.preliminary_flash_count,
        "flash_count": manifest.flash_count,
        "first_estimate_count": manifest.first_estimate_count,
        "second_estimate_count": manifest.second_estimate_count,
        "third_estimate_count": manifest.third_estimate_count,
        "regular_estimate_count": manifest.regular_estimate_count,
        "page_date_offset_count": manifest.page_date_offset_count,
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
        content, resolved_uri, content_type = _read_or_fetch(
            _local_path(source_directory, request),
            request,
            fetch_missing=args.fetch_missing,
            fetcher=fetcher,
        )
        return _snapshot(request, content, resolved_uri, content_type, captured_at_ns)

    search_snapshots = tuple(
        capture(request)
        for request in build_eurostat_gdp_search_requests(
            registry, page_count=args.page_count
        )
    )
    dataset_snapshot = capture(build_eurostat_gdp_dataset_request(registry))
    inventory = parse_eurostat_gdp_inventory(search_snapshots, as_of_date=args.as_of)
    release_snapshots = tuple(
        capture(request)
        for request in build_eurostat_gdp_release_requests(registry, inventory)
    )
    document_snapshots = tuple(
        capture(request)
        for request in build_eurostat_gdp_document_requests(
            registry, inventory, release_snapshots
        )
    )
    manifest = build_eurostat_gdp_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=args.as_of,
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = asset_directory / packaged_eurostat_gdp_manifest_path().name
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
