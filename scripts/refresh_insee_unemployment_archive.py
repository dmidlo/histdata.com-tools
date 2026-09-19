#!/usr/bin/env python3
"""Refresh or replay the official INSEE ILO-unemployment archive for #539."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlsplit

import requests

from histdatacom.market_context.insee_unemployment_archive import (
    build_insee_unemployment_archive_manifest,
    build_insee_unemployment_catalog_request,
    build_insee_unemployment_current_series_request,
    build_insee_unemployment_release_requests,
    build_insee_unemployment_search_request,
    packaged_insee_unemployment_manifest_path,
    parse_insee_unemployment_search,
    replay_insee_unemployment_archive,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 INSEE ILO-unemployment qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIRECTORY: Final = (
    REPO_ROOT / ".histdatacom" / "issue-539-insee-unemployment" / "corpus"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        required=True,
        help="Explicit YYYY-MM-DD observation boundary.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=DEFAULT_SOURCE_DIRECTORY,
        help="Operator-retained exact INSEE response corpus.",
    )
    parser.add_argument(
        "--asset-path",
        type=Path,
        default=packaged_insee_unemployment_manifest_path(),
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="Fetch official artifacts absent from the retained corpus.",
    )
    return parser.parse_args()


def _corpus_path(
    root: Path,
    request: OfficialSourceRequestV1,
    *,
    role: str,
) -> Path:
    if role == "search":
        return root / "search.json"
    if role == "release":
        return (
            root
            / "releases"
            / f"{urlsplit(request.uri).path.rsplit('/', 1)[-1]}.html"
        )
    if role == "catalog":
        return root / "current" / "catalog.json"
    if role == "series":
        return root / "current" / "011818543.xml"
    raise ValueError(f"unsupported INSEE unemployment corpus role: {role}")


def _content_type(source_format: OfficialSourceFormat) -> str:
    return {
        OfficialSourceFormat.JSON: "application/json",
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.SDMX_21: "application/xml",
    }[source_format]


def _validate_content(
    request: OfficialSourceRequestV1,
    content: bytes,
    source: str | Path,
) -> None:
    if not content or len(content) > 64 * 1024 * 1024:
        raise ValueError(
            f"INSEE unemployment response size is invalid: {source}"
        )
    if request.source_format is OfficialSourceFormat.JSON:
        if content.lstrip()[:1] not in {b"{", b"["}:
            raise ValueError(
                f"retained INSEE unemployment JSON differs: {source}"
            )
    elif request.source_format is OfficialSourceFormat.HTML:
        if b"<html" not in content[:16_384].lower():
            raise ValueError(
                f"retained INSEE unemployment HTML differs: {source}"
            )
    elif (
        request.source_format is OfficialSourceFormat.SDMX_21
        and b"StructureSpecificData" not in content[:2_048]
    ):
        raise ValueError(f"retained INSEE unemployment SDMX differs: {source}")


def _fetch(request: OfficialSourceRequestV1) -> bytes:
    delay = 1.0
    for attempt in range(1, 7):
        try:
            response = requests.request(
                request.method.value,
                request.uri,
                data=(
                    request.body_text.encode("utf-8")
                    if request.body_text
                    else None
                ),
                headers={
                    "User-Agent": USER_AGENT,
                    **(
                        {"Content-Type": request.body_content_type}
                        if request.body_content_type
                        else {}
                    ),
                },
                timeout=(10.0, 60.0),
            )
        except requests.RequestException:
            if attempt == 6:
                raise
            time.sleep(delay)
            delay *= 2.0
            continue
        if response.status_code in {429, 500, 502, 503, 504} and attempt < 6:
            retry_after = response.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else delay
            except ValueError:
                wait = delay
            time.sleep(min(max(wait, delay), 32.0))
            delay *= 2.0
            continue
        response.raise_for_status()
        content = cast(bytes, response.content)
        requested = urlsplit(request.uri)
        resolved = urlsplit(response.url)
        if (
            resolved.scheme != "https"
            or resolved.hostname != requested.hostname
            or resolved.username is not None
            or resolved.fragment
        ):
            raise ValueError(
                f"INSEE unemployment response resolved outside its host: {response.url}"
            )
        expected = _content_type(request.source_format)
        reported = response.headers.get("Content-Type", "").split(";", 1)[0]
        if reported.casefold() != expected:
            raise ValueError(
                "INSEE unemployment response content type differs: "
                f"expected {expected}, received {reported or '<missing>'}"
            )
        _validate_content(request, content, request.uri)
        return content
    raise RuntimeError(
        f"INSEE unemployment request retry bound exhausted: {request.uri}"
    )


def _snapshot(
    root: Path,
    request: OfficialSourceRequestV1,
    *,
    role: str,
    fetch_missing: bool,
) -> OfficialRawSnapshotV1:
    path = _corpus_path(root, request, role=role)
    if not path.is_file():
        if not fetch_missing:
            raise FileNotFoundError(
                f"retained INSEE unemployment artifact is missing: {path}"
            )
        content = _fetch(request)
        _validate_content(request, content, request.uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        time.sleep(0.35)
    content = path.read_bytes()
    _validate_content(request, content, path)
    captured = time.time_ns()
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=captured,
        completed_at_ns=captured + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": _content_type(request.source_format)},
        content=content,
        content_type=_content_type(request.source_format),
    )


def main() -> int:
    args = _arguments()
    root = args.source_directory.expanduser().resolve()
    registry = load_packaged_official_source_registry()

    search_request = build_insee_unemployment_search_request(registry)
    search_snapshot = _snapshot(
        root,
        search_request,
        role="search",
        fetch_missing=args.fetch_missing,
    )
    search, documents = parse_insee_unemployment_search(
        search_snapshot,
        as_of_date=args.as_of,
    )
    release_requests = build_insee_unemployment_release_requests(
        registry, documents
    )
    release_snapshots = {
        int(urlsplit(request.uri).path.rsplit("/", 1)[-1]): _snapshot(
            root,
            request,
            role="release",
            fetch_missing=args.fetch_missing,
        )
        for request in release_requests
    }
    catalog_snapshot = _snapshot(
        root,
        build_insee_unemployment_catalog_request(registry),
        role="catalog",
        fetch_missing=args.fetch_missing,
    )
    series_snapshot = _snapshot(
        root,
        build_insee_unemployment_current_series_request(registry),
        role="series",
        fetch_missing=args.fetch_missing,
    )
    manifest = build_insee_unemployment_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshot,
        series_snapshot,
    )
    replay_insee_unemployment_archive(
        registry,
        manifest,
        search_snapshot,
        release_snapshots,
        catalog_snapshot,
        series_snapshot,
    )
    asset_path = args.asset_path.expanduser().resolve()
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        f"wrote {asset_path} ({len(manifest.releases)} releases, "
        f"{len(manifest.comparisons)} exact-scope comparisons)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
