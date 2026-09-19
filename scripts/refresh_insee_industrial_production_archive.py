#!/usr/bin/env python3
"""Refresh or replay the official INSEE industrial-production archive for #539."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlsplit

import requests

from histdatacom.market_context.insee_industrial_production_archive import (
    build_insee_industrial_production_archive_manifest,
    build_insee_industrial_production_catalog_requests,
    build_insee_industrial_production_current_series_requests,
    build_insee_industrial_production_release_requests,
    build_insee_industrial_production_search_requests,
    packaged_insee_industrial_production_manifest_path,
    parse_insee_industrial_production_search,
    replay_insee_industrial_production_archive,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 INSEE industrial-production qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIRECTORY: Final = (
    REPO_ROOT
    / ".histdatacom"
    / "issue-539-insee-industrial-production"
    / "corpus"
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
        default=packaged_insee_industrial_production_manifest_path(),
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
        body = json.loads(request.body_text or "{}")
        query = str(body.get("q", ""))
        if query == '"Industrial production index"':
            return root / "search.json"
        raise ValueError(
            f"unsupported INSEE industrial production search query: {query}"
        )
    if role == "release":
        return (
            root
            / "releases"
            / f"{urlsplit(request.uri).path.rsplit('/', 1)[-1]}.html"
        )
    if role == "catalog":
        body = json.loads(request.body_text or "{}")
        start = int(body.get("start", -1))
        if start not in {0, 1000}:
            raise ValueError(f"unsupported INSEE catalog page offset: {start}")
        return root / "current" / f"catalog-{start}.json"
    if role == "series":
        return (
            root
            / "current"
            / f"{urlsplit(request.uri).path.rsplit('/', 1)[-1]}.xml"
        )
    raise ValueError(
        f"unsupported INSEE industrial production corpus role: {role}"
    )


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
            f"INSEE industrial production response size is invalid: {source}"
        )
    if request.source_format is OfficialSourceFormat.JSON:
        if content.lstrip()[:1] not in {b"{", b"["}:
            raise ValueError(
                f"retained INSEE industrial production JSON differs: {source}"
            )
    elif request.source_format is OfficialSourceFormat.HTML:
        if b"<html" not in content[:16_384].lower():
            raise ValueError(
                f"retained INSEE industrial production HTML differs: {source}"
            )
    elif (
        request.source_format is OfficialSourceFormat.SDMX_21
        and b"StructureSpecificData" not in content[:2_048]
    ):
        raise ValueError(
            f"retained INSEE industrial production SDMX differs: {source}"
        )


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
                f"INSEE industrial production response resolved outside its host: {response.url}"
            )
        if response.url != request.uri:
            raise ValueError(
                "INSEE industrial production response resolved to a different endpoint"
            )
        expected = _content_type(request.source_format)
        reported = response.headers.get("Content-Type", "").split(";", 1)[0]
        if reported.casefold() != expected:
            raise ValueError(
                "INSEE industrial production response content type differs: "
                f"expected {expected}, received {reported or '<missing>'}"
            )
        _validate_content(request, content, request.uri)
        return content
    raise RuntimeError(
        f"INSEE industrial production request retry bound exhausted: {request.uri}"
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
                f"retained INSEE industrial production artifact is missing: {path}"
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

    search_snapshots = tuple(
        _snapshot(
            root,
            request,
            role="search",
            fetch_missing=args.fetch_missing,
        )
        for request in build_insee_industrial_production_search_requests(
            registry
        )
    )
    search, documents = parse_insee_industrial_production_search(
        search_snapshots,
        as_of_date=args.as_of,
    )
    release_requests = build_insee_industrial_production_release_requests(
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
    catalog_snapshots = tuple(
        _snapshot(
            root,
            request,
            role="catalog",
            fetch_missing=args.fetch_missing,
        )
        for request in build_insee_industrial_production_catalog_requests(
            registry
        )
    )
    series_snapshots = tuple(
        _snapshot(
            root,
            request,
            role="series",
            fetch_missing=args.fetch_missing,
        )
        for request in build_insee_industrial_production_current_series_requests(
            registry
        )
    )
    manifest = build_insee_industrial_production_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshots,
        series_snapshots,
    )
    replay_insee_industrial_production_archive(
        registry,
        manifest,
        search_snapshots,
        release_snapshots,
        catalog_snapshots,
        series_snapshots,
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
