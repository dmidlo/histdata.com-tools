#!/usr/bin/env python3
"""Refresh or replay the complete Census housing-construction corpus."""

from __future__ import annotations

import argparse
import base64
import json
import time
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_housing_construction_archive import (
    HOUSING_INDEX_ENVELOPE_SCHEMA_VERSION,
    HOUSING_INDEX_URI,
    build_census_housing_archive_manifest,
    build_census_housing_archive_requests,
    build_census_housing_index_request,
    packaged_census_housing_archive_manifest_path,
    packaged_census_housing_index_path,
    parse_census_housing_release_index,
)

USER_AGENT = (
    "histdata.com-tools housing-construction archive qualification "
    "(contact: repository maintainers)"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="retained index, text-release, and PDF directory",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        help="override the package asset output directory",
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="fetch missing official bytes sequentially with retries",
    )
    return parser.parse_args()


def _fetch(uri: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            response = requests.get(
                uri,
                headers={"User-Agent": USER_AGENT},
                timeout=(15, 90),
            )
            if response.status_code == 429:
                time.sleep(min(30, attempt * 3))
                continue
            response.raise_for_status()
            return cast(bytes, response.content)
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 10:
                time.sleep(min(30, attempt * 2))
    raise RuntimeError(f"unable to fetch {uri}") from last_error


def _read_or_fetch(path: Path, uri: str, *, fetch_missing: bool) -> bytes:
    if path.exists():
        return path.read_bytes()
    if not fetch_missing:
        raise FileNotFoundError(path)
    content = _fetch(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
        OfficialSourceFormat.TEXT: "text/plain",
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


def _local_name(uri: str) -> str:
    return Path(urlparse(uri).path).name


def main() -> None:
    args = _arguments()
    source_directory = args.source_directory.resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    captured_at_ns = time.time_ns()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)

    index_request = build_census_housing_index_request(
        registry, as_of_date=args.as_of
    )
    index_content = _read_or_fetch(
        source_directory / "index.html",
        HOUSING_INDEX_URI,
        fetch_missing=args.fetch_missing,
    )
    index_snapshot = _snapshot(index_request, index_content, captured_at_ns)
    release_index = parse_census_housing_release_index(
        index_snapshot, as_of_date=args.as_of
    )

    snapshots: dict[str, OfficialRawSnapshotV1] = {}
    for request in build_census_housing_archive_requests(
        registry, release_index
    ):
        content = _read_or_fetch(
            source_directory / _local_name(request.uri),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        snapshots[request.uri] = _snapshot(request, content, captured_at_ns)
    manifest = build_census_housing_archive_manifest(
        registry, profile, release_index, snapshots
    )

    asset_directory = (
        args.asset_directory.resolve()
        if args.asset_directory is not None
        else packaged_census_housing_archive_manifest_path().parent
    )
    asset_directory.mkdir(parents=True, exist_ok=True)
    index_payload = {
        "schema_version": HOUSING_INDEX_ENVELOPE_SCHEMA_VERSION,
        "snapshot": index_snapshot.to_dict(),
        "content_base64": base64.b64encode(index_content).decode("ascii"),
    }
    (asset_directory / packaged_census_housing_index_path().name).write_text(
        canonical_contract_json(index_payload) + "\n", encoding="utf-8"
    )
    (
        asset_directory / packaged_census_housing_archive_manifest_path().name
    ).write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "publications": len(manifest.publications),
                "measures": manifest.measure_occurrence_count,
                "comparable_revisions": manifest.comparable_measure_count,
                "changed_revisions": manifest.changed_revision_count,
                "artifacts": manifest.raw_artifact_count,
                "bytes": manifest.total_content_bytes,
                "corrected_links": manifest.corrected_link_count,
                "catch_up_publications": manifest.catch_up_publication_count,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
