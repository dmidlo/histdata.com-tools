#!/usr/bin/env python3
"""Refresh or replay the complete Census MTIS archive corpus."""

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
from histdatacom.market_context.us_mtis_archive import (
    MTIS_BRIEFING_URI,
    MTIS_DIRECTORY_URI,
    MTIS_INDEX_ENVELOPE_SCHEMA_VERSION,
    MTIS_INDEX_URI,
    build_census_mtis_archive_manifest,
    build_census_mtis_archive_requests,
    build_census_mtis_index_requests,
    packaged_census_mtis_archive_manifest_path,
    packaged_census_mtis_index_path,
    parse_census_mtis_release_index,
)

USER_AGENT = (
    "histdata.com-tools MTIS archive qualification "
    "(contact: repository maintainers)"
)
LEGACY_WITNESS_SPECS = (
    ("20000229040444", "http://www.census.gov/mtis/www/current.html"),
    ("20000510124016", "http://www.census.gov/mtis/www/current.html"),
    ("20000520033743", "http://www.census.gov/mtis/www/current.html"),
    ("20000622191557", "http://www.census.gov/mtis/www/current.html"),
    ("20000815053030", "http://www.census.gov/mtis/www/current.html"),
    ("20001008090329", "http://www.census.gov/mtis/www/current.html"),
    ("20001206223700", "http://www.census.gov/mtis/www/current.html"),
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="retained index, table, workbook, PDF, and witness directory",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        help="override the package asset output directory",
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="fetch missing source bytes sequentially with retries",
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


def _read_or_fetch(
    path: Path, uri: str, *, fetch_missing: bool
) -> bytes:
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
        OfficialSourceFormat.XLS: "application/vnd.ms-excel",
        OfficialSourceFormat.XLSX: (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
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


def _index_local_name(uri: str) -> str:
    return {
        MTIS_INDEX_URI: "historic_releases.html",
        MTIS_DIRECTORY_URI: "directory.html",
        MTIS_BRIEFING_URI: "briefing.html",
    }[uri]


def main() -> None:
    args = _arguments()
    source_directory = args.source_directory.resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    captured_at_ns = time.time_ns()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)

    index_snapshots: dict[str, OfficialRawSnapshotV1] = {}
    for request in build_census_mtis_index_requests(
        registry, as_of_date=args.as_of
    ):
        content = _read_or_fetch(
            source_directory / _index_local_name(request.uri),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        index_snapshots[request.uri] = _snapshot(
            request, content, captured_at_ns
        )

    witnesses: dict[str, bytes] = {}
    for timestamp, original_uri in LEGACY_WITNESS_SPECS:
        uri = (
            f"https://web.archive.org/web/{timestamp}id_/"
            f"{original_uri}"
        )
        witnesses[uri] = _read_or_fetch(
            source_directory / f"mtis-wa-{timestamp}.html",
            uri,
            fetch_missing=args.fetch_missing,
        )
    release_index = parse_census_mtis_release_index(
        index_snapshots,
        witnesses,
        as_of_date=args.as_of,
    )

    release_snapshots: dict[str, OfficialRawSnapshotV1] = {}
    for request in build_census_mtis_archive_requests(
        registry, release_index
    ):
        content = _read_or_fetch(
            source_directory / _local_name(request.uri),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        release_snapshots[request.uri] = _snapshot(
            request, content, captured_at_ns
        )
    manifest = build_census_mtis_archive_manifest(
        registry,
        profile,
        release_index,
        release_snapshots,
    )

    asset_directory = (
        args.asset_directory.resolve()
        if args.asset_directory is not None
        else packaged_census_mtis_archive_manifest_path().parent
    )
    asset_directory.mkdir(parents=True, exist_ok=True)
    index_payload = {
        "schema_version": MTIS_INDEX_ENVELOPE_SCHEMA_VERSION,
        "official_snapshots": [
            {
                "snapshot": snapshot.to_dict(),
                "content_base64": base64.b64encode(
                    snapshot.content
                ).decode("ascii"),
            }
            for snapshot in index_snapshots.values()
        ],
        "legacy_witnesses": [
            {
                "uri": uri,
                "content_base64": base64.b64encode(content).decode("ascii"),
            }
            for uri, content in witnesses.items()
        ],
    }
    packaged_index = (
        asset_directory / packaged_census_mtis_index_path().name
    )
    packaged_manifest = (
        asset_directory / packaged_census_mtis_archive_manifest_path().name
    )
    packaged_index.write_text(
        canonical_contract_json(index_payload) + "\n", encoding="utf-8"
    )
    packaged_manifest.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "publications": len(manifest.publications),
                "measures": manifest.measure_occurrence_count,
                "changed_revisions": manifest.changed_revision_count,
                "artifacts": manifest.raw_artifact_count,
                "bytes": manifest.total_content_bytes,
                "corrected_entries": manifest.corrected_index_entry_count,
                "schedule_fallbacks": (
                    manifest.previous_schedule_metadata_count
                ),
                "disruptions": manifest.disruption_publication_count,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
