"""Refresh the complete real-artifact BEA PIO archive manifest.

The command requires an explicit as-of date.  The package receives the exact
multi-page official archive inventory and a compact content-addressed manifest;
the release corpus remains in an optional operator-owned directory.
"""

from __future__ import annotations

import argparse
import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

import requests

from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_personal_income_outlays_archive import (
    BEA_PIO_INDEX_ENVELOPE_SCHEMA_VERSION,
    BeaPioReleaseIndexV1,
    build_bea_pio_archive_manifest,
    build_bea_pio_archive_requests,
    build_bea_pio_index_requests,
    parse_bea_pio_release_index,
)

USER_AGENT: Final = "histdatacom-tools/2.5 contact=david@histdata.com"
MAX_ATTEMPTS: Final = 5
MAX_RETRY_SECONDS: Final = 15.0
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        required=True,
        help="Explicit YYYY-MM-DD archive observation boundary.",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        default=DEFAULT_ASSET_DIRECTORY,
        help="Directory for the generated packaged index and manifest.",
    )
    parser.add_argument(
        "--raw-directory",
        type=Path,
        help="Optional external directory for content-addressed raw bytes.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        help=(
            "Optional retained corpus containing archive-page-N.html, "
            "release-YYYY-MM-DD-slug.html and support files by URI basename."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        choices=range(1, 13),
        metavar="1..12",
    )
    return parser


def _snapshot(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
    if not isinstance(request, OfficialSourceRequestV1):
        raise TypeError("refresh request is invalid")
    started = time.time_ns()
    response: requests.Response | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            candidate = requests.get(
                request.uri,
                timeout=(10.0, 60.0),
                headers={"User-Agent": USER_AGENT},
            )
        except requests.RequestException:
            if attempt + 1 == MAX_ATTEMPTS:
                raise
            time.sleep(min(2.0**attempt, MAX_RETRY_SECONDS))
            continue
        if candidate.status_code not in {429, 500, 502, 503, 504}:
            response = candidate
            break
        if attempt + 1 == MAX_ATTEMPTS:
            candidate.raise_for_status()
        retry_after = candidate.headers.get("Retry-After", "").strip()
        try:
            delay = float(retry_after)
        except ValueError:
            delay = 2.0**attempt
        time.sleep(max(0.25, min(delay, MAX_RETRY_SECONDS)))
    if response is None:
        raise RuntimeError(f"official BEA PIO request exhausted: {request.uri}")
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"official BEA PIO response is empty: {request.uri}")
    completed = max(started, time.time_ns())
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=started,
        completed_at_ns=completed,
        status_code=response.status_code,
        resolved_uri=response.url,
        response_headers={
            "Content-Type": response.headers.get("Content-Type", "")
        },
        content=response.content,
        content_type=content_type,
    )


def _retained_filename(request: OfficialSourceRequestV1) -> str:
    if request.page_number is not None:
        return f"archive-page-{request.page_number}.html"
    if request.source_format is not OfficialSourceFormat.HTML:
        return Path(urlparse(request.uri).path).name
    if request.window_start is None:
        raise ValueError("retained PIO release request has no date")
    return f"release-{request.window_start}-{Path(urlparse(request.uri).path).name}.html"


def _retained_snapshot(
    request: OfficialSourceRequestV1,
    source_directory: Path,
) -> OfficialRawSnapshotV1:
    if not isinstance(request, OfficialSourceRequestV1):
        raise TypeError("retained refresh request is invalid")
    path = source_directory.expanduser().resolve() / _retained_filename(request)
    if not path.is_file():
        raise ValueError(
            f"retained BEA PIO artifact is unavailable: {path.name}"
        )
    content = path.read_bytes()
    if not content:
        raise ValueError(f"retained BEA PIO artifact is empty: {path.name}")
    content_type = {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
        OfficialSourceFormat.TEXT: "text/plain",
        OfficialSourceFormat.XLSX: (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    }[request.source_format]
    observed = time.time_ns()
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=observed,
        completed_at_ns=observed,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def _prefer_retained_index_evidence(
    asset_directory: Path,
    observed_snapshots: tuple[OfficialRawSnapshotV1, ...],
    observed_index: BeaPioReleaseIndexV1,
) -> tuple[tuple[OfficialRawSnapshotV1, ...], BeaPioReleaseIndexV1]:
    path = (
        asset_directory.expanduser().resolve() / "us_pio_release_index_v1.json"
    )
    if not path.is_file():
        return observed_snapshots, observed_index
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        pages = envelope["pages"]
        if len(pages) != len(observed_snapshots):
            return observed_snapshots, observed_index
        retained = tuple(
            OfficialRawSnapshotV1(
                request=observed.request,
                retrieved_at_ns=observed.retrieved_at_ns,
                completed_at_ns=observed.completed_at_ns,
                status_code=observed.status_code,
                resolved_uri=observed.resolved_uri,
                response_headers=observed.response_headers,
                content=base64.b64decode(
                    pages[position]["content_base64"], validate=True
                ),
                content_type=observed.content_type,
            )
            for position, observed in enumerate(observed_snapshots)
        )
        retained_index = parse_bea_pio_release_index(
            retained, as_of_date=observed_index.as_of_date
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return observed_snapshots, observed_index
    if retained_index.releases == observed_index.releases:
        return retained, retained_index
    return observed_snapshots, observed_index


def _retain_raw(
    raw_directory: Path,
    snapshots: tuple[OfficialRawSnapshotV1, ...],
) -> None:
    root = raw_directory.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    for snapshot in snapshots:
        suffix = {
            OfficialSourceFormat.HTML: ".html",
            OfficialSourceFormat.PDF: ".pdf",
            OfficialSourceFormat.TEXT: ".txt",
            OfficialSourceFormat.XLSX: ".xlsx",
        }[snapshot.request.source_format]
        path = root / f"{snapshot.content_sha256}{suffix}"
        if path.exists():
            if path.read_bytes() != snapshot.content:
                raise ValueError(f"retained raw artifact differs: {path.name}")
            continue
        path.write_bytes(snapshot.content)


def _index_envelope(
    snapshots: tuple[OfficialRawSnapshotV1, ...], as_of: str
) -> dict[str, object]:
    captured_at_ns = int(
        datetime.fromisoformat(f"{as_of}T00:00:00+00:00").timestamp()
        * 1_000_000_000
    )
    return {
        "schema_version": BEA_PIO_INDEX_ENVELOPE_SCHEMA_VERSION,
        "captured_at_ns": captured_at_ns,
        "pages": [
            {
                "page_number": snapshot.request.page_number,
                "uri": snapshot.request.uri,
                "content_base64": base64.b64encode(snapshot.content).decode(
                    "ascii"
                ),
            }
            for snapshot in snapshots
        ],
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    asset_directory = args.asset_directory.expanduser().resolve()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    loader = (
        _snapshot
        if args.source_directory is None
        else lambda request: _retained_snapshot(request, args.source_directory)
    )
    index_requests = build_bea_pio_index_requests(registry)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        observed_index_snapshots = tuple(pool.map(loader, index_requests))
    observed_index = parse_bea_pio_release_index(
        observed_index_snapshots, as_of_date=args.as_of
    )
    index_snapshots, release_index = _prefer_retained_index_evidence(
        asset_directory, observed_index_snapshots, observed_index
    )
    release_requests = build_bea_pio_archive_requests(registry, release_index)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(loader, release_requests))
    snapshots_by_uri = {
        snapshot.request.uri: snapshot for snapshot in release_snapshots
    }
    manifest = build_bea_pio_archive_manifest(
        registry, profile, release_index, snapshots_by_uri
    )

    asset_directory.mkdir(parents=True, exist_ok=True)
    index_path = asset_directory / "us_pio_release_index_v1.json"
    index_path.write_text(
        json.dumps(
            _index_envelope(index_snapshots, args.as_of),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path = asset_directory / "us_pio_archive_v1.json"
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    if args.raw_directory is not None:
        _retain_raw(
            args.raw_directory,
            (*observed_index_snapshots, *release_snapshots),
        )

    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": release_index.as_of_date,
                "publication_count": len(manifest.publications),
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "initial_publication_count": (
                    manifest.initial_publication_count
                ),
                "revision_publication_count": (
                    manifest.revision_publication_count
                ),
                "measure_occurrence_count": manifest.measure_occurrence_count,
                "comparable_revision_count": (
                    manifest.comparable_revision_count
                ),
                "derived_price_occurrence_count": (
                    manifest.derived_price_occurrence_count
                ),
                "historical_update_notice_count": (
                    manifest.historical_update_notice_count
                ),
                "shutdown_publication_count": (
                    manifest.shutdown_publication_count
                ),
                "support_artifact_count": manifest.support_artifact_count,
                "exact_minute_count": manifest.exact_minute_count,
                "date_only_count": manifest.date_only_count,
                "zone_mismatch_count": manifest.zone_mismatch_count,
                "archive_date_mismatch_count": (
                    manifest.archive_date_mismatch_count
                ),
                "index_path": str(index_path),
                "manifest_path": str(manifest_path),
                "raw_directory": (
                    None
                    if args.raw_directory is None
                    else str(args.raw_directory.expanduser().resolve())
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
