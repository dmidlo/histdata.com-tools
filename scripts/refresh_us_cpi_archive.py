"""Refresh the compact real-artifact BLS CPI manifest for issue #538.

The command requires an explicit as-of date.  The package receives the exact
official release index and compact content-addressed manifest; the selected
text, HTML, and PDF release corpus remains in an optional caller-owned raw
directory.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final

import requests

from histdatacom.market_context import (
    BlsCpiArchiveManifestV1,
    BlsCpiReleaseIndexV1,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_bls_cpi_archive_manifest,
    build_bls_cpi_archive_requests,
    build_bls_cpi_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    parse_bls_cpi_release_index,
)

USER_AGENT: Final = "histdatacom-tools/2.5 contact=david@histdata.com"
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
    response = requests.get(
        request.uri,
        timeout=(10.0, 60.0),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    if not response.content:
        raise ValueError(f"official CPI response is empty: {request.uri}")
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


def _prefer_retained_index_evidence(
    asset_directory: Path,
    request: OfficialSourceRequestV1,
    observed_snapshot: OfficialRawSnapshotV1,
    observed_index: BlsCpiReleaseIndexV1,
) -> tuple[OfficialRawSnapshotV1, BlsCpiReleaseIndexV1]:
    """Avoid presentation-only index churn when its inventory is unchanged."""
    manifest_path = asset_directory / "us_cpi_archive_v1.json"
    index_path = asset_directory / "us_cpi_release_index_v1.html.b64"
    if not manifest_path.exists() and not index_path.exists():
        return observed_snapshot, observed_index
    if not manifest_path.is_file() or not index_path.is_file():
        raise ValueError("retained CPI index evidence is incomplete")
    try:
        manifest = BlsCpiArchiveManifestV1.from_json(
            manifest_path.read_text(encoding="utf-8")
        )
        content = base64.b64decode(
            index_path.read_text(encoding="ascii").strip(), validate=True
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError("retained CPI index evidence is invalid") from exc
    retained = manifest.release_index
    if (
        len(content) != retained.content_length
        or hashlib.sha256(content).hexdigest() != retained.content_sha256
    ):
        raise ValueError("retained CPI index bytes differ from the manifest")
    retained_snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=observed_snapshot.retrieved_at_ns,
        completed_at_ns=observed_snapshot.completed_at_ns,
        status_code=observed_snapshot.status_code,
        resolved_uri=observed_snapshot.resolved_uri,
        response_headers=observed_snapshot.response_headers,
        content=content,
        content_type=observed_snapshot.content_type,
    )
    retained_as_observed = parse_bls_cpi_release_index(
        retained_snapshot, as_of_date=observed_index.as_of_date
    )
    if (
        retained_as_observed.releases == observed_index.releases
        and retained_as_observed.unavailable_reference_periods
        == observed_index.unavailable_reference_periods
    ):
        return retained_snapshot, retained_as_observed
    return observed_snapshot, observed_index


def _retain_raw(
    raw_directory: Path,
    index_snapshot: OfficialRawSnapshotV1,
    release_snapshots: tuple[OfficialRawSnapshotV1, ...],
) -> None:
    root = raw_directory.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    for snapshot in (index_snapshot, *release_snapshots):
        suffix = {
            OfficialSourceFormat.HTML: ".html",
            OfficialSourceFormat.TEXT: ".txt",
            OfficialSourceFormat.PDF: ".pdf",
        }[snapshot.request.source_format]
        path = root / f"{snapshot.content_sha256}{suffix}"
        if path.exists():
            if path.read_bytes() != snapshot.content:
                raise ValueError(f"retained raw artifact differs: {path.name}")
            continue
        path.write_bytes(snapshot.content)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    asset_directory = args.asset_directory.expanduser().resolve()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    index_request = build_bls_cpi_index_request(registry, as_of_date=args.as_of)
    observed_index_snapshot = _snapshot(index_request)
    observed_index = parse_bls_cpi_release_index(
        observed_index_snapshot, as_of_date=args.as_of
    )
    index_snapshot, release_index = _prefer_retained_index_evidence(
        asset_directory,
        index_request,
        observed_index_snapshot,
        observed_index,
    )
    requests_to_fetch = build_bls_cpi_archive_requests(registry, release_index)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(_snapshot, requests_to_fetch))
    manifest = build_bls_cpi_archive_manifest(
        profile, release_index, release_snapshots
    )

    asset_directory.mkdir(parents=True, exist_ok=True)
    index_path = asset_directory / "us_cpi_release_index_v1.html.b64"
    index_path.write_text(
        base64.b64encode(index_snapshot.content).decode("ascii") + "\n",
        encoding="ascii",
    )
    manifest_path = asset_directory / "us_cpi_archive_v1.json"
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    if args.raw_directory is not None:
        _retain_raw(
            args.raw_directory, observed_index_snapshot, release_snapshots
        )

    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": release_index.as_of_date,
                "release_count": len(manifest.entries),
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "revision_occurrence_count": (
                    manifest.revision_occurrence_count
                ),
                "noncomparable_revision_count": (
                    manifest.noncomparable_revision_count
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
