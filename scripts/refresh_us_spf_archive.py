"""Refresh the complete Philadelphia Fed SPF archive for issue #538."""

from __future__ import annotations

import argparse
import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final

import requests

from histdatacom.market_context import (
    OfficialRawSnapshotV1,
    OfficialSourceRequestV1,
    build_philadelphia_fed_spf_archive_manifest,
    build_philadelphia_fed_spf_release_requests,
    build_philadelphia_fed_spf_support_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    parse_philadelphia_fed_spf_release_index,
)

USER_AGENT: Final = "histdata.com-tools issue-538 SPF archive refresh/1"
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of",
        required=True,
        help="Explicit YYYY-MM-DD observation boundary.",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        default=DEFAULT_ASSET_DIRECTORY,
        help="Directory for the compact packaged manifest and ledger.",
    )
    parser.add_argument(
        "--raw-directory",
        type=Path,
        help="Optional caller-owned content-addressed raw corpus directory.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        choices=range(1, 17),
        metavar="1..16",
    )
    return parser


def _snapshot(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
    started = time.time_ns()
    response = requests.get(
        request.uri,
        timeout=(10.0, 90.0),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    completed = max(started, time.time_ns())
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
    if not content_type:
        content_type = "application/octet-stream"
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


def _suffix(snapshot: OfficialRawSnapshotV1) -> str:
    value = snapshot.request.source_format.value
    return ".txt" if value == "text" else f".{value}"


def _retain_raw(
    raw_directory: Path,
    snapshots: tuple[OfficialRawSnapshotV1, ...],
) -> None:
    root = raw_directory.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    for snapshot in snapshots:
        path = root / f"{snapshot.content_sha256}{_suffix(snapshot)}"
        if path.exists():
            if path.read_bytes() != snapshot.content:
                raise ValueError(f"retained raw artifact differs: {path.name}")
            continue
        path.write_bytes(snapshot.content)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    support_requests = build_philadelphia_fed_spf_support_requests(
        registry, as_of_date=args.as_of
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        support_snapshots = tuple(pool.map(_snapshot, support_requests))
    support = {item.request.uri: item for item in support_snapshots}
    release_index = parse_philadelphia_fed_spf_release_index(
        support[support_requests[0].uri],
        support[support_requests[2].uri],
        as_of_date=args.as_of,
    )
    release_requests = build_philadelphia_fed_spf_release_requests(
        registry, release_index
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(_snapshot, release_requests))
    snapshots = (*support_snapshots, *release_snapshots)
    manifest = build_philadelphia_fed_spf_archive_manifest(
        registry,
        profile,
        release_index,
        {item.request.uri: item for item in snapshots},
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = asset_directory / "us_spf_archive_v1.json"
    ledger_path = asset_directory / "us_spf_release_dates_v1.txt.b64"
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    ledger_path.write_text(
        base64.b64encode(support_snapshots[2].content).decode("ascii") + "\n",
        encoding="ascii",
    )
    if args.raw_directory is not None:
        _retain_raw(args.raw_directory, snapshots)

    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": manifest.release_index.as_of_date,
                "publication_count": len(manifest.publications),
                "forecast_occurrence_count": (
                    manifest.forecast_occurrence_count
                ),
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "manifest_path": str(manifest_path),
                "ledger_path": str(ledger_path),
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
