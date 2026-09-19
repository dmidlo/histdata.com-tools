#!/usr/bin/env python3
"""Refresh or replay the official ECB SPF archive for #539."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Final, cast
from urllib.parse import urlsplit

import requests

from histdatacom.market_context.ecb_spf_archive import (
    build_ecb_spf_archive_manifest,
    build_ecb_spf_round_requests,
    build_ecb_spf_support_requests,
    packaged_ecb_spf_manifest_path,
    replay_ecb_spf_archive,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-539 ECB SPF qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIRECTORY: Final = (
    REPO_ROOT / ".histdatacom" / "issue-539-ecb-spf" / "corpus"
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
        help="Operator-retained exact ECB response corpus.",
    )
    parser.add_argument(
        "--asset-path",
        type=Path,
        default=packaged_ecb_spf_manifest_path(),
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
    if role == "all-data":
        return root / "indexes" / "all-data.html"
    if role == "all-reports":
        return root / "indexes" / "all-reports.html"
    if role == "microdata":
        return root / "support" / "SPF_individual_forecasts.zip"
    if role == "description":
        return root / "support" / "SPF_dataset_description.pdf"
    if role == "round":
        name = cast(str, urlsplit(request.uri).path.rsplit("/", 1)[-1])
        return root / "rounds" / name
    raise ValueError(f"unsupported ECB SPF corpus role: {role}")


def _content_type(source_format: OfficialSourceFormat) -> str:
    return {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.ARCHIVE: "application/zip",
        OfficialSourceFormat.PDF: "application/pdf",
    }[source_format]


def _validate_content(
    request: OfficialSourceRequestV1,
    content: bytes,
    source: str | Path,
) -> None:
    if not content or len(content) > 64 * 1024 * 1024:
        raise ValueError(f"ECB SPF response size is invalid: {source}")
    if request.source_format is OfficialSourceFormat.HTML:
        if b"<html" not in content[:16_384].lower():
            raise ValueError(f"retained ECB SPF HTML differs: {source}")
    elif request.source_format is OfficialSourceFormat.ARCHIVE:
        if not content.startswith(b"PK"):
            raise ValueError(f"retained ECB SPF archive differs: {source}")
    elif (
        request.source_format is OfficialSourceFormat.PDF
        and not content.startswith(b"%PDF")
    ):
        raise ValueError(f"retained ECB SPF PDF differs: {source}")


def _fetch(request: OfficialSourceRequestV1) -> bytes:
    delay = 1.0
    for attempt in range(1, 7):
        try:
            response = requests.request(
                request.method.value,
                request.uri,
                headers={"User-Agent": USER_AGENT},
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
                f"ECB SPF response resolved outside its host: {response.url}"
            )
        expected = _content_type(request.source_format)
        reported = response.headers.get("Content-Type", "").split(";", 1)[0]
        if reported.casefold() != expected:
            raise ValueError(
                "ECB SPF response content type differs: "
                f"expected {expected}, received {reported or '<missing>'}"
            )
        _validate_content(request, content, request.uri)
        return content
    raise RuntimeError(f"ECB SPF request retry bound exhausted: {request.uri}")


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
                f"retained ECB SPF artifact is missing: {path}"
            )
        content = _fetch(request)
        _validate_content(request, content, request.uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        time.sleep(0.2)
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
    support_requests = build_ecb_spf_support_requests(registry)
    all_data_snapshot = _snapshot(
        root,
        support_requests[0],
        role="all-data",
        fetch_missing=args.fetch_missing,
    )
    all_reports_snapshot = _snapshot(
        root,
        support_requests[1],
        role="all-reports",
        fetch_missing=args.fetch_missing,
    )
    microdata_snapshot = _snapshot(
        root,
        support_requests[2],
        role="microdata",
        fetch_missing=args.fetch_missing,
    )
    description_snapshot = _snapshot(
        root,
        support_requests[3],
        role="description",
        fetch_missing=args.fetch_missing,
    )
    round_requests = build_ecb_spf_round_requests(registry, all_data_snapshot)
    round_snapshots = {
        request.uri: _snapshot(
            root,
            request,
            role="round",
            fetch_missing=args.fetch_missing,
        )
        for request in round_requests
    }
    manifest = build_ecb_spf_archive_manifest(
        registry,
        all_data_snapshot,
        all_reports_snapshot,
        microdata_snapshot,
        description_snapshot,
        round_snapshots,
        as_of_date=args.as_of,
    )
    replay_ecb_spf_archive(
        registry,
        manifest,
        all_data_snapshot,
        all_reports_snapshot,
        microdata_snapshot,
        description_snapshot,
        round_snapshots,
    )
    asset_path = args.asset_path.expanduser().resolve()
    asset_path.parent.mkdir(parents=True, exist_ok=True)
    asset_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        f"wrote {asset_path} ({manifest.raw_artifact_count} artifacts, "
        f"{manifest.round_count} rounds, {manifest.forecast_count} forecasts)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
