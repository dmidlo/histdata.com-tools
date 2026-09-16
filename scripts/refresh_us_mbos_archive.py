#!/usr/bin/env python3
"""Refresh or replay the complete Philadelphia Fed MBOS corpus."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from typing import Final, cast
from zipfile import ZipFile

import requests

from histdatacom.market_context import (
    MBOS_BULK_ARCHIVE_URI,
    MBOS_INDEX_URI,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    PhiladelphiaFedMbosIndexEntryV1,
    PhiladelphiaFedMbosOcrCorpusV1,
    PhiladelphiaFedMbosOcrEntryV1,
    build_philadelphia_fed_mbos_archive_manifest,
    build_philadelphia_fed_mbos_release_requests,
    build_philadelphia_fed_mbos_support_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    packaged_philadelphia_fed_mbos_index_path,
    packaged_philadelphia_fed_mbos_manifest_path,
    packaged_philadelphia_fed_mbos_ocr_path,
    parse_philadelphia_fed_mbos_release_index,
)

USER_AGENT: Final = (
    "histdata.com-tools issue-538 MBOS archive qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="caller-retained archive index, ZIP, PDFs, and report pages",
    )
    parser.add_argument(
        "--ocr-page-directory",
        type=Path,
        required=True,
        help="reviewed <YYYY-MM>/page-{1,2}.txt directory",
    )
    parser.add_argument(
        "--asset-directory",
        type=Path,
        default=DEFAULT_ASSET_DIRECTORY,
    )
    parser.add_argument(
        "--fetch-missing",
        action="store_true",
        help="fetch official artifacts missing from the retained corpus",
    )
    parser.add_argument(
        "--generate-ocr",
        action="store_true",
        help="generate missing OCR candidates for operator review",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        choices=range(1, 17),
        metavar="1..16",
    )
    return parser.parse_args()


def _fetch(uri: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            response = requests.get(
                uri,
                headers={"User-Agent": USER_AGENT},
                timeout=(15, 120),
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


def _local_path(
    source_directory: Path,
    request: OfficialSourceRequestV1,
    period_by_uri: dict[str, str],
) -> Path:
    if request.uri == MBOS_INDEX_URI:
        return source_directory / "archive-index.html"
    if request.uri == MBOS_BULK_ARCHIVE_URI:
        return source_directory / "MBOS-1968_2007.zip"
    period = period_by_uri[request.uri]
    suffix = (
        ".html"
        if request.source_format is OfficialSourceFormat.HTML
        else ".pdf"
    )
    folder = "pages" if suffix == ".html" else "releases"
    return source_directory / folder / f"{period}{suffix}"


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
        OfficialSourceFormat.ARCHIVE: "application/zip",
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


def _ocr_candidate(content: bytes, destination: Path) -> tuple[str, str]:
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mbos-ocr-") as temporary:
        root = Path(temporary)
        pdf = root / "report.pdf"
        pdf.write_bytes(content)
        subprocess.run(
            [
                "pdftoppm",
                "-f",
                "1",
                "-l",
                "2",
                "-r",
                "300",
                "-png",
                pdf.name,
                "page",
            ],
            cwd=root,
            check=True,
        )
        texts: list[str] = []
        for page in (1, 2):
            image = root / f"page-{page}.png"
            output = root / f"ocr-{page}"
            subprocess.run(
                ["tesseract", image.name, output.name, "--psm", "6"],
                cwd=root,
                check=True,
            )
            text = output.with_suffix(".txt").read_text(encoding="utf-8")
            (destination / f"page-{page}.txt").write_text(
                text, encoding="utf-8"
            )
            shutil.copy2(image, destination / f"page-{page}.png")
            texts.append(text)
    return texts[0], texts[1]


def _release_content(
    entry: PhiladelphiaFedMbosIndexEntryV1,
    snapshots: dict[str, OfficialRawSnapshotV1],
    members: dict[str, bytes],
) -> bytes:
    if entry.archive_member is None:
        return snapshots[entry.artifact_uri].content
    return members[entry.archive_member]


def _ocr_corpus(
    release_index: object,
    snapshots: dict[str, OfficialRawSnapshotV1],
    ocr_directory: Path,
    *,
    generate: bool,
) -> PhiladelphiaFedMbosOcrCorpusV1:
    from histdatacom.market_context import PhiladelphiaFedMbosReleaseIndexV1

    if not isinstance(release_index, PhiladelphiaFedMbosReleaseIndexV1):
        raise TypeError("MBOS OCR requires a v1 release index")
    with ZipFile(BytesIO(snapshots[MBOS_BULK_ARCHIVE_URI].content)) as archive:
        members = {
            name: archive.read(name)
            for name in archive.namelist()
            if name.lower().endswith(".pdf")
        }
    entries: list[PhiladelphiaFedMbosOcrEntryV1] = []
    for release in release_index.entries:
        if release.reference_period > "2001-12":
            continue
        content = _release_content(release, snapshots, members)
        root = ocr_directory / release.reference_period
        paths = (root / "page-1.txt", root / "page-2.txt")
        if not all(path.exists() for path in paths):
            if not generate:
                raise FileNotFoundError(
                    f"reviewed OCR is missing for {release.reference_period}; "
                    "use --generate-ocr to create a candidate"
                )
            page_texts = _ocr_candidate(content, root)
        else:
            page_texts = (
                paths[0].read_text(encoding="utf-8"),
                paths[1].read_text(encoding="utf-8"),
            )
        entries.append(
            PhiladelphiaFedMbosOcrEntryV1(
                source_locator=release.source_locator,
                content_sha256=hashlib.sha256(content).hexdigest(),
                page_texts=page_texts,
            )
        )
    version = (
        subprocess.run(
            ["tesseract", "--version"],
            check=True,
            capture_output=True,
            text=True,
        )
        .stdout.splitlines()[0]
        .removeprefix("tesseract ")
    )
    return PhiladelphiaFedMbosOcrCorpusV1(
        engine="tesseract",
        engine_version=version,
        render_dpi=300,
        page_modes=(6, 6),
        entries=tuple(sorted(entries, key=lambda item: item.source_locator)),
    )


def main() -> int:
    args = _arguments()
    source_directory = args.source_directory.expanduser().resolve()
    ocr_directory = args.ocr_page_directory.expanduser().resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    ocr_directory.mkdir(parents=True, exist_ok=True)
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    captured_at_ns = time.time_ns()

    support_requests = build_philadelphia_fed_mbos_support_requests(
        registry, as_of_date=args.as_of
    )
    support_snapshots = tuple(
        _snapshot(
            request,
            _read_or_fetch(
                _local_path(source_directory, request, {}),
                request.uri,
                fetch_missing=args.fetch_missing,
            ),
            captured_at_ns,
        )
        for request in support_requests
    )
    snapshots = {item.request.uri: item for item in support_snapshots}
    release_index = parse_philadelphia_fed_mbos_release_index(
        snapshots[MBOS_INDEX_URI],
        snapshots[MBOS_BULK_ARCHIVE_URI],
        as_of_date=args.as_of,
    )
    period_by_uri = {
        uri: entry.reference_period
        for entry in release_index.entries
        for uri in (entry.artifact_uri, entry.release_page_uri)
        if uri is not None and uri != MBOS_BULK_ARCHIVE_URI
    }
    release_requests = build_philadelphia_fed_mbos_release_requests(
        registry, release_index
    )

    def capture(request: OfficialSourceRequestV1) -> OfficialRawSnapshotV1:
        path = _local_path(source_directory, request, period_by_uri)
        return _snapshot(
            request,
            _read_or_fetch(path, request.uri, fetch_missing=args.fetch_missing),
            captured_at_ns,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(capture, release_requests))
    snapshots.update({item.request.uri: item for item in release_snapshots})
    ocr_corpus = _ocr_corpus(
        release_index,
        snapshots,
        ocr_directory,
        generate=args.generate_ocr,
    )
    manifest = build_philadelphia_fed_mbos_archive_manifest(
        registry, profile, release_index, snapshots, ocr_corpus
    )

    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = (
        asset_directory / packaged_philadelphia_fed_mbos_manifest_path().name
    )
    index_path = (
        asset_directory / packaged_philadelphia_fed_mbos_index_path().name
    )
    ocr_path = asset_directory / packaged_philadelphia_fed_mbos_ocr_path().name
    manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    index_path.write_text(
        base64.b64encode(snapshots[MBOS_INDEX_URI].content).decode("ascii")
        + "\n",
        encoding="ascii",
    )
    ocr_path.write_text(ocr_corpus.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "as_of_date": manifest.release_index.as_of_date,
                "publication_count": len(manifest.publications),
                "exact_minute_count": manifest.exact_minute_count,
                "inferred_bounded_count": manifest.inferred_bounded_count,
                "forecast_occurrence_count": manifest.forecast_occurrence_count,
                "revision_record_count": manifest.revision_record_count,
                "revision_publication_count": manifest.revision_publication_count,
                "ocr_publication_count": manifest.ocr_publication_count,
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "source_directory": str(source_directory),
                "ocr_page_directory": str(ocr_directory),
                "manifest_path": str(manifest_path),
                "index_path": str(index_path),
                "ocr_path": str(ocr_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
