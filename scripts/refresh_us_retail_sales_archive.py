"""Refresh the complete real-artifact Census MARTS archive manifest.

The command requires an explicit as-of date.  Exact archive HTML, reviewed
page-aware OCR, and a compact content-addressed manifest are packaged; the
large PDF/XLS/XLSX corpus remains in an optional operator-owned directory.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

import requests
from pypdf import PdfReader

from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_retail_sales_archive import (
    CENSUS_RETAIL_BENCHMARK_INDEX_ENVELOPE_SCHEMA_VERSION,
    CENSUS_RETAIL_BENCHMARK_URIS,
    CENSUS_RETAIL_INDEX_ENVELOPE_SCHEMA_VERSION,
    CensusRetailOcrCorpusV1,
    CensusRetailOcrEntryV1,
    build_census_retail_archive_manifest,
    build_census_retail_archive_requests,
    build_census_retail_benchmark_index_request,
    build_census_retail_benchmark_requests,
    build_census_retail_index_request,
    parse_census_retail_benchmark_index,
    parse_census_retail_release_index,
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
        help="Directory for generated index, OCR, and manifest assets.",
    )
    parser.add_argument(
        "--raw-directory",
        type=Path,
        help="Optional external directory for content-addressed raw bytes.",
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        help="Optional retained corpus containing official URI basenames.",
    )
    parser.add_argument(
        "--benchmark-source-directory",
        type=Path,
        help="Optional retained annual-index and benchmark artifact corpus.",
    )
    parser.add_argument(
        "--ocr-page-directory",
        type=Path,
        help="Optional reviewed <PDF stem>/page-{1,2,3}.txt evidence.",
    )
    parser.add_argument(
        "--refresh-ocr",
        action="store_true",
        help="Regenerate OCR even when matching packaged evidence exists.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
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
                timeout=(10.0, 90.0),
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
        raise RuntimeError(
            f"official Census retail request exhausted: {request.uri}"
        )
    response.raise_for_status()
    if not response.content:
        raise ValueError(
            f"official Census retail response is empty: {request.uri}"
        )
    content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=started,
        completed_at_ns=max(started, time.time_ns()),
        status_code=response.status_code,
        resolved_uri=response.url,
        response_headers={
            "Content-Type": response.headers.get("Content-Type", "")
        },
        content=response.content,
        content_type=content_type,
    )


def _retained_filename(request: OfficialSourceRequestV1) -> str:
    if request.source_format is OfficialSourceFormat.HTML:
        return "historic_releases.html"
    for report_year, uri in CENSUS_RETAIL_BENCHMARK_URIS.items():
        if request.uri == uri:
            return f"benchmark-{report_year}{Path(urlparse(uri).path).suffix}"
    return Path(urlparse(request.uri).path).name


def _content_type(source_format: OfficialSourceFormat) -> str:
    return {
        OfficialSourceFormat.HTML: "text/html",
        OfficialSourceFormat.PDF: "application/pdf",
        OfficialSourceFormat.XLS: "application/vnd.ms-excel",
        OfficialSourceFormat.XLSX: (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    }[source_format]


def _retained_snapshot(
    request: OfficialSourceRequestV1,
    source_directory: Path,
) -> OfficialRawSnapshotV1:
    if not isinstance(request, OfficialSourceRequestV1):
        raise TypeError("retained refresh request is invalid")
    path = source_directory.expanduser().resolve() / _retained_filename(request)
    if not path.is_file():
        raise ValueError(
            f"retained Census retail artifact is unavailable: {path.name}"
        )
    content = path.read_bytes()
    if not content:
        raise ValueError(
            f"retained Census retail artifact is empty: {path.name}"
        )
    observed = time.time_ns()
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=observed,
        completed_at_ns=observed,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": _content_type(request.source_format)},
        content=content,
        content_type=_content_type(request.source_format),
    )


def _native_pdf_text(content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content))
        return "\f".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError("retained Census retail PDF is unreadable") from exc


def _tesseract_version() -> str:
    completed = subprocess.run(
        ["tesseract", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    first_line = completed.stdout.splitlines()[0].strip()
    return first_line.removeprefix("tesseract ")


def _generate_page_texts(content: bytes) -> tuple[str, str, str]:
    with tempfile.TemporaryDirectory(
        prefix="histdatacom-retail-ocr-"
    ) as raw_root:
        root = Path(raw_root)
        pdf_path = root / "release.pdf"
        pdf_path.write_bytes(content)
        subprocess.run(
            [
                "pdftoppm",
                "-f",
                "1",
                "-l",
                "3",
                "-r",
                "300",
                "-gray",
                "-png",
                str(pdf_path),
                str(root / "page"),
            ],
            check=True,
            capture_output=True,
        )
        pages: list[str] = []
        for page_number, page_mode in ((1, 3), (2, 6), (3, 6)):
            image_path = root / f"page-{page_number}.png"
            command = [
                "tesseract",
                str(image_path),
                "stdout",
                "-l",
                "eng",
                "--psm",
                str(page_mode),
            ]
            if page_number > 1:
                command.extend(["-c", "preserve_interword_spaces=1"])
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
            text = completed.stdout.strip()
            if not text:
                raise ValueError(
                    "Tesseract returned an empty Census retail page"
                )
            pages.append(text)
        return pages[0], pages[1], pages[2]


def _reviewed_page_texts(
    root: Path,
    artifact_uri: str,
) -> tuple[str, str, str]:
    directory = (
        root.expanduser().resolve() / Path(urlparse(artifact_uri).path).stem
    )
    pages = tuple(
        (directory / f"page-{page_number}.txt")
        .read_text(encoding="utf-8")
        .strip()
        for page_number in (1, 2, 3)
    )
    if any(not item for item in pages):
        raise ValueError(
            f"reviewed Census retail OCR is empty: {directory.name}"
        )
    return pages[0], pages[1], pages[2]


def _matching_packaged_ocr(
    asset_directory: Path,
    image_snapshots: tuple[OfficialRawSnapshotV1, ...],
) -> CensusRetailOcrCorpusV1 | None:
    path = asset_directory / "us_retail_sales_ocr_v1.json"
    if not path.is_file():
        return None
    try:
        corpus = CensusRetailOcrCorpusV1.from_json(
            path.read_text(encoding="utf-8")
        )
    except (OSError, TypeError, ValueError):
        return None
    observed = {
        item.request.uri: item.content_sha256 for item in image_snapshots
    }
    retained = {
        item.artifact_uri: item.content_sha256 for item in corpus.entries
    }
    return corpus if retained == observed else None


def _build_ocr_corpus(
    pdf_snapshots: tuple[OfficialRawSnapshotV1, ...],
    *,
    asset_directory: Path,
    ocr_page_directory: Path | None,
    refresh_ocr: bool,
) -> CensusRetailOcrCorpusV1:
    image_snapshots = tuple(
        item
        for item in pdf_snapshots
        if len(_native_pdf_text(item.content).strip()) < 1000
    )
    if not refresh_ocr:
        retained = _matching_packaged_ocr(asset_directory, image_snapshots)
        if retained is not None:
            return retained
    version = _tesseract_version()
    entries: list[CensusRetailOcrEntryV1] = []
    for snapshot in sorted(image_snapshots, key=lambda item: item.request.uri):
        pages = (
            _generate_page_texts(snapshot.content)
            if ocr_page_directory is None
            else _reviewed_page_texts(ocr_page_directory, snapshot.request.uri)
        )
        entries.append(
            CensusRetailOcrEntryV1(
                artifact_uri=snapshot.request.uri,
                content_sha256=snapshot.content_sha256,
                page_texts=pages,
            )
        )
    return CensusRetailOcrCorpusV1(
        engine="tesseract",
        engine_version=version,
        render_dpi=300,
        page_modes=(3, 6, 6),
        entries=tuple(entries),
    )


def _retain_raw(
    raw_directory: Path,
    snapshots: tuple[OfficialRawSnapshotV1, ...],
) -> None:
    root = raw_directory.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    suffixes = {
        OfficialSourceFormat.HTML: ".html",
        OfficialSourceFormat.PDF: ".pdf",
        OfficialSourceFormat.XLS: ".xls",
        OfficialSourceFormat.XLSX: ".xlsx",
    }
    for snapshot in snapshots:
        path = (
            root
            / f"{snapshot.content_sha256}{suffixes[snapshot.request.source_format]}"
        )
        if path.exists():
            if path.read_bytes() != snapshot.content:
                raise ValueError(f"retained raw artifact differs: {path.name}")
            continue
        path.write_bytes(snapshot.content)


def _index_envelope(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of: str,
) -> dict[str, object]:
    captured_at_ns = int(
        datetime.fromisoformat(f"{as_of}T00:00:00+00:00").timestamp()
        * 1_000_000_000
    )
    return {
        "schema_version": CENSUS_RETAIL_INDEX_ENVELOPE_SCHEMA_VERSION,
        "as_of_date": as_of,
        "captured_at_ns": captured_at_ns,
        "uri": snapshot.request.uri,
        "content_sha256": snapshot.content_sha256,
        "content_base64": base64.b64encode(snapshot.content).decode("ascii"),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    asset_directory = args.asset_directory.expanduser().resolve()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    release_loader = (
        _snapshot
        if args.source_directory is None
        else lambda request: _retained_snapshot(request, args.source_directory)
    )
    benchmark_loader = (
        _snapshot
        if args.benchmark_source_directory is None
        else lambda request: _retained_snapshot(
            request, args.benchmark_source_directory
        )
    )
    index_request = build_census_retail_index_request(
        registry, as_of_date=args.as_of
    )
    index_snapshot = release_loader(index_request)
    release_index = parse_census_retail_release_index(
        index_snapshot, as_of_date=args.as_of
    )
    benchmark_index_request = build_census_retail_benchmark_index_request(
        registry, as_of_date=args.as_of
    )
    benchmark_index_snapshot = benchmark_loader(benchmark_index_request)
    benchmark_requests = build_census_retail_benchmark_requests(
        registry, benchmark_index_snapshot
    )
    release_requests = build_census_retail_archive_requests(
        registry, release_index
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        release_snapshots = tuple(pool.map(release_loader, release_requests))
        benchmark_snapshots = tuple(
            pool.map(benchmark_loader, benchmark_requests)
        )
    snapshots_by_uri = {
        snapshot.request.uri: snapshot for snapshot in release_snapshots
    }
    benchmark_snapshots_by_uri = {
        snapshot.request.uri: snapshot for snapshot in benchmark_snapshots
    }
    benchmark_index = parse_census_retail_benchmark_index(
        registry,
        benchmark_index_snapshot,
        benchmark_snapshots_by_uri,
        as_of_date=args.as_of,
    )
    pdf_snapshots = tuple(
        item
        for item in release_snapshots
        if item.request.source_format is OfficialSourceFormat.PDF
    )
    ocr_corpus = _build_ocr_corpus(
        pdf_snapshots,
        asset_directory=asset_directory,
        ocr_page_directory=args.ocr_page_directory,
        refresh_ocr=args.refresh_ocr,
    )
    manifest = build_census_retail_archive_manifest(
        registry,
        profile,
        release_index,
        benchmark_index,
        snapshots_by_uri,
        ocr_corpus,
    )

    asset_directory.mkdir(parents=True, exist_ok=True)
    (asset_directory / "us_retail_sales_release_index_v1.json").write_text(
        json.dumps(
            _index_envelope(index_snapshot, as_of=args.as_of),
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    (asset_directory / "us_retail_sales_benchmark_index_v1.json").write_text(
        json.dumps(
            {
                **_index_envelope(benchmark_index_snapshot, as_of=args.as_of),
                "schema_version": (
                    CENSUS_RETAIL_BENCHMARK_INDEX_ENVELOPE_SCHEMA_VERSION
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    (asset_directory / "us_retail_sales_ocr_v1.json").write_text(
        ocr_corpus.to_json() + "\n", encoding="utf-8"
    )
    (asset_directory / "us_retail_sales_archive_v1.json").write_text(
        manifest.to_json() + "\n", encoding="utf-8"
    )
    if args.raw_directory is not None:
        _retain_raw(
            args.raw_directory,
            (
                index_snapshot,
                benchmark_index_snapshot,
                *release_snapshots,
                *benchmark_snapshots,
            ),
        )

    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "ocr_corpus_id": manifest.ocr_corpus_id,
                "as_of_date": release_index.as_of_date,
                "publication_count": len(manifest.publications),
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
                "measure_occurrence_count": manifest.measure_occurrence_count,
                "comparable_revision_count": manifest.comparable_revision_count,
                "derived_aggregate_occurrence_count": (
                    manifest.derived_aggregate_occurrence_count
                ),
                "support_artifact_count": manifest.support_artifact_count,
                "corrected_support_link_count": (
                    manifest.corrected_support_link_count
                ),
                "ocr_publication_count": manifest.ocr_publication_count,
                "benchmark_revision_notice_count": (
                    manifest.benchmark_revision_notice_count
                ),
                "benchmark_state_override_count": (
                    manifest.benchmark_state_override_count
                ),
                "shutdown_delayed_count": manifest.shutdown_delayed_count,
                "reported_rate_cross_check_count": (
                    manifest.reported_rate_cross_check_count
                ),
                "exact_minute_count": manifest.exact_minute_count,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
