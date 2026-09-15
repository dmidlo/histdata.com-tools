#!/usr/bin/env python3
"""Refresh or replay the complete Census Advance Durable Goods corpus."""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import requests

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_durable_goods_archive import (
    DURABLE_INDEX_ENVELOPE_SCHEMA_VERSION,
    DURABLE_INDEX_URI,
    DURABLE_JULY_2005_ARTIFACT_URI,
    CensusDurableOcrCorpusV1,
    CensusDurableOcrEntryV1,
    build_census_durable_archive_manifest,
    build_census_durable_archive_requests,
    build_census_durable_index_request,
    packaged_census_durable_archive_manifest_path,
    packaged_census_durable_index_path,
    packaged_census_durable_ocr_path,
    parse_census_durable_release_index,
)

USER_AGENT = (
    "histdata.com-tools durable-goods archive qualification "
    "(contact: repository maintainers)"
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument(
        "--source-directory",
        type=Path,
        required=True,
        help="retained index and PDF directory",
    )
    parser.add_argument(
        "--ocr-page-directory",
        type=Path,
        required=True,
        help="reviewed <stem>/page-{1,2}.txt directory",
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
    parser.add_argument(
        "--generate-ocr",
        action="store_true",
        help="generate missing reviewed OCR candidates with pdftoppm/tesseract",
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
    request: object, content: bytes, captured_at_ns: int
) -> OfficialRawSnapshotV1:
    from histdatacom.market_context.official_sources import (
        OfficialSourceRequestV1,
    )

    if not isinstance(request, OfficialSourceRequestV1):
        raise TypeError("refresh request is not a v1 official request")
    content_type = (
        "text/html" if request.uri == DURABLE_INDEX_URI else "application/pdf"
    )
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


def _local_pdf_name(uri: str) -> str:
    if uri == DURABLE_JULY_2005_ARTIFACT_URI:
        return "jul05adv.pdf"
    return Path(urlparse(uri).path).name


def _ocr_candidate(pdf: Path, destination: Path) -> tuple[str, str]:
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="durable-ocr-") as temporary:
        root = Path(temporary)
        output_prefix = root / "page"
        subprocess.run(
            [
                "pdftoppm",
                "-f",
                "1",
                "-l",
                "2",
                "-r",
                "300",
                "-gray",
                "-png",
                str(pdf),
                str(output_prefix),
            ],
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


def _ocr_corpus(
    snapshots: dict[str, OfficialRawSnapshotV1],
    source_directory: Path,
    ocr_directory: Path,
    *,
    generate: bool,
) -> CensusDurableOcrCorpusV1:
    entries: list[CensusDurableOcrEntryV1] = []
    for uri, snapshot in sorted(snapshots.items()):
        assert snapshot.request.window_start is not None
        period = snapshot.request.window_start[:7]
        if not "1999-12" <= period <= "2002-12":
            continue
        stem = Path(_local_pdf_name(uri)).stem
        page_root = ocr_directory / stem
        paths = (page_root / "page-1.txt", page_root / "page-2.txt")
        if not all(path.exists() for path in paths):
            if not generate:
                raise FileNotFoundError(
                    f"reviewed OCR is missing for {stem}: use --generate-ocr"
                )
            page_texts = _ocr_candidate(
                source_directory / _local_pdf_name(uri), page_root
            )
        else:
            page_texts = (
                paths[0].read_text(encoding="utf-8"),
                paths[1].read_text(encoding="utf-8"),
            )
        entries.append(
            CensusDurableOcrEntryV1(
                artifact_uri=uri,
                content_sha256=snapshot.content_sha256,
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
    return CensusDurableOcrCorpusV1(
        engine="tesseract",
        engine_version=version,
        render_dpi=300,
        page_modes=(6, 6),
        entries=tuple(entries),
    )


def main() -> None:
    args = _arguments()
    source_directory = args.source_directory.resolve()
    ocr_directory = args.ocr_page_directory.resolve()
    source_directory.mkdir(parents=True, exist_ok=True)
    ocr_directory.mkdir(parents=True, exist_ok=True)
    captured_at_ns = time.time_ns()
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)

    index_request = build_census_durable_index_request(
        registry, as_of_date=args.as_of
    )
    index_content = _read_or_fetch(
        source_directory / "advance-index.html",
        DURABLE_INDEX_URI,
        fetch_missing=args.fetch_missing,
    )
    index_snapshot = _snapshot(index_request, index_content, captured_at_ns)
    release_index = parse_census_durable_release_index(
        index_snapshot, as_of_date=args.as_of
    )

    snapshots: dict[str, OfficialRawSnapshotV1] = {}
    for request in build_census_durable_archive_requests(
        registry, release_index
    ):
        content = _read_or_fetch(
            source_directory / _local_pdf_name(request.uri),
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        snapshots[request.uri] = _snapshot(request, content, captured_at_ns)
    ocr_corpus = _ocr_corpus(
        snapshots,
        source_directory,
        ocr_directory,
        generate=args.generate_ocr,
    )
    manifest = build_census_durable_archive_manifest(
        registry, profile, release_index, snapshots, ocr_corpus
    )

    asset_directory = (
        args.asset_directory.resolve()
        if args.asset_directory is not None
        else packaged_census_durable_archive_manifest_path().parent
    )
    asset_directory.mkdir(parents=True, exist_ok=True)
    index_payload = {
        "schema_version": DURABLE_INDEX_ENVELOPE_SCHEMA_VERSION,
        "as_of_date": args.as_of,
        "captured_at_ns": captured_at_ns,
        "uri": DURABLE_INDEX_URI,
        "content_sha256": index_snapshot.content_sha256,
        "content_base64": base64.b64encode(index_content).decode("ascii"),
    }
    (asset_directory / packaged_census_durable_index_path().name).write_text(
        canonical_contract_json(index_payload) + "\n", encoding="utf-8"
    )
    (asset_directory / packaged_census_durable_ocr_path().name).write_text(
        ocr_corpus.to_json() + "\n", encoding="utf-8"
    )
    (
        asset_directory / packaged_census_durable_archive_manifest_path().name
    ).write_text(manifest.to_json() + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "publications": len(manifest.publications),
                "measures": manifest.measure_occurrence_count,
                "revisions": manifest.comparable_revision_count,
                "artifacts": manifest.raw_artifact_count,
                "bytes": manifest.total_content_bytes,
                "ocr_publications": manifest.ocr_publication_count,
                "officially_unavailable": manifest.officially_unavailable_count,
                "core_source_unavailable": manifest.core_source_unavailable_count,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
