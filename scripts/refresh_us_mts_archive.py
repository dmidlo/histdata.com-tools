#!/usr/bin/env python3
"""Refresh or replay the complete qualified Monthly Treasury Statement."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

import requests

from histdatacom.market_context import (
    MTS_CATALOG_ENVELOPE_SCHEMA_VERSION,
    MTS_CATALOG_URI,
    MTS_FIRST_REFERENCE_PERIOD,
    MTS_LATEST_PACKAGED_REFERENCE_PERIOD,
    MTS_PARSER_ID,
    MTS_PROGRAM_KEY,
    MTS_SOURCE_KEY,
    MtsTimingBasis,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    TreasuryMtsReleaseTimingV1,
    TreasuryMtsReviewedExtractsV1,
    TreasuryMtsValueOverrideV1,
    build_treasury_mts_archive_manifest,
    build_treasury_mts_catalog_request,
    build_treasury_mts_report_requests,
    built_in_united_states_backfill_profile,
    load_packaged_official_source_registry,
    load_packaged_treasury_mts_reviewed_extracts,
    packaged_treasury_mts_catalog_path,
    packaged_treasury_mts_manifest_path,
    packaged_treasury_mts_reviewed_extracts_path,
    parse_treasury_mts_release_index,
)
from histdatacom.market_context.economic_calendar import EconomicTimePrecision

USER_AGENT: Final = (
    "histdata.com-tools issue-538 Monthly Treasury Statement qualification "
    "(contact: repository maintainers)"
)
REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_ASSET_DIRECTORY: Final = (
    REPO_ROOT / "src" / "histdatacom" / "market_context" / "assets"
)
REPORT_URI_TEMPLATE: Final = (
    "https://fiscaldata.treasury.gov/static-data/published-reports/mts/"
    "MonthlyTreasuryStatement_{period}.pdf"
)
PRESS_URI_PREFIX: Final = "https://home.treasury.gov/news/press-releases/"

YEAR_END_EVIDENCE: Final = {
    2000: ("2000-10-24", "ls968"),
    2001: ("2001-10-29", "po734"),
    2002: ("2002-10-25", "po3578"),
    2003: ("2003-10-20", "js916"),
    2004: ("2004-10-14", "js2032"),
    2005: ("2005-10-14", "js2973"),
    2006: ("2006-10-12", "2006101215465518257"),
    2007: ("2007-10-11", "hp603"),
    2008: ("2008-10-14", "hp1213"),
    2009: ("2009-10-16", "tg322"),
    2010: ("2010-10-15", "tg911"),
    2011: ("2011-10-14", "tg1328"),
    2012: ("2012-10-12", "tg1734"),
    2013: ("2013-10-30", "jl2197"),
    2014: ("2014-10-15", "jl2664"),
    2015: ("2015-10-15", "jl0213"),
    2016: ("2016-10-14", "jl0583"),
    2017: ("2017-10-20", "sm0184"),
    2018: ("2018-10-15", "sm522"),
    2019: ("2019-10-25", "sm806"),
    2020: ("2020-10-16", "sm1155"),
    2021: ("2021-10-22", "jy0428"),
    2022: ("2022-10-21", "jy1043"),
    2023: ("2023-10-20", "jy1829"),
    2024: ("2024-10-18", "jy2657"),
}
MONTH_NUMBER: Final = {
    month: number
    for number, month in enumerate(
        (
            "",
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        )
    )
    if month
}
DATE_RE: Final = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+"
    r"(?P<day>\d{1,2}),\s*(?P<year>20\d{2})",
    re.IGNORECASE,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--source-directory", type=Path, required=True)
    parser.add_argument("--schedule-ocr-directory", type=Path)
    parser.add_argument("--asset-directory", type=Path, default=DEFAULT_ASSET_DIRECTORY)
    parser.add_argument("--fetch-missing", action="store_true")
    return parser.parse_args()


def _next_period(period: str) -> str:
    year, month = (int(item) for item in period.split("-"))
    return f"{year + 1:04d}-01" if month == 12 else f"{year:04d}-{month + 1:02d}"


def _previous_period(period: str) -> str:
    year, month = (int(item) for item in period.split("-"))
    return f"{year - 1:04d}-12" if month == 1 else f"{year:04d}-{month - 1:02d}"


def _periods(start: str, end: str) -> tuple[str, ...]:
    result = [start]
    while result[-1] < end:
        result.append(_next_period(result[-1]))
    return tuple(result)


def _report_uri(period: str) -> str:
    return REPORT_URI_TEMPLATE.format(period=period.replace("-", ""))


def _report_path(source_directory: Path, period: str) -> Path:
    return (
        source_directory
        / "reports"
        / f"MonthlyTreasuryStatement_{period.replace('-', '')}.pdf"
    )


def _fetch(path: Path, uri: str) -> bytes:
    response = requests.get(uri, headers={"User-Agent": USER_AGENT}, timeout=(15, 180))
    response.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(response.content)
    return response.content


def _read_or_fetch(path: Path, uri: str, *, fetch_missing: bool) -> bytes:
    if path.is_file():
        return path.read_bytes()
    if not fetch_missing:
        raise FileNotFoundError(path)
    return _fetch(path, uri)


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    captured_at_ns: int,
) -> OfficialRawSnapshotV1:
    content_type = {
        OfficialSourceFormat.JSON: "application/json",
        OfficialSourceFormat.PDF: "application/pdf",
        OfficialSourceFormat.HTML: "text/html",
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


def _press_request(
    registry: object, uri: str, release_date: str
) -> OfficialSourceRequestV1:
    source = registry.source(MTS_SOURCE_KEY)  # type: ignore[attr-defined]
    return OfficialSourceRequestV1(
        source_key=MTS_SOURCE_KEY,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=MTS_PARSER_ID,
        parser_version="1",
        window_start=release_date,
        window_end=release_date,
    )


def _parse_date(match: re.Match[str]) -> str:
    month = MONTH_NUMBER[match.group("month").title()]
    return (
        f"{int(match.group('year')):04d}-{month:02d}-" f"{int(match.group('day')):02d}"
    )


def _scheduled_date(text: str, target: str, *, table: bool = False) -> tuple[str, str]:
    year, month = (int(item) for item in target.split("-"))
    month_name = next(name for name, number in MONTH_NUMBER.items() if number == month)
    if table:
        pattern = re.compile(
            rf"{month_name}\s+{year}\s+" rf"(?P<date>{DATE_RE.pattern})",
            re.IGNORECASE,
        )
    else:
        pattern = re.compile(
            rf"release date for the\s+{month_name}(?:\s+{year})?\s+"
            rf"Statement\s+will be.*?(?P<date>{DATE_RE.pattern})",
            re.IGNORECASE | re.DOTALL,
        )
    match = pattern.search(text)
    if match is None:
        raise ValueError(f"schedule OCR lacks {target}")
    date_match = DATE_RE.search(match.group("date"))
    if date_match is None:
        raise ValueError(f"schedule OCR date differs for {target}")
    lexical = " ".join(match.group(0).split())
    return _parse_date(date_match), lexical


def _build_reviewed(
    source_directory: Path, schedule_directory: Path
) -> TreasuryMtsReviewedExtractsV1:
    timings: list[TreasuryMtsReleaseTimingV1] = []
    for period in _periods(
        MTS_FIRST_REFERENCE_PERIOD, MTS_LATEST_PACKAGED_REFERENCE_PERIOD
    ):
        year, month = (int(item) for item in period.split("-"))
        if month == 9:
            if year <= 2024:
                release_date, slug = YEAR_END_EVIDENCE[year]
                uri = PRESS_URI_PREFIX + slug
                path = source_directory / "year-end" / f"{year}-{slug}.html"
                content = path.read_bytes()
                lexical = (
                    f"Treasury year-end budget results published " f"{release_date}"
                )
                basis = MtsTimingBasis.YEAR_END_PRESS_RELEASE
            else:
                release_date = "2025-10-16"
                uri = _report_uri(period)
                content = _report_path(source_directory, period).read_bytes()
                lexical = "Last-Modified: Thu, 16 Oct 2025 18:58:28 GMT"
                basis = MtsTimingBasis.RESPONSE_LAST_MODIFIED
            timings.append(
                TreasuryMtsReleaseTimingV1(
                    reference_period=period,
                    release_date=release_date,
                    release_time_local=None,
                    time_precision=EconomicTimePrecision.DATE_ONLY,
                    basis=basis,
                    evidence_uri=uri,
                    evidence_sha256=hashlib.sha256(content).hexdigest(),
                    source_lexical=lexical,
                )
            )
            continue
        source_period = _previous_period(period)
        basis = MtsTimingBasis.PREVIOUS_REPORT
        if month == 12:
            annual_sources = []
            for candidate in schedule_directory.glob(
                "MonthlyTreasuryStatement_*11.txt"
            ):
                text = candidate.read_text(encoding="utf-8")
                if re.search(rf"^December\s+{year}\b", text, re.MULTILINE):
                    annual_sources.append(candidate.stem.rsplit("_", 1)[1])
            if not annual_sources:
                raise ValueError(f"annual schedule lacks {period}")
            source_period = f"{max(annual_sources)[:4]}-{max(annual_sources)[4:]}"
            basis = MtsTimingBasis.ANNUAL_TABLE
        if period == "2007-01":
            source_period = "2006-11"
            basis = MtsTimingBasis.REVIEWED_SOURCE_CORRECTION
        ocr_path = schedule_directory / (
            "MonthlyTreasuryStatement_" f"{source_period.replace('-', '')}.txt"
        )
        if period == "2001-12":
            release_date = "2002-01-21"
            lexical = "December 2001 Januay 21, 2002"
        else:
            try:
                release_date, lexical = _scheduled_date(
                    ocr_path.read_text(encoding="utf-8"),
                    period,
                    table=basis
                    in {
                        MtsTimingBasis.ANNUAL_TABLE,
                        MtsTimingBasis.REVIEWED_SOURCE_CORRECTION,
                    },
                )
            except ValueError:
                target_name = next(
                    name for name, number in MONTH_NUMBER.items() if number == month
                )
                fallbacks = []
                for candidate in schedule_directory.glob(
                    "MonthlyTreasuryStatement_*11.txt"
                ):
                    text = candidate.read_text(encoding="utf-8")
                    if re.search(
                        rf"^{target_name}\s+{year}\b",
                        text,
                        re.MULTILINE,
                    ):
                        fallbacks.append(candidate)
                if not fallbacks:
                    raise
                ocr_path = max(fallbacks, key=lambda item: item.name)
                compact = ocr_path.stem.rsplit("_", 1)[1]
                source_period = f"{compact[:4]}-{compact[4:]}"
                basis = MtsTimingBasis.ANNUAL_TABLE
                release_date, lexical = _scheduled_date(
                    ocr_path.read_text(encoding="utf-8"),
                    period,
                    table=True,
                )
        source_content = _report_path(source_directory, source_period).read_bytes()
        precise = not (month == 12 and year <= 2005)
        timings.append(
            TreasuryMtsReleaseTimingV1(
                reference_period=period,
                release_date=release_date,
                release_time_local="14:00" if precise else None,
                time_precision=(
                    EconomicTimePrecision.EXACT_MINUTE
                    if precise
                    else EconomicTimePrecision.INFERRED_BOUNDED
                ),
                basis=basis,
                evidence_uri=_report_uri(source_period),
                evidence_sha256=hashlib.sha256(source_content).hexdigest(),
                source_lexical=lexical,
            )
        )
    overrides = []
    values = {
        "2023-09": (467_473, 638_455, -170_982, 89_256),
        "2024-01": (477_320, 499_250, -21_930, -129_354),
        "2024-04": (776_198, 566_669, 209_529, -236_556),
        "2024-05": (323_647, 670_778, -347_131, 209_529),
    }
    for period, (receipts, outlays, actual, revised) in values.items():
        content = _report_path(source_directory, period).read_bytes()
        overrides.append(
            TreasuryMtsValueOverrideV1(
                reference_period=period,
                report_sha256=hashlib.sha256(content).hexdigest(),
                actual_receipts=receipts,
                actual_outlays=outlays,
                actual_balance=actual,
                revised_previous_balance=revised,
                locator=(
                    "reviewed 300-DPI Tesseract OCR of release PDF Table 1; "
                    "cross-checked against Table 2"
                ),
            )
        )
    return TreasuryMtsReviewedExtractsV1(
        timings=tuple(timings),
        value_overrides=tuple(overrides),
        review_method=(
            "Schedule pages rendered at 300 DPI and reviewed from Tesseract "
            "OCR; exceptional Table 1 values independently checked against "
            "Table 2 and bound to exact PDF hashes."
        ),
    )


def main() -> int:
    args = _arguments()
    source_directory = args.source_directory.expanduser().resolve()
    asset_directory = args.asset_directory.expanduser().resolve()
    asset_directory.mkdir(parents=True, exist_ok=True)
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    if profile.by_key[MTS_PROGRAM_KEY].source_key != MTS_SOURCE_KEY:
        raise ValueError("MTS profile source differs")
    captured_at_ns = time.time_ns()

    catalog_request = build_treasury_mts_catalog_request(
        registry, as_of_date=args.as_of
    )
    catalog_content = _read_or_fetch(
        source_directory / "page-data.json",
        MTS_CATALOG_URI,
        fetch_missing=args.fetch_missing,
    )
    catalog_snapshot = _snapshot(catalog_request, catalog_content, captured_at_ns)
    release_index = parse_treasury_mts_release_index(
        catalog_snapshot, as_of_date=args.as_of
    )
    report_requests = build_treasury_mts_report_requests(registry, release_index)
    snapshots_by_uri = {}
    for request in report_requests:
        period = Path(urlparse(request.uri).path).stem.rsplit("_", 1)[1]
        content = _read_or_fetch(
            source_directory / "reports" / f"MonthlyTreasuryStatement_{period}.pdf",
            request.uri,
            fetch_missing=args.fetch_missing,
        )
        snapshots_by_uri[request.uri] = _snapshot(request, content, captured_at_ns)

    if args.schedule_ocr_directory is None:
        reviewed = load_packaged_treasury_mts_reviewed_extracts()
    else:
        reviewed = _build_reviewed(
            source_directory,
            args.schedule_ocr_directory.expanduser().resolve(),
        )
    for timing in reviewed.timings:
        if timing.basis is not MtsTimingBasis.YEAR_END_PRESS_RELEASE:
            continue
        slug = timing.evidence_uri.rstrip("/").rsplit("/", 1)[1]
        year = timing.release_date[:4]
        path = source_directory / "year-end" / f"{year}-{slug}.html"
        content = _read_or_fetch(
            path, timing.evidence_uri, fetch_missing=args.fetch_missing
        )
        request = _press_request(registry, timing.evidence_uri, timing.release_date)
        snapshots_by_uri[timing.evidence_uri] = _snapshot(
            request, content, captured_at_ns
        )
    manifest = build_treasury_mts_archive_manifest(
        registry, profile, release_index, snapshots_by_uri, reviewed
    )

    (asset_directory / packaged_treasury_mts_manifest_path().name).write_text(
        manifest.to_json() + "\n", encoding="utf-8"
    )
    (asset_directory / packaged_treasury_mts_reviewed_extracts_path().name).write_text(
        reviewed.to_json() + "\n", encoding="utf-8"
    )
    catalog_envelope = {
        "schema_version": MTS_CATALOG_ENVELOPE_SCHEMA_VERSION,
        "content_base64": base64.b64encode(catalog_content).decode("ascii"),
    }
    (asset_directory / packaged_treasury_mts_catalog_path().name).write_text(
        json.dumps(
            catalog_envelope,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "manifest_id": manifest.manifest_id,
                "publication_count": len(manifest.publications),
                "revision_occurrence_count": (manifest.revision_occurrence_count),
                "exact_minute_count": manifest.exact_minute_count,
                "inferred_bounded_count": manifest.inferred_bounded_count,
                "date_only_count": manifest.date_only_count,
                "raw_artifact_count": manifest.raw_artifact_count,
                "total_content_bytes": manifest.total_content_bytes,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
