"""Real-archive qualification tests for the United States CPI program."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

import histdatacom.market_context.us_cpi_archive as cpi_module
from histdatacom.market_context import (
    BLS_CPI_INDEX_URI,
    BLS_CPI_PDF_SUBSTITUTION_NOTE,
    BLS_CPI_PREDECESSOR_URI,
    BlsCpiArchiveManifestV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    bls_cpi_coverage_from_manifest,
    build_bls_cpi_archive_manifest,
    build_bls_cpi_archive_requests,
    build_bls_cpi_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_bls_cpi_archive_manifest,
    load_packaged_bls_cpi_index,
    load_packaged_official_source_registry,
    parse_bls_cpi_release,
    parse_bls_cpi_release_index,
    replay_bls_cpi_archive,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _request(
    uri: str, source_format: OfficialSourceFormat
) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bls.cpi")
    compact = uri.rsplit("cpi_", 1)[1].split(".", 1)[0]
    release_date = f"{compact[4:]}-{compact[:2]}-{compact[2:4]}"
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=release_date,
        window_end=release_date,
    )


def _snapshot(
    request: OfficialSourceRequestV1,
    content: bytes,
    *,
    content_type: str,
) -> OfficialRawSnapshotV1:
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=CAPTURED_AT_NS,
        completed_at_ns=CAPTURED_AT_NS + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def _text_snapshot(uri: str, content: str) -> OfficialRawSnapshotV1:
    return _snapshot(
        _request(uri, OfficialSourceFormat.TEXT),
        content.encode("windows-1252"),
        content_type="text/plain",
    )


def _html_snapshot(uri: str, content: str) -> OfficialRawSnapshotV1:
    return _snapshot(
        _request(uri, OfficialSourceFormat.HTML),
        content.encode("utf-8"),
        content_type="text/html",
    )


def _legacy_release(
    *,
    release_header: str,
    title: str,
    values: str,
) -> str:
    return f"""Transmission of material in this release is embargoed
UNTIL 8:30 A.M. (EST) INTERNET ADDRESS: https://www.bls.gov {release_header}
CONSUMER PRICE INDEX: {title}
Table A. Percent changes in CPI for All Urban Consumers (CPI-U)
Seasonally adjusted                     Unadjusted
All Items            {values}
Source encoding marker: –
"""


def _semantic_release(
    *,
    release_header: str,
    title: str,
    values: tuple[str, ...],
    narrative: str = "",
) -> str:
    cells = "".join(f"<td>{item}</td>" for item in values)
    return f"""<!doctype html><html><body>
<pre>Transmission of material in this release is embargoed until
8:30 a.m. (ET) {release_header}
USDL fixture
CONSUMER PRICE INDEX - {title}
{narrative}
</pre>
<table id="cpi_pressa"><tr><th>Expenditure category</th></tr>
<tr><th>All items</th>{cells}</tr></table>
</body></html>"""


def _profile():
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def test_packaged_archive_quantifies_every_real_release() -> None:
    source = load_packaged_official_source_registry().source("us.bls.cpi")
    manifest = load_packaged_bls_cpi_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("CPI",)
    assert isinstance(manifest, BlsCpiArchiveManifestV1)
    assert len(manifest.entries) == 318
    assert manifest.raw_artifact_count == 319
    assert manifest.entries[0].reference_period == "2000-01"
    assert manifest.entries[0].previous_reference_period == "1999-12"
    assert manifest.entries[-1].reference_period == "2026-07"
    assert manifest.entries[-1].release_date == "2026-08-12"
    assert manifest.total_content_bytes == 214_928_540
    assert manifest.revision_occurrence_count == 15
    assert manifest.noncomparable_revision_count == 1
    assert len({item.content_sha256 for item in manifest.entries}) == 318
    assert len({item.normalized_sha256 for item in manifest.entries}) == 318
    assert manifest.release_index.unavailable_reference_periods == ("2025-10",)

    formats = tuple(item.source_format.value for item in manifest.entries)
    assert formats.count("text") == 96
    assert formats.count("html") == 221
    assert formats.count("pdf") == 1


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_bls_cpi_index_request(registry, as_of_date="2026-09-14")
    content = load_packaged_bls_cpi_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_bls_cpi_release_index(snapshot, as_of_date="2026-09-14")
    manifest = load_packaged_bls_cpi_archive_manifest()

    assert request.uri == BLS_CPI_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    assert observed.content_length == 128_354
    requests = build_bls_cpi_archive_requests(registry, observed)
    assert len(requests) == 319
    assert requests[0].uri == BLS_CPI_PREDECESSOR_URI
    assert requests[1].uri.endswith("/cpi_02182000.txt")
    assert requests[-1].uri.endswith("/cpi_08122026.htm")
    pdf = requests[
        1
        + next(
            index
            for index, entry in enumerate(observed.releases)
            if entry.reference_period == "2016-05"
        )
    ]
    assert pdf.source_format is OfficialSourceFormat.PDF
    assert pdf.uri.endswith("/cpi_06162016.pdf")


def test_packaged_archive_derives_complete_cpi_coverage() -> None:
    profile = _profile()
    manifest = load_packaged_bls_cpi_archive_manifest()
    coverage = bls_cpi_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 318
    assert coverage.schedule_count == 318
    assert coverage.initial_actual_count == 318
    assert coverage.previous_as_known_count == 318
    assert coverage.revision_count == 15
    assert coverage.exact_minute_count == 318
    assert len(coverage.artifact_sha256s) == 318
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_legacy_text_parser_keeps_previous_as_known_distinct() -> None:
    previous = _text_snapshot(
        BLS_CPI_PREDECESSOR_URI,
        _legacy_release(
            release_header="Friday, January 14, 2000",
            title="DECEMBER 1999",
            values=".0 .4 .2 .5 .1 .1 .3 2.2 2.7",
        ),
    )
    current = _text_snapshot(
        "https://www.bls.gov/news.release/history/cpi_02182000.txt",
        _legacy_release(
            release_header="Friday, February 18, 2000",
            title="JANUARY 2000",
            values=".3 .3 .4 .2 .2 .2 .2r 2.4 2.7",
        ),
    )

    triplet = parse_bls_cpi_release(
        current,
        reference_period="2000-01",
        previous_snapshot=previous,
        previous_reference_period="1999-12",
    )

    assert triplet.actual_value == 0.2
    assert triplet.actual_lexical == ".2r"
    assert triplet.previous_as_known_value == 0.3
    assert triplet.revised_previous_value == 0.2
    assert triplet.transformation == "month-over-month-percent-change"
    assert any("windows-1252" in item for item in triplet.limitations)


def test_shutdown_releases_preserve_two_month_and_noncomparable_semantics() -> (
    None
):
    september = _html_snapshot(
        "https://www.bls.gov/news.release/archives/cpi_10242025.htm",
        _semantic_release(
            release_header="Friday, October 24, 2025",
            title="SEPTEMBER 2025",
            values=("0.2", "0.1", "0.3", "0.2", "0.4", "0.4", "0.3", "3.0"),
        ),
    )
    november = _html_snapshot(
        "https://www.bls.gov/news.release/archives/cpi_12182025.htm",
        _semantic_release(
            release_header="Thursday, December 18, 2025",
            title="NOVEMBER 2025",
            values=("0.2", "0.1", "0.3", "0.2", "0.3", "-", "-", "2.7"),
            narrative=(
                "The CPI-U increased 0.2 percent on a seasonally adjusted "
                "basis over the 2 months from September to November."
            ),
        ),
    )
    december = _html_snapshot(
        "https://www.bls.gov/news.release/archives/cpi_01132026.htm",
        _semantic_release(
            release_header="Tuesday, January 13, 2026",
            title="DECEMBER 2025",
            values=("0.3", "0.2", "0.4", "0.3", "-", "-", "0.3", "2.7"),
        ),
    )

    november_triplet = parse_bls_cpi_release(
        november,
        reference_period="2025-11",
        previous_snapshot=september,
        previous_reference_period="2025-09",
    )
    december_triplet = parse_bls_cpi_release(
        december,
        reference_period="2025-12",
        previous_snapshot=november,
        previous_reference_period="2025-11",
    )

    assert november_triplet.transformation == "two-month-percent-change"
    assert november_triplet.actual_value == 0.2
    assert november_triplet.previous_reference_period == "2025-09"
    assert december_triplet.actual_value == 0.3
    assert december_triplet.previous_as_known_value == 0.2
    assert december_triplet.revised_previous_value == 0.2
    assert any(
        "no comparable revised November" in item
        for item in december_triplet.limitations
    )


def test_pdf_substitution_parser_uses_official_table_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    april = _html_snapshot(
        "https://www.bls.gov/news.release/archives/cpi_05172016.htm",
        _semantic_release(
            release_header="Tuesday, May 17, 2016",
            title="APRIL 2016",
            values=("0.1", "0.2", "0.3", "0.2", "0.3", "0.1", "0.4", "1.1"),
        ),
    )
    pdf_text = """Transmission is embargoed until 8:30 a.m. (EDT) June 16, 2016
CONSUMER PRICE INDEX – MAY 2016
Table 1. Consumer Price Index for All Urban Consumers (CPI-U)
All items. ............................................ 100.000 237.805 239.261 240.236 1.0 0.4 0.1 0.4 0.2
"""
    monkeypatch.setattr(cpi_module, "_pdf_release_text", lambda _: pdf_text)
    current = _snapshot(
        _request(
            "https://www.bls.gov/news.release/archives/cpi_06162016.pdf",
            OfficialSourceFormat.PDF,
        ),
        b"%PDF-fixture",
        content_type="application/pdf",
    )

    triplet = parse_bls_cpi_release(
        current,
        reference_period="2016-05",
        previous_snapshot=april,
        previous_reference_period="2016-04",
    )

    assert triplet.actual_value == 0.2
    assert triplet.previous_as_known_value == 0.4
    assert triplet.revised_previous_value == 0.4
    assert any(
        BLS_CPI_PDF_SUBSTITUTION_NOTE in item for item in triplet.limitations
    )


def test_one_release_manifest_build_and_replay_are_exact() -> None:
    profile = _profile()
    packaged = load_packaged_bls_cpi_archive_manifest()
    index = replace(
        packaged.release_index,
        releases=(packaged.release_index.releases[0],),
        unavailable_reference_periods=(),
        index_id="",
    )
    previous = _text_snapshot(
        BLS_CPI_PREDECESSOR_URI,
        _legacy_release(
            release_header="Friday, January 14, 2000",
            title="DECEMBER 1999",
            values=".0 .4 .2 .5 .1 .1 .3 2.2 2.7",
        ),
    )
    current = _text_snapshot(
        index.releases[0].artifact_uri,
        _legacy_release(
            release_header="Friday, February 18, 2000",
            title="JANUARY 2000",
            values=".3 .3 .4 .2 .2 .2 .2 2.4 2.7",
        ),
    )
    manifest = build_bls_cpi_archive_manifest(
        profile, index, (previous, current)
    )

    assert len(manifest.entries) == 1
    assert replay_bls_cpi_archive(manifest, (previous, current))[0].triplet_id
    with pytest.raises(ValueError, match="differ from the manifest"):
        replay_bls_cpi_archive(manifest, (previous,))


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_bls_cpi_archive_manifest()

    with pytest.raises(ValueError, match="unexplained monthly gap"):
        replace(
            manifest.release_index,
            releases=(
                manifest.release_index.releases[0],
                manifest.release_index.releases[2],
            ),
            index_id="",
        )
    with pytest.raises(ValueError, match="differs from actual_value"):
        replace(manifest.entries[0], actual_lexical=".9", entry_id="")
    with pytest.raises(ValueError, match="total byte count differs"):
        replace(manifest, total_content_bytes=manifest.total_content_bytes + 1)
    with pytest.raises(ValueError, match="identity differs"):
        BlsCpiArchiveManifestV1.from_dict(
            {
                **manifest.to_dict(),
                "profile_id": manifest.profile_id + "-tampered",
            }
        )
