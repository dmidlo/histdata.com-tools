"""Real-archive qualification tests for the United States PPI program."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    BLS_PPI_INDEX_URI,
    BLS_PPI_PREDECESSOR_URI,
    BlsPpiArchiveManifestV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    bls_ppi_coverage_from_manifest,
    build_bls_ppi_archive_manifest,
    build_bls_ppi_archive_requests,
    build_bls_ppi_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_bls_ppi_archive_manifest,
    load_packaged_bls_ppi_index,
    load_packaged_official_source_registry,
    parse_bls_ppi_release,
    parse_bls_ppi_release_index,
    replay_bls_ppi_archive,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _request(
    uri: str, source_format: OfficialSourceFormat
) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bls.ppi")
    compact = uri.rsplit("ppi_", 1)[1].split(".", 1)[0]
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
    rows: str,
) -> str:
    return f"""Transmission of material in this release is embargoed
UNTIL 8:30 A.M. (E.S.T), {release_header}
PRODUCER PRICE INDEXES -- {title}
Table A. Monthly and annual percent changes in selected stage-of-processing
price indexes, seasonally adjusted
{rows}
r=revised. Source encoding marker: –
"""


def _semantic_release(
    *,
    release_header: str,
    title: str,
    years: tuple[tuple[int, tuple[tuple[str, str], ...]], ...],
    zone: str = "ET",
    table_id: str = "ppi_nrtablea",
) -> str:
    rows: list[str] = []
    for year, values in years:
        rows.append(f"<tr><th>{year}</th><td colspan='2'></td></tr>")
        rows.extend(
            f"<tr><th>{month}</th><td>{value}</td><td>9.9</td></tr>"
            for month, value in values
        )
    return f"""<!doctype html><html><body>
<pre>Transmission of material in this release is embargoed until
8:30 a.m. ({zone}) {release_header}
USDL fixture
PRODUCER PRICE INDEXES - {title}
</pre>
<table id="{table_id}"><tr><th>Month</th><th>Total</th></tr>
{''.join(rows)}
</table></body></html>"""


def _profile():
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def _synthetic_boundary_snapshots() -> (
    tuple[OfficialRawSnapshotV1, OfficialRawSnapshotV1]
):
    previous = _text_snapshot(
        BLS_PPI_PREDECESSOR_URI,
        _legacy_release(
            release_header="THURSDAY, JANUARY 13, 2000",
            title="DECEMBER 1999",
            rows="  1999\nDec.       .3   1.0   2.0",
        ),
    )
    current = _text_snapshot(
        "https://www.bls.gov/news.release/history/ppi_02172000.txt",
        _legacy_release(
            release_header="THURSDAY, FEBRUARY 17, 2000",
            title="JANUARY 2000",
            rows=(
                "  1999\nDec.     r .1   1.0   2.0\n"
                "  2000\nJan.        0   1.0   2.0"
            ),
        ),
    )
    return previous, current


def test_packaged_archive_quantifies_every_real_release() -> None:
    source = load_packaged_official_source_registry().source("us.bls.ppi")
    manifest = load_packaged_bls_ppi_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("PPI",)
    assert isinstance(manifest, BlsPpiArchiveManifestV1)
    assert len(manifest.entries) == 319
    assert manifest.raw_artifact_count == 320
    assert manifest.entries[0].reference_period == "2000-01"
    assert manifest.entries[0].previous_reference_period == "1999-12"
    assert manifest.entries[-1].reference_period == "2026-08"
    assert manifest.entries[-1].release_date == "2026-09-10"
    assert manifest.total_content_bytes == 237_890_117
    assert manifest.revision_occurrence_count == 59
    assert manifest.noncomparable_revision_count == 1
    assert len({item.content_sha256 for item in manifest.entries}) == 319
    assert len({item.normalized_sha256 for item in manifest.entries}) == 319
    assert manifest.release_index.unavailable_reference_periods == ("2025-10",)

    formats = tuple(item.source_format.value for item in manifest.entries)
    assert formats.count("text") == 96
    assert formats.count("html") == 223
    transition = manifest.by_reference_period["2014-01"]
    assert transition.series_lineage == "final-demand"
    assert not transition.revision_comparable


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_bls_ppi_index_request(registry, as_of_date="2026-09-14")
    content = load_packaged_bls_ppi_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_bls_ppi_release_index(snapshot, as_of_date="2026-09-14")
    manifest = load_packaged_bls_ppi_archive_manifest()

    assert request.uri == BLS_PPI_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    assert observed.content_length == 129_766
    requests = build_bls_ppi_archive_requests(registry, observed)
    assert len(requests) == 320
    assert requests[0].uri == BLS_PPI_PREDECESSOR_URI
    assert requests[1].uri.endswith("/ppi_02172000.txt")
    assert requests[-1].uri.endswith("/ppi_09102026.htm")
    assert {request.source_format for request in requests} == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.TEXT,
    }


def test_packaged_archive_derives_complete_ppi_coverage() -> None:
    profile = _profile()
    manifest = load_packaged_bls_ppi_archive_manifest()
    coverage = bls_ppi_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 319
    assert coverage.schedule_count == 319
    assert coverage.initial_actual_count == 319
    assert coverage.previous_as_known_count == 319
    assert coverage.revision_count == 59
    assert coverage.exact_minute_count == 319
    assert len(coverage.artifact_sha256s) == 319
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_legacy_text_parser_keeps_previous_as_known_distinct() -> None:
    previous, current = _synthetic_boundary_snapshots()
    triplet = parse_bls_ppi_release(
        current,
        reference_period="2000-01",
        previous_snapshot=previous,
        previous_reference_period="1999-12",
    )

    assert triplet.actual_value == 0.0
    assert triplet.actual_lexical == "0"
    assert triplet.previous_as_known_value == 0.3
    assert triplet.revised_previous_value == 0.1
    assert triplet.revised_previous_lexical == ".1r"
    assert triplet.transformation == "month-over-month-percent-change"
    assert any("windows-1252" in item for item in triplet.limitations)


def test_final_demand_transition_is_explicitly_noncomparable() -> None:
    previous = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_01152014.htm",
        _semantic_release(
            release_header="Wednesday, January 15, 2014",
            title="DECEMBER 2013",
            years=((2013, (("Nov.", "-0.1"), ("Dec.", "0.4"))),),
            zone="EST",
            table_id="ppinr_tablea",
        ),
    )
    current = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_02192014.htm",
        _semantic_release(
            release_header="Wednesday, February 19, 2014",
            title="JANUARY 2014",
            years=(
                (2013, (("Dec.", "0.1"),)),
                (2014, (("Jan.", "0.2"),)),
            ),
            zone="EST",
            table_id="ppinr2014_tablea",
        ),
    )
    triplet = parse_bls_ppi_release(
        current,
        reference_period="2014-01",
        previous_snapshot=previous,
        previous_reference_period="2013-12",
    )

    assert triplet.actual_value == 0.2
    assert triplet.previous_as_known_value == 0.4
    assert triplet.revised_previous_value == 0.4
    assert ".final-demand." in triplet.logical_event_key
    assert any("not a comparable revision" in x for x in triplet.limitations)


def test_text_table_html_fallback_and_malformed_header_are_bounded() -> None:
    previous = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_12142016.htm",
        _semantic_release(
            release_header="Wednesday, December 14, 2016",
            title="NOVEMBER 2016",
            years=((2016, (("Oct.", "0.0"), ("Nov.", "0.4"))),),
            zone="EST",
        ),
    )
    current = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_01132017.htm",
        """<html><body><pre>Transmission embargoed until
8:30 a.m. (EST), Friday, January, 13, 2017
PRODUCER PRICE INDEXES - DECEMBER 2016
Table A. Monthly and annual percent changes
  2016
Nov.       0.5   1.0
Dec.       0.3   1.0
r=revised.
</pre></body></html>""",
    )
    triplet = parse_bls_ppi_release(
        current,
        reference_period="2016-12",
        previous_snapshot=previous,
        previous_reference_period="2016-11",
    )

    assert triplet.actual_value == 0.3
    assert triplet.previous_as_known_value == 0.4
    assert triplet.revised_previous_value == 0.5
    assert "line:" in triplet.actual_locator
    assert any("extra comma" in x for x in triplet.limitations)


def test_shutdown_gap_retains_previous_published_period() -> None:
    september = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_11252025.htm",
        _semantic_release(
            release_header="Tuesday, November 25, 2025",
            title="SEPTEMBER 2025",
            years=((2025, (("Aug.", "-0.1"), ("Sept.", "0.3"))),),
        ),
    )
    november = _html_snapshot(
        "https://www.bls.gov/news.release/archives/ppi_01142026.htm",
        _semantic_release(
            release_header="Wednesday, January 14, 2026",
            title="NOVEMBER 2025",
            years=(
                (
                    2025,
                    (("Sept.", "0.6"), ("Oct.", "0.1"), ("Nov.", "0.2")),
                ),
            ),
        ),
    )
    triplet = parse_bls_ppi_release(
        november,
        reference_period="2025-11",
        previous_snapshot=september,
        previous_reference_period="2025-09",
    )

    assert triplet.transformation == "month-over-month-percent-change"
    assert triplet.actual_value == 0.2
    assert triplet.previous_reference_period == "2025-09"
    assert triplet.previous_as_known_value == 0.3
    assert triplet.revised_previous_value == 0.6
    assert any(
        "October 2025 was not published" in x for x in triplet.limitations
    )


def test_one_release_manifest_build_and_replay_are_exact() -> None:
    profile = _profile()
    packaged = load_packaged_bls_ppi_archive_manifest()
    index = replace(
        packaged.release_index,
        releases=(packaged.release_index.releases[0],),
        unavailable_reference_periods=(),
        index_id="",
    )
    previous, current = _synthetic_boundary_snapshots()
    manifest = build_bls_ppi_archive_manifest(
        profile, index, (previous, current)
    )

    assert len(manifest.entries) == 1
    assert replay_bls_ppi_archive(manifest, (previous, current))[0].triplet_id
    with pytest.raises(ValueError, match="differ from the manifest"):
        replay_bls_ppi_archive(manifest, (previous,))


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_bls_ppi_archive_manifest()

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
    with pytest.raises(ValueError, match="series lineage differs"):
        replace(
            manifest.by_reference_period["2014-01"],
            series_lineage="finished-goods",
            entry_id="",
        )
    with pytest.raises(ValueError, match="total byte count differs"):
        replace(manifest, total_content_bytes=manifest.total_content_bytes + 1)
    with pytest.raises(ValueError, match="identity differs"):
        BlsPpiArchiveManifestV1.from_dict(
            {**manifest.to_dict(), "profile_id": manifest.profile_id[:-1] + "0"}
        )
