"""Real-archive qualification tests for the U.S. JOLTS release."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    BLS_JOLTS_HIRES,
    BLS_JOLTS_INDEX_TYPO_REFERENCE_PERIOD,
    BLS_JOLTS_INDEX_TYPO_RELEASE_URI,
    BLS_JOLTS_INDEX_URI,
    BLS_JOLTS_JOB_OPENINGS,
    BLS_JOLTS_TOTAL_SEPARATIONS,
    BlsJoltsArchiveManifestV1,
    BlsJoltsReleaseV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    UnitedStatesBackfillProfileV1,
    bls_jolts_coverage_from_manifest,
    build_bls_jolts_archive_manifest,
    build_bls_jolts_archive_requests,
    build_bls_jolts_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_bls_jolts_archive_manifest,
    load_packaged_bls_jolts_index,
    load_packaged_official_source_registry,
    parse_bls_jolts_release,
    parse_bls_jolts_release_index,
    replay_bls_jolts_archive,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000
PREDECESSOR_URI = "https://www.bls.gov/news.release/history/jolts_04152004.txt"
FIRST_RELEASE_URI = (
    "https://www.bls.gov/news.release/history/jolts_05112004.txt"
)


def _request(
    uri: str, source_format: OfficialSourceFormat
) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bls.jolts")
    compact = uri.rsplit("jolts_", 1)[1].split(".", 1)[0]
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
        content.encode(),
        content_type="text/html",
    )


def _legacy_release(
    *,
    release_header: str,
    title: str,
    values: tuple[str, ...],
    zone: str = "EST",
) -> str:
    return f"""Transmission of material in this release is embargoed
until 10:00 A.M. ({zone}), Thursday, {release_header}.
JOB OPENINGS AND LABOR TURNOVER: {title}
Table A. Job openings, hires, and total separations by industry, seasonally adjusted
                   |                     Levels (in thousands)
Total..............|{"|".join(values)}
                   |                       Rates (percent)
Total..............|1.0|1.1|1.2|2.0|2.1|2.2|3.0|3.1|3.2
p=preliminary. Source encoding marker: –
"""


def _semantic_release(
    *,
    release_header: str,
    title: str,
    periods: tuple[str, str, str],
    values: tuple[str, ...],
    zone: str,
) -> str:
    headers = "".join(f"<th>{period}</th>" for period in periods * 3)
    cells = "".join(
        f'<td><span class="datavalue">{value}</span></td>' for value in values
    )
    rates = "".join("<td>1.0</td>" for _ in range(9))
    return f"""<html><body><pre>
Transmission of material in this release is embargoed until
10:00 a.m. ({zone}) Friday, {release_header}
JOB OPENINGS AND LABOR TURNOVER - {title}
</pre>
<table id="jolts_tablea"><thead><tr><th>Category</th>{headers}</tr></thead>
<tbody><tr><th id="jolts_tablea.r.3"><p>Total</p></th>{cells}</tr>
<tr><th id="jolts_tablea.r.20"><p>Total</p></th>{rates}</tr></tbody></table>
</body></html>"""


def _profile() -> UnitedStatesBackfillProfileV1:
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def _synthetic_boundary_snapshots() -> (
    tuple[OfficialRawSnapshotV1, OfficialRawSnapshotV1]
):
    previous = _text_snapshot(
        PREDECESSOR_URI,
        _legacy_release(
            release_header="April 15, 2004",
            title="FEBRUARY 2004",
            values=(
                "2,000",
                "2,800",
                "2,907",
                "3,500",
                "4,000",
                "4,064",
                "3,400",
                "3,900",
                "4,019",
            ),
            zone="EDT",
        ),
    )
    current = _text_snapshot(
        FIRST_RELEASE_URI,
        _legacy_release(
            release_header="May 11, 2004",
            title="MARCH 2004",
            values=(
                "2,100",
                "2,906",
                "3,072",
                "3,600",
                "4,103",
                "4,544",
                "3,500",
                "4,073",
                "4,113",
            ),
            zone="EDT",
        ),
    )
    return previous, current


def test_packaged_archive_quantifies_every_real_release() -> None:
    source = load_packaged_official_source_registry().source("us.bls.jolts")
    manifest = load_packaged_bls_jolts_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("Job Openings and Labor Turnover",)
    assert isinstance(manifest, BlsJoltsArchiveManifestV1)
    assert len(manifest.entries) == 268
    assert manifest.raw_artifact_count == 269
    assert manifest.entries[0].reference_period == "2004-03"
    assert manifest.entries[0].previous_reference_period == "2004-02"
    assert manifest.entries[-1].reference_period == "2026-07"
    assert manifest.entries[-1].release_date == "2026-09-01"
    assert manifest.total_content_bytes == 99_093_148
    assert manifest.revision_occurrence_count == 267
    assert manifest.job_openings_revision_count == 266
    assert manifest.hires_revision_count == 267
    assert manifest.total_separations_revision_count == 266
    assert manifest.noncomparable_revision_count == 3
    assert manifest.annual_benchmark_release_count == 22
    assert manifest.release_index.unavailable_reference_periods == ("2025-09",)

    formats = tuple(item.source_format.value for item in manifest.entries)
    assert formats.count("text") == 45
    assert formats.count("html") == 223
    eras = tuple(item.source_era for item in manifest.entries)
    assert eras.count("fixed-width-text") == 45
    assert eras.count("preformatted-html") == 77
    assert eras.count("semantic-html") == 146
    assert sum(item.annual_benchmark_release for item in manifest.entries) == 22


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_bls_jolts_index_request(registry, as_of_date="2026-09-14")
    content = load_packaged_bls_jolts_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_bls_jolts_release_index(snapshot, as_of_date="2026-09-14")
    manifest = load_packaged_bls_jolts_archive_manifest()

    assert request.uri == BLS_JOLTS_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    assert observed.content_length == 89_112
    assert len(observed.releases) == 269
    assert (
        observed.by_reference_period[
            BLS_JOLTS_INDEX_TYPO_REFERENCE_PERIOD
        ].artifact_uri
        == BLS_JOLTS_INDEX_TYPO_RELEASE_URI
    )
    requests = build_bls_jolts_archive_requests(registry, observed)
    assert len(requests) == 269
    assert requests[0].uri == PREDECESSOR_URI
    assert requests[0].window_start == "2004-04-15"
    assert requests[-1].uri.endswith("/jolts_09012026.htm")


def test_packaged_archive_derives_complete_coverage() -> None:
    profile = _profile()
    manifest = load_packaged_bls_jolts_archive_manifest()
    coverage = bls_jolts_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 268
    assert coverage.schedule_count == 268
    assert coverage.initial_actual_count == 268
    assert coverage.previous_as_known_count == 268
    assert coverage.revision_count == 267
    assert coverage.exact_minute_count == 268
    assert coverage.forecast_count == 0
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_fixed_width_parser_keeps_three_measures_distinct() -> None:
    previous, current = _synthetic_boundary_snapshots()
    release = parse_bls_jolts_release(
        current,
        reference_period="2004-03",
        previous_snapshot=previous,
        previous_reference_period="2004-02",
    )

    assert isinstance(release, BlsJoltsReleaseV1)
    assert release.job_openings.actual_value == 3_072.0
    assert release.job_openings.previous_as_known_value == 2_907.0
    assert release.job_openings.revised_previous_value == 2_906.0
    assert release.hires.actual_value == 4_544.0
    assert release.hires.previous_as_known_value == 4_064.0
    assert release.hires.revised_previous_value == 4_103.0
    assert release.total_separations.actual_value == 4_113.0
    assert release.total_separations.previous_as_known_value == 4_019.0
    assert release.total_separations.revised_previous_value == 4_073.0
    assert BlsJoltsReleaseV1.from_dict(release.to_dict()) == release


def test_semantic_parser_reads_total_level_row() -> None:
    previous = _html_snapshot(
        "https://www.bls.gov/news.release/archives/jolts_09032025.htm",
        _semantic_release(
            release_header="September 3, 2025",
            title="JULY 2025",
            periods=("July 2024", "June 2025", "July 2025"),
            values=(
                "7,600",
                "7,500",
                "7,437",
                "5,600",
                "5,300",
                "5,288",
                "5,500",
                "5,200",
                "5,293",
            ),
            zone="ET",
        ),
    )
    current = _html_snapshot(
        "https://www.bls.gov/news.release/archives/jolts_09302025.htm",
        _semantic_release(
            release_header="September 30, 2025",
            title="AUGUST 2025",
            periods=("August 2024", "July 2025", "August 2025"),
            values=(
                "7,700",
                "7,438",
                "7,227",
                "5,500",
                "5,289",
                "5,126",
                "5,400",
                "5,294",
                "5,091",
            ),
            zone="ET",
        ),
    )
    release = parse_bls_jolts_release(
        current,
        reference_period="2025-08",
        previous_snapshot=previous,
        previous_reference_period="2025-07",
    )

    assert release.job_openings.actual_value == 7_227.0
    assert release.job_openings.previous_as_known_value == 7_437.0
    assert release.job_openings.revised_previous_value == 7_438.0
    assert "table#jolts_tablea" in release.hires.actual_locator


def test_shutdown_gap_is_explicitly_noncomparable() -> None:
    august = _html_snapshot(
        "https://www.bls.gov/news.release/archives/jolts_09302025.htm",
        _semantic_release(
            release_header="September 30, 2025",
            title="AUGUST 2025",
            periods=("August 2024", "July 2025", "August 2025"),
            values=(
                "7,700",
                "7,438",
                "7,227",
                "5,500",
                "5,289",
                "5,126",
                "5,400",
                "5,294",
                "5,091",
            ),
            zone="ET",
        ),
    )
    october = _html_snapshot(
        "https://www.bls.gov/news.release/archives/jolts_12092025.htm",
        _semantic_release(
            release_header="December 9, 2025",
            title="OCTOBER 2025",
            periods=("October 2024", "September 2025", "October 2025"),
            values=(
                "7,615",
                "7,658",
                "7,670",
                "5,350",
                "5,367",
                "5,149",
                "5,285",
                "5,264",
                "5,050",
            ),
            zone="ET",
        ),
    )
    release = parse_bls_jolts_release(
        october,
        reference_period="2025-10",
        previous_snapshot=august,
        previous_reference_period="2025-08",
    )

    assert release.job_openings.previous_reference_period == "2025-08"
    assert release.job_openings.previous_as_known_value == 7_227.0
    assert release.job_openings.revised_previous_value == 7_227.0
    assert any(
        "September 2025 was not published" in item
        for item in release.job_openings.limitations
    )
    packaged = load_packaged_bls_jolts_archive_manifest()
    assert all(
        not item.revision_comparable
        for item in packaged.by_reference_period["2025-10"].measures
    )


def test_one_release_manifest_build_and_replay_are_exact() -> None:
    profile = _profile()
    packaged = load_packaged_bls_jolts_archive_manifest()
    index = replace(
        packaged.release_index,
        releases=packaged.release_index.releases[:2],
        unavailable_reference_periods=(),
        index_id="",
    )
    previous, current = _synthetic_boundary_snapshots()
    manifest = build_bls_jolts_archive_manifest(
        profile, index, (previous, current)
    )

    assert len(manifest.entries) == 1
    assert replay_bls_jolts_archive(manifest, (previous, current))[0].release_id
    with pytest.raises(ValueError, match="differ from the manifest"):
        replay_bls_jolts_archive(manifest, (previous,))


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_bls_jolts_archive_manifest()

    with pytest.raises(ValueError, match="unexplained monthly gap"):
        replace(
            manifest.release_index,
            releases=(
                manifest.release_index.releases[0],
                manifest.release_index.releases[2],
            ),
            index_id="",
        )
    measure = manifest.entries[0].by_measure[BLS_JOLTS_JOB_OPENINGS]
    with pytest.raises(ValueError, match="differs from actual_value"):
        replace(measure, actual_lexical="999", measure_id="")
    with pytest.raises(ValueError, match="annual benchmark status differs"):
        replace(
            manifest.entries[0],
            annual_benchmark_release=True,
            entry_id="",
        )
    with pytest.raises(ValueError, match="total_content_bytes differs"):
        replace(
            manifest,
            total_content_bytes=manifest.total_content_bytes + 1,
            manifest_id="",
        )


def test_measure_keys_select_expected_real_manifest_rows() -> None:
    entry = load_packaged_bls_jolts_archive_manifest().entries[0]

    assert tuple(entry.by_measure) == (
        BLS_JOLTS_JOB_OPENINGS,
        BLS_JOLTS_HIRES,
        BLS_JOLTS_TOTAL_SEPARATIONS,
    )
