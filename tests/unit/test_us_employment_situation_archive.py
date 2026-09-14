"""Real-archive qualification tests for the U.S. Employment Situation."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS,
    BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE,
    BLS_EMPLOYMENT_SITUATION_INDEX_URI,
    BLS_EMPLOYMENT_SITUATION_PREDECESSOR_RELEASE_DATE,
    BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI,
    BLS_EMPLOYMENT_UNEMPLOYMENT_RATE,
    BlsEmploymentSituationArchiveManifestV1,
    BlsEmploymentSituationReleaseV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    UnitedStatesBackfillProfileV1,
    bls_employment_situation_coverage_from_manifest,
    build_bls_employment_situation_archive_manifest,
    build_bls_employment_situation_archive_requests,
    build_bls_employment_situation_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_bls_employment_situation_archive_manifest,
    load_packaged_bls_employment_situation_index,
    load_packaged_official_source_registry,
    parse_bls_employment_situation_release,
    parse_bls_employment_situation_release_index,
    replay_bls_employment_situation_archive,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _request(
    uri: str, source_format: OfficialSourceFormat
) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bls.employment-situation")
    if uri == BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI:
        release_date = BLS_EMPLOYMENT_SITUATION_PREDECESSOR_RELEASE_DATE
    else:
        compact = uri.rsplit("empsit_", 1)[1].split(".", 1)[0]
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
    unemployment: tuple[str, ...],
    nonfarm: tuple[str, ...],
    earnings: tuple[str, ...],
    zone: str = "EST",
) -> str:
    return f"""Transmission of material in this release is embargoed
until 8:30 A.M. ({zone}), Friday, {release_header}.
THE EMPLOYMENT SITUATION: {title}
Table A. Major indicators of labor market activity, seasonally adjusted
All workers...........|{"|".join(unemployment)}
Nonfarm employment....|{"|".join(nonfarm)}
Avg. hourly earnings, |        |        |        |        |        |
  total private.......|{"|".join(earnings)}
p=preliminary. Source encoding marker: –
"""


def _semantic_release(
    *,
    release_header: str,
    title: str,
    periods: tuple[str, str, str, str],
    unemployment: tuple[str, str, str, str],
    nonfarm: tuple[str, str, str, str],
    earnings: tuple[str, str, str, str],
    zone: str,
    earnings_row_id: str = "ces_table10.r.4.1.2",
) -> str:
    def header(table: str) -> str:
        cells = "".join(f"<th>{period}</th>" for period in periods)
        return f"<thead><tr><th>Category</th>{cells}</tr></thead>"

    def cells(values: tuple[str, str, str, str]) -> str:
        return "".join(f"<td>{value}</td>" for value in values)

    return f"""<html><body><pre>
Transmission of material in this release is embargoed until
8:30 a.m. ({zone}) Friday, {release_header}
THE EMPLOYMENT SITUATION - {title}
</pre>
<table id="cps_empsit_sum">{header("cps_empsit_sum")}<tbody>
<tr><th id="cps_empsit_sum.r.2.1.3.1">Unemployment rate</th>
{cells(unemployment)}<td>0.0</td></tr></tbody></table>
<table id="ces_table10">{header("ces_table10")}<tbody>
<tr><th id="ces_table10.r.1.1">Total nonfarm</th>{cells(nonfarm)}</tr>
<tr><th id="{earnings_row_id}">Average hourly earnings</th>
{cells(earnings)}</tr></tbody></table>
</body></html>"""


def _profile() -> UnitedStatesBackfillProfileV1:
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def _synthetic_boundary_snapshots() -> (
    tuple[OfficialRawSnapshotV1, OfficialRawSnapshotV1]
):
    previous = _text_snapshot(
        BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI,
        _legacy_release(
            release_header="January 7, 2000",
            title="DECEMBER 1999",
            unemployment=("4.2", "4.1", "4.1", "4.1", "4.1", "0.0"),
            nonfarm=(
                "128,500",
                "129,000",
                "129,274",
                "129,589",
                "p129,904",
                "p315",
            ),
            earnings=(
                "$13.30",
                "$13.40",
                "$13.42",
                "$13.45",
                "p$13.46",
                "p$0.01",
            ),
        ),
    )
    current = _text_snapshot(
        "https://www.bls.gov/news.release/history/empsit_02042000.txt",
        _legacy_release(
            release_header="February 4, 2000",
            title="JANUARY 2000",
            unemployment=("4.2", "4.1", "4.1", "4.1", "4.0", "-0.1"),
            nonfarm=(
                "128,936",
                "129,609",
                "129,589",
                "p129,905",
                "p130,292",
                "p387",
            ),
            earnings=(
                "$13.31",
                "$13.41",
                "$13.40",
                "p$13.44",
                "p$13.50",
                "p$0.06",
            ),
        ),
    )
    return previous, current


def test_packaged_archive_quantifies_every_real_release() -> None:
    source = load_packaged_official_source_registry().source(
        "us.bls.employment-situation"
    )
    manifest = load_packaged_bls_employment_situation_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("Employment Situation",)
    assert isinstance(manifest, BlsEmploymentSituationArchiveManifestV1)
    assert len(manifest.entries) == 319
    assert manifest.raw_artifact_count == 320
    assert manifest.entries[0].reference_period == "2000-01"
    assert manifest.entries[0].previous_reference_period == "1999-12"
    assert manifest.entries[-1].reference_period == "2026-08"
    assert manifest.entries[-1].release_date == "2026-09-04"
    assert manifest.total_content_bytes == 183_693_130
    assert manifest.revision_occurrence_count == 316
    assert manifest.unemployment_revision_count == 6
    assert manifest.nonfarm_payroll_revision_count == 314
    assert manifest.earnings_revision_count == 239
    assert manifest.noncomparable_revision_count == 1
    assert manifest.release_index.unavailable_reference_periods == ("2025-10",)

    formats = tuple(item.source_format.value for item in manifest.entries)
    assert formats.count("text") == 96
    assert formats.count("html") == 223
    eras = tuple(item.source_era for item in manifest.entries)
    assert eras.count("fixed-width-text") == 96
    assert eras.count("preformatted-html") == 24
    assert eras.count("semantic-html") == 199
    assert manifest.by_reference_period["2012-11"].reported_zone == "EDT"


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_bls_employment_situation_index_request(
        registry, as_of_date="2026-09-14"
    )
    content = load_packaged_bls_employment_situation_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_bls_employment_situation_release_index(
        snapshot, as_of_date="2026-09-14"
    )
    manifest = load_packaged_bls_employment_situation_archive_manifest()

    assert request.uri == BLS_EMPLOYMENT_SITUATION_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    assert observed.content_length == 132_524
    requests = build_bls_employment_situation_archive_requests(
        registry, observed
    )
    assert len(requests) == 320
    assert requests[0].uri == BLS_EMPLOYMENT_SITUATION_PREDECESSOR_URI
    assert requests[0].window_start == "2000-01-07"
    assert requests[1].uri.endswith("/empsit_02042000.txt")
    assert requests[-1].uri.endswith("/empsit_09042026.htm")


def test_packaged_archive_derives_complete_coverage() -> None:
    profile = _profile()
    manifest = load_packaged_bls_employment_situation_archive_manifest()
    coverage = bls_employment_situation_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 319
    assert coverage.schedule_count == 319
    assert coverage.initial_actual_count == 319
    assert coverage.previous_as_known_count == 319
    assert coverage.revision_count == 316
    assert coverage.exact_minute_count == 319
    assert coverage.forecast_count == 0
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_fixed_width_parser_keeps_three_survey_measures_distinct() -> None:
    previous, current = _synthetic_boundary_snapshots()
    release = parse_bls_employment_situation_release(
        current,
        reference_period="2000-01",
        previous_snapshot=previous,
        previous_reference_period="1999-12",
    )

    assert isinstance(release, BlsEmploymentSituationReleaseV1)
    assert release.unemployment_rate.actual_value == 4.0
    assert release.unemployment_rate.previous_as_known_value == 4.1
    assert release.nonfarm_payroll_change.actual_value == 387.0
    assert release.nonfarm_payroll_change.previous_as_known_value == 315.0
    assert release.nonfarm_payroll_change.revised_previous_value == 316.0
    assert release.average_hourly_earnings.actual_value == 13.5
    assert release.average_hourly_earnings.previous_as_known_value == 13.46
    assert release.average_hourly_earnings.revised_previous_value == 13.44
    assert "derived-columns" in release.nonfarm_payroll_change.revision_locator
    assert (
        BlsEmploymentSituationReleaseV1.from_dict(release.to_dict()) == release
    )


def test_earnings_transition_is_explicitly_noncomparable() -> None:
    previous = _text_snapshot(
        "https://www.bls.gov/news.release/history/empsit_01082010.txt",
        _legacy_release(
            release_header="January 8, 2010",
            title="DECEMBER 2009",
            unemployment=("9.7", "10.0", "10.1", "10.0", "10.0", "0.0"),
            nonfarm=(
                "131,262",
                "130,965",
                "130,991",
                "p130,995",
                "p130,910",
                "p-85",
            ),
            earnings=(
                "$18.64",
                "$18.77",
                "$18.74",
                "p$18.77",
                "p$18.80",
                "p$0.03",
            ),
        ),
    )
    current = _html_snapshot(
        "https://www.bls.gov/news.release/archives/empsit_02052010.htm",
        _semantic_release(
            release_header="February 5, 2010",
            title="JANUARY 2010",
            periods=("Jan. 2009", "Nov. 2009", "Dec. 2009", "Jan. 2010"),
            unemployment=("7.8", "9.9", "10.0", "9.7"),
            nonfarm=("-787", "64", "-150", "-20"),
            earnings=("$21.92", "$22.39", "$22.41", "$22.45"),
            zone="EST",
            earnings_row_id="ces_table10.r.3.1.2",
        ),
    )
    release = parse_bls_employment_situation_release(
        current,
        reference_period="2010-01",
        previous_snapshot=previous,
        previous_reference_period="2009-12",
    )

    earnings = release.average_hourly_earnings
    assert earnings.actual_value == 22.45
    assert earnings.previous_as_known_value == 18.8
    assert earnings.revised_previous_value == 18.8
    assert "all-employees" in earnings.logical_event_key
    assert any("noncomparable" in item for item in earnings.limitations)


def test_shutdown_gap_retains_previous_published_period() -> None:
    september = _html_snapshot(
        "https://www.bls.gov/news.release/archives/empsit_11202025.htm",
        _semantic_release(
            release_header="November 20, 2025",
            title="SEPTEMBER 2025",
            periods=("Sept. 2024", "July 2025", "Aug. 2025", "Sept. 2025"),
            unemployment=("4.1", "4.2", "4.3", "4.4"),
            nonfarm=("240", "72", "-4", "119"),
            earnings=("$35.33", "$36.43", "$36.58", "$36.67"),
            zone="ET",
        ),
    )
    november = _html_snapshot(
        "https://www.bls.gov/news.release/archives/empsit_12162025.htm",
        _semantic_release(
            release_header="December 16, 2025",
            title="NOVEMBER 2025",
            periods=("Nov. 2024", "Sept. 2025", "Oct. 2025", "Nov. 2025"),
            unemployment=("4.2", "4.4", "-", "4.6"),
            nonfarm=("261", "108", "-105", "64"),
            earnings=("$35.61", "$36.65", "$36.81", "$36.86"),
            zone="ET",
        ),
    )
    release = parse_bls_employment_situation_release(
        november,
        reference_period="2025-11",
        previous_snapshot=september,
        previous_reference_period="2025-09",
    )

    assert release.unemployment_rate.previous_reference_period == "2025-09"
    assert release.nonfarm_payroll_change.previous_as_known_value == 119.0
    assert release.nonfarm_payroll_change.revised_previous_value == 108.0
    assert release.average_hourly_earnings.revised_previous_value == 36.65
    assert any(
        "October 2025 was not published" in item
        for item in release.unemployment_rate.limitations
    )


def test_one_release_manifest_build_and_replay_are_exact() -> None:
    profile = _profile()
    packaged = load_packaged_bls_employment_situation_archive_manifest()
    index = replace(
        packaged.release_index,
        releases=(packaged.release_index.releases[0],),
        unavailable_reference_periods=(),
        index_id="",
    )
    previous, current = _synthetic_boundary_snapshots()
    manifest = build_bls_employment_situation_archive_manifest(
        profile, index, (previous, current)
    )

    assert len(manifest.entries) == 1
    assert replay_bls_employment_situation_archive(
        manifest, (previous, current)
    )[0].release_id
    with pytest.raises(ValueError, match="differ from the manifest"):
        replay_bls_employment_situation_archive(manifest, (previous,))


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_bls_employment_situation_archive_manifest()

    with pytest.raises(ValueError, match="unexplained monthly gap"):
        replace(
            manifest.release_index,
            releases=(
                manifest.release_index.releases[0],
                manifest.release_index.releases[2],
            ),
            index_id="",
        )
    measure = manifest.entries[0].by_measure[
        BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE
    ]
    with pytest.raises(ValueError, match="differs from actual_value"):
        replace(measure, actual_lexical="999", measure_id="")
    transition = manifest.by_reference_period["2010-01"].by_measure[
        BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS
    ]
    with pytest.raises(ValueError, match="measure definition differs"):
        replace(
            manifest.by_reference_period["2010-01"],
            measures=(
                *manifest.by_reference_period["2010-01"].measures[:2],
                replace(transition, revision_comparable=True, measure_id=""),
            ),
            entry_id="",
        )
    with pytest.raises(ValueError, match="total_content_bytes differs"):
        replace(
            manifest,
            total_content_bytes=manifest.total_content_bytes + 1,
            manifest_id="",
        )


def test_measure_keys_select_expected_real_manifest_rows() -> None:
    entry = load_packaged_bls_employment_situation_archive_manifest().entries[0]

    assert tuple(entry.by_measure) == (
        BLS_EMPLOYMENT_UNEMPLOYMENT_RATE,
        BLS_EMPLOYMENT_NONFARM_PAYROLL_CHANGE,
        BLS_EMPLOYMENT_AVERAGE_HOURLY_EARNINGS,
    )
