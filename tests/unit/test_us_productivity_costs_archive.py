"""Real-archive qualification tests for BLS Productivity and Costs."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256

import pytest

from histdatacom.market_context import (
    BLS_PRODUCTIVITY,
    BLS_PRODUCTIVITY_COSTS_CORRECTION_URI,
    BLS_PRODUCTIVITY_COSTS_INDEX_URI,
    BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI,
    BLS_PRODUCTIVITY_COSTS_UNAVAILABLE_URI,
    BLS_UNIT_LABOR_COSTS,
    BlsProductivityCostsArchiveManifestV1,
    BlsProductivityCostsReleaseIndexEntryV1,
    EconomicReleaseStage,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    UnitedStatesBackfillProfileV1,
    bls_productivity_costs_coverage_from_manifest,
    build_bls_productivity_costs_archive_requests,
    build_bls_productivity_costs_index_request,
    built_in_united_states_backfill_profile,
    load_packaged_bls_productivity_costs_archive_manifest,
    load_packaged_bls_productivity_costs_index,
    load_packaged_official_source_registry,
    parse_bls_productivity_costs_release,
    parse_bls_productivity_costs_release_index,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


def _request(
    uri: str,
    source_format: OfficialSourceFormat,
    release_date: str,
) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source("us.bls.productivity-costs")
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


def _text_snapshot(
    uri: str, release_date: str, content: str
) -> OfficialRawSnapshotV1:
    return _snapshot(
        _request(uri, OfficialSourceFormat.TEXT, release_date),
        content.encode("windows-1252"),
        content_type="text/plain",
    )


def _html_snapshot(
    uri: str, release_date: str, content: str
) -> OfficialRawSnapshotV1:
    return _snapshot(
        _request(uri, OfficialSourceFormat.HTML, release_date),
        content.encode("windows-1252"),
        content_type="text/html",
    )


def _index_entry(
    *,
    reference_period: str,
    release_stage: EconomicReleaseStage,
    release_date: str,
    artifact_uri: str,
    source_format: OfficialSourceFormat,
) -> BlsProductivityCostsReleaseIndexEntryV1:
    label_stage = (
        "Preliminary"
        if release_stage is EconomicReleaseStage.PRELIMINARY
        else "Revised"
    )
    return BlsProductivityCostsReleaseIndexEntryV1(
        reference_period=reference_period,
        release_stage=release_stage,
        release_date=release_date,
        artifact_uri=artifact_uri,
        source_format=source_format,
        label=f"{reference_period} ({label_stage}) Productivity and Costs",
    )


def _profile() -> UnitedStatesBackfillProfileV1:
    registry = load_packaged_official_source_registry()
    return built_in_united_states_backfill_profile(registry)


def test_packaged_archive_quantifies_every_real_release_stage() -> None:
    source = load_packaged_official_source_registry().source(
        "us.bls.productivity-costs"
    )
    manifest = load_packaged_bls_productivity_costs_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert source.source_release_ids == ("Productivity and Costs",)
    assert isinstance(manifest, BlsProductivityCostsArchiveManifestV1)
    assert len(manifest.entries) == 214
    assert manifest.raw_artifact_count == 215
    assert manifest.total_content_bytes == 21_086_013
    assert manifest.available_release_count == 213
    assert manifest.unavailable_release_count == 1
    assert manifest.revision_observation_count == 426
    assert manifest.revision_occurrence_count == 208
    assert manifest.productivity_revision_count == 191
    assert manifest.unit_labor_cost_revision_count == 202
    assert manifest.noncomparable_lineage_count == 3
    assert manifest.historical_revision_notice_count == 76
    assert manifest.entries[0].stage_key == "1999-Q4:preliminary"
    assert manifest.entries[-1].stage_key == "2026-Q2:revision"

    formats = tuple(item.source_format.value for item in manifest.entries)
    assert formats.count("text") == 64
    assert formats.count("html") == 150
    layouts = tuple(item.table_layout for item in manifest.entries)
    assert layouts.count("sector-row") == 155
    assert layouts.count("measure-row") == 59


def test_retained_release_index_replays_to_packaged_inventory() -> None:
    registry = load_packaged_official_source_registry()
    request = build_bls_productivity_costs_index_request(
        registry, as_of_date="2026-09-14"
    )
    content = load_packaged_bls_productivity_costs_index()
    snapshot = _snapshot(request, content, content_type="text/html")
    observed = parse_bls_productivity_costs_release_index(
        snapshot, as_of_date="2026-09-14"
    )
    manifest = load_packaged_bls_productivity_costs_archive_manifest()

    assert request.uri == BLS_PRODUCTIVITY_COSTS_INDEX_URI
    assert observed == manifest.release_index
    assert observed.content_sha256 == sha256(content).hexdigest()
    assert observed.content_length == 117_331
    assert len(observed.releases) == 214
    assert observed.predecessor.artifact_uri == (
        BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI
    )
    assert (
        sum(
            item.release_stage is EconomicReleaseStage.PRELIMINARY
            for item in observed.releases
        )
        == 107
    )
    assert (
        sum(
            item.release_stage is EconomicReleaseStage.REVISION
            for item in observed.releases
        )
        == 107
    )
    requests = build_bls_productivity_costs_archive_requests(registry, observed)
    assert len(requests) == 215
    assert requests[0].uri == BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI
    assert requests[-1].uri.endswith("/prod2_09032026.htm")


def test_packaged_archive_derives_complete_qualified_coverage() -> None:
    profile = _profile()
    manifest = load_packaged_bls_productivity_costs_archive_manifest()
    coverage = bls_productivity_costs_coverage_from_manifest(
        profile, manifest, window_end_date="2026-09-14"
    )

    assert coverage.expected_occurrence_count == 214
    assert coverage.schedule_count == 214
    assert coverage.initial_actual_count == 213
    assert coverage.previous_as_known_count == 213
    assert coverage.revision_count == 213
    assert coverage.exact_minute_count == 214
    assert coverage.officially_unavailable_count == 1
    assert coverage.is_complete_for(profile.by_key[coverage.program_key])


def test_first_quarter_pair_keeps_both_release_stages_distinct() -> None:
    manifest = load_packaged_bls_productivity_costs_archive_manifest()
    preliminary = manifest.by_stage_key["1999-Q4:preliminary"]
    revised = manifest.by_stage_key["1999-Q4:revision"]

    preliminary_productivity = preliminary.by_measure[BLS_PRODUCTIVITY]
    assert preliminary_productivity.actual_value == 5.0
    assert preliminary_productivity.previous_as_known_value == 4.9
    assert preliminary_productivity.revised_comparison_value == 5.0
    assert preliminary_productivity.revision_value == pytest.approx(0.1)
    assert preliminary.by_measure[BLS_UNIT_LABOR_COSTS].actual_value == -1.0
    assert (
        preliminary.by_measure[BLS_UNIT_LABOR_COSTS].revised_comparison_value
        == -0.3
    )

    revised_productivity = revised.by_measure[BLS_PRODUCTIVITY]
    assert revised_productivity.actual_value == 6.4
    assert revised_productivity.previous_as_known_value == 5.0
    assert revised_productivity.revised_comparison_value == 6.4
    assert revised_productivity.revision_value == pytest.approx(1.4)
    assert revised.by_measure[BLS_UNIT_LABOR_COSTS].actual_value == -2.5


def test_shutdown_unavailability_and_following_revision_are_explicit() -> None:
    manifest = load_packaged_bls_productivity_costs_archive_manifest()
    preliminary = manifest.by_stage_key["2018-Q4:preliminary"]
    revised = manifest.by_stage_key["2018-Q4:revision"]

    assert preliminary.artifact_uri == BLS_PRODUCTIVITY_COSTS_UNAVAILABLE_URI
    assert not preliminary.actual_available
    assert all(item.actual_lexical == "N.A." for item in preliminary.measures)
    assert preliminary.by_measure[
        BLS_PRODUCTIVITY
    ].revision_value == pytest.approx(-0.1)
    assert preliminary.by_measure[BLS_UNIT_LABOR_COSTS].revision_value == 0.0

    assert revised.actual_available
    assert revised.by_measure[BLS_PRODUCTIVITY].actual_value == 1.9
    assert revised.by_measure[BLS_PRODUCTIVITY].previous_as_known_value is None
    assert revised.by_measure[BLS_PRODUCTIVITY].revision_value is None
    assert all(
        not item.predecessor_lineage_comparable for item in revised.measures
    )


def test_reissued_2024_lineage_discrepancy_is_not_smoothed_over() -> None:
    entry = (
        load_packaged_bls_productivity_costs_archive_manifest().by_stage_key[
            "2024-Q1:preliminary"
        ]
    )
    productivity = entry.by_measure[BLS_PRODUCTIVITY]

    assert entry.artifact_uri == BLS_PRODUCTIVITY_COSTS_CORRECTION_URI
    assert entry.source_correction_notice
    assert productivity.previous_as_known_value == 3.2
    assert productivity.source_comparison_previous_value == 3.3
    assert productivity.revised_comparison_value == 3.5
    assert productivity.revision_value == pytest.approx(0.2)
    assert not productivity.predecessor_lineage_comparable
    assert entry.by_measure[BLS_UNIT_LABOR_COSTS].predecessor_lineage_comparable


def test_fixed_width_parser_reads_main_and_comparison_tables() -> None:
    previous_uri = BLS_PRODUCTIVITY_COSTS_PREDECESSOR_URI
    current_uri = "https://www.bls.gov/news.release/history/prod2_02082000.txt"
    previous = _text_snapshot(
        previous_uri,
        "1999-12-07",
        """TRANSMISSION OF THIS MATERIAL IS EMBARGOED UNTIL 8:30 A.M. EST,
TUESDAY, DECEMBER 7, 1999
PRODUCTIVITY AND COSTS
Table A. Productivity and costs: Revised third-quarter 1999 measures
Nonfarm business    4.9  6.6  1.6  4.7  2.1  -0.2
""",
    )
    current = _text_snapshot(
        current_uri,
        "2000-02-08",
        """MATERIAL IS EMBARGOED UNTIL 8:30 A.M. EST,
TUESDAY, FEBRUARY 8, 2000
PRODUCTIVITY AND COSTS
Table A. Productivity and costs: Preliminary fourth-quarter 1999 measures
Nonfarm business    5.0  6.6  1.5  4.0  1.1  -1.0
Table C. Previous and revised productivity and related measures
Nonfarm business:
  Previous          4.9  6.6  1.6  4.7  2.1  -0.2
  Current           5.0  6.8  1.7  4.7  2.0  -0.3
""",
    )
    entry = parse_bls_productivity_costs_release(
        current,
        indexed_release=_index_entry(
            reference_period="1999-Q4",
            release_stage=EconomicReleaseStage.PRELIMINARY,
            release_date="2000-02-08",
            artifact_uri=current_uri,
            source_format=OfficialSourceFormat.TEXT,
        ),
        previous_snapshot=previous,
        previous_indexed_release=_index_entry(
            reference_period="1999-Q3",
            release_stage=EconomicReleaseStage.REVISION,
            release_date="1999-12-07",
            artifact_uri=previous_uri,
            source_format=OfficialSourceFormat.TEXT,
        ),
    )

    assert entry.table_layout == "sector-row"
    assert entry.by_measure[BLS_PRODUCTIVITY].actual_value == 5.0
    assert entry.by_measure[BLS_PRODUCTIVITY].previous_as_known_value == 4.9
    assert entry.by_measure[BLS_UNIT_LABOR_COSTS].actual_value == -1.0
    assert entry.reported_zone == "EST"


def test_measure_row_parser_reads_2010_layout() -> None:
    previous_uri = (
        "https://www.bls.gov/news.release/archives/prod2_03042010.htm"
    )
    current_uri = "https://www.bls.gov/news.release/archives/prod2_05062010.htm"
    previous = _html_snapshot(
        previous_uri,
        "2010-03-04",
        """<pre>
Table A. Revised fourth-quarter 2009 measures: percent change
Sector Nonfarm Business
Productivity 6.3 1.0 2.0 3.0
Unit labor costs -5.6 1.0 2.0 3.0
</pre>""",
    )
    current = _html_snapshot(
        current_uri,
        "2010-05-06",
        """<pre>
Transmission embargoed until 8:30 a.m. (EDT) Thursday, May 6, 2010
Table A. Preliminary first-quarter 2010 measures: percent change
Sector Nonfarm Business
Productivity 3.6 6.3 2.0 3.0
Unit labor costs -1.6 -3.7 2.0 3.0
Table B. Revised and previous measures: fourth quarter 2009
Sector Nonfarm Business Revised Previous Revised Previous
Productivity 6.4 6.3 2.0 3.0
Unit labor costs -5.7 -5.6 2.0 3.0
</pre>""",
    )
    entry = parse_bls_productivity_costs_release(
        current,
        indexed_release=_index_entry(
            reference_period="2010-Q1",
            release_stage=EconomicReleaseStage.PRELIMINARY,
            release_date="2010-05-06",
            artifact_uri=current_uri,
            source_format=OfficialSourceFormat.HTML,
        ),
        previous_snapshot=previous,
        previous_indexed_release=_index_entry(
            reference_period="2009-Q4",
            release_stage=EconomicReleaseStage.REVISION,
            release_date="2010-03-04",
            artifact_uri=previous_uri,
            source_format=OfficialSourceFormat.HTML,
        ),
    )

    assert entry.table_layout == "measure-row"
    assert entry.by_measure[BLS_PRODUCTIVITY].actual_value == 3.6
    assert entry.by_measure[BLS_PRODUCTIVITY].revision_value == pytest.approx(
        0.1
    )
    assert entry.by_measure[
        BLS_UNIT_LABOR_COSTS
    ].revision_value == pytest.approx(-0.1)


def test_archive_contracts_reject_tampering() -> None:
    manifest = load_packaged_bls_productivity_costs_archive_manifest()

    with pytest.raises(ValueError, match="stage pairing"):
        replace(
            manifest.release_index,
            releases=manifest.release_index.releases[1:],
            index_id="",
        )
    measure = manifest.entries[0].by_measure[BLS_PRODUCTIVITY]
    with pytest.raises(ValueError, match="differs from its numeric value"):
        replace(measure, actual_lexical="999", measure_id="")
    with pytest.raises(ValueError, match="total_content_bytes differs"):
        replace(
            manifest,
            total_content_bytes=manifest.total_content_bytes + 1,
            manifest_id="",
        )
