"""Real-archive qualification tests for BEA Gross Domestic Product."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    BEA_GDP_ARCHIVE_DATE_CORRECTIONS,
    BEA_GDP_PREDECESSOR_URI,
    BEA_GDP_SOURCE_KEY,
    BEA_GDP_SUPPLEMENTAL_URI,
    BEA_GDP_TIME_SUPPORT_URI,
    BeaGdpArchiveManifestV1,
    BeaGdpReleaseIndexEntryV1,
    EconomicReleaseStage,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    UnitedStatesBackfillProfileV1,
    bea_gdp_coverage_from_manifest,
    build_bea_gdp_archive_requests,
    built_in_united_states_backfill_profile,
    load_packaged_bea_gdp_archive_manifest,
    load_packaged_bea_gdp_index,
    load_packaged_bea_gdp_index_snapshots,
    load_packaged_official_source_registry,
    parse_bea_gdp_release,
    parse_bea_gdp_release_index,
)

CAPTURED_AT_NS = 1_789_344_000_000_000_000


def _profile() -> UnitedStatesBackfillProfileV1:
    return built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )


def _entry(
    *,
    reference_period: str = "2000-Q1",
    release_stage: EconomicReleaseStage = EconomicReleaseStage.ADVANCE,
    release_date: str = "2000-04-27",
    archive_published_date: str = "2000-04-27",
    artifact_uri: str = (
        "https://www.bea.gov/news/2000/"
        "gross-domestic-product-first-quarter-2000-advance-estimate"
    ),
    title: str = "Gross Domestic Product, First Quarter 2000 advance estimate",
) -> BeaGdpReleaseIndexEntryV1:
    return BeaGdpReleaseIndexEntryV1(
        reference_period=reference_period,
        release_stage=release_stage,
        release_date=release_date,
        archive_published_date=archive_published_date,
        artifact_uri=artifact_uri,
        title=title,
        archive_listed=True,
    )


def _snapshot(
    entry: BeaGdpReleaseIndexEntryV1, content: str
) -> OfficialRawSnapshotV1:
    source = load_packaged_official_source_registry().source(BEA_GDP_SOURCE_KEY)
    request = OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=entry.artifact_uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=entry.release_date,
        window_end=entry.release_date,
    )
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=CAPTURED_AT_NS,
        completed_at_ns=CAPTURED_AT_NS + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "text/html"},
        content=content.encode("utf-8"),
        content_type="text/html",
    )


def test_packaged_gdp_archive_quantifies_every_release_stage() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(BEA_GDP_SOURCE_KEY)
    manifest = load_packaged_bea_gdp_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision.value == "exact-minute"
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    }
    assert isinstance(manifest, BeaGdpArchiveManifestV1)
    assert len(manifest.entries) == 318
    assert manifest.raw_artifact_count == 320
    assert manifest.total_content_bytes == 27_626_013
    assert manifest.advance_release_count == 107
    assert manifest.second_release_count == 105
    assert manifest.final_release_count == 106
    assert manifest.nonzero_revision_count == 192
    assert manifest.advance_revision_count == 17
    assert manifest.second_revision_count == 92
    assert manifest.final_revision_count == 83
    assert manifest.historical_update_notice_count == 15
    assert manifest.correction_notice_count == 1
    assert manifest.shutdown_replacement_count == 3
    assert manifest.shutdown_disruption_quarter_count == 2
    assert manifest.fixed_width_release_count == 213
    assert manifest.narrative_release_count == 86
    assert manifest.comparison_table_release_count == 19
    assert manifest.zone_mismatch_count == 5
    assert manifest.archive_date_mismatch_count == 2
    assert BeaGdpArchiveManifestV1.from_json(manifest.to_json()) == manifest


def test_packaged_multi_page_index_replays_exactly() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()
    snapshots = load_packaged_bea_gdp_index_snapshots()
    parsed = parse_bea_gdp_release_index(
        snapshots, as_of_date=manifest.release_index.as_of_date
    )

    assert len(snapshots) == 18
    assert parsed == manifest.release_index
    assert parsed == load_packaged_bea_gdp_index()
    assert len(parsed.releases) == 318
    assert sum(item.archive_listed for item in parsed.releases) == 317
    assert parsed.releases[-1].artifact_uri == BEA_GDP_SUPPLEMENTAL_URI
    assert not parsed.releases[-1].archive_listed


def test_gdp_coverage_is_complete_for_the_builtin_profile() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()
    profile = _profile()
    coverage = bea_gdp_coverage_from_manifest(manifest)

    assert coverage.expected_occurrence_count == 318
    assert coverage.schedule_count == 318
    assert coverage.initial_actual_count == 318
    assert coverage.previous_as_known_count == 318
    assert coverage.revision_count == 192
    assert coverage.exact_minute_count == 318
    assert len(coverage.artifact_sha256s) == 320
    assert coverage.is_complete_for(profile.by_key["us.bea.gdp"])


def test_first_stage_is_bound_to_the_pdf_predecessor() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()
    entry = manifest.entries[0]

    assert entry.reference_period == "1999-Q4"
    assert entry.release_stage is EconomicReleaseStage.ADVANCE
    assert entry.observation.actual_value == 5.8
    assert entry.comparison_reference_period == "1999-Q3"
    assert entry.previous_artifact_uri == BEA_GDP_PREDECESSOR_URI
    assert entry.previous_as_known_value == 5.7
    assert entry.revised_previous_value == 5.7


def test_stage_and_annual_update_revisions_remain_distinct() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()
    advance = manifest.by_stage_key["1999-Q4:advance"]
    second = manifest.by_stage_key["1999-Q4:second"]
    final = manifest.by_stage_key["1999-Q4:final"]
    corrected = manifest.by_stage_key["2000-Q2:advance"]

    assert [
        advance.observation.actual_value,
        second.observation.actual_value,
        final.observation.actual_value,
    ] == [5.8, 6.9, 7.3]
    assert second.previous_as_known_value == 5.8
    assert second.revised_previous_value == 6.9
    assert final.previous_as_known_value == 6.9
    assert final.revised_previous_value == 7.3
    assert corrected.observation.correction_notice
    assert corrected.comparison_reference_period == "2000-Q1"
    assert corrected.previous_as_known_value == 5.5
    assert corrected.revised_previous_value == 4.8


def test_shutdown_sequences_preserve_their_actual_stage_inventory() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()

    assert "2018-Q4:second" not in manifest.by_stage_key
    assert "2025-Q3:second" not in manifest.by_stage_key
    assert (
        manifest.by_stage_key["2018-Q4:advance"].observation.actual_value == 2.6
    )
    assert (
        manifest.by_stage_key["2018-Q4:final"].observation.actual_value == 2.2
    )
    assert (
        manifest.by_stage_key["2025-Q3:advance"].observation.actual_value == 4.3
    )
    assert (
        manifest.by_stage_key["2025-Q3:final"].observation.actual_value == 4.4
    )


def test_source_metadata_anomalies_remain_explicit() -> None:
    manifest = load_packaged_bea_gdp_archive_manifest()
    mismatched_dates = {
        item.observation.artifact_uri: (
            item.observation.release_date,
            manifest.release_index.by_stage_key[
                item.stage_key
            ].archive_published_date,
        )
        for item in manifest.entries
        if item.observation.artifact_uri in BEA_GDP_ARCHIVE_DATE_CORRECTIONS
    }
    mismatched_zones = {
        (item.observation.release_date, item.observation.reported_zone)
        for item in manifest.entries
        if not item.observation.zone_consistent
    }

    assert mismatched_dates == {
        uri: (
            release_date,
            "2009-10-28" if release_date == "2009-10-29" else "2018-10-25",
        )
        for uri, release_date in BEA_GDP_ARCHIVE_DATE_CORRECTIONS.items()
    }
    assert mismatched_zones == {
        ("2007-11-29", "EDT"),
        ("2008-11-25", "EDT"),
        ("2008-12-23", "EDT"),
        ("2009-03-26", "EST"),
        ("2009-11-24", "EDT"),
    }
    supported = manifest.by_stage_key["2000-Q4:final"].observation
    assert supported.release_time_support_uri == BEA_GDP_TIME_SUPPORT_URI
    assert (
        supported.release_time_locator == "supporting-pdf:page-1:release-header"
    )


def test_parser_handles_preformatted_advance_release() -> None:
    entry = _entry()
    snapshot = _snapshot(
        entry,
        """
        <html><body>
        <div class="field--name-field-release-date">
        FOR WIRE TRANSMISSION: 8:30 A.M. EDT, THURSDAY, APRIL 27, 2000
        </div>
        <div class="field--name-body"><pre>
        Real gross domestic product -- the output of goods and services --
        increased at an annual rate of 1.5 percent in the first quarter of
        2000, according to advance estimates. In the fourth quarter of 1999,
        real GDP increased 7.3 percent.
        </pre></div></body></html>
        """,
    )

    observation = parse_bea_gdp_release(snapshot, entry)

    assert observation.actual_value == 1.5
    assert observation.prior_period_reference == "1999-Q4"
    assert observation.prior_period_value == 7.3
    assert observation.source_era == "fixed-width-preformatted-html"
    assert observation.zone_consistent


def test_request_inventory_and_tamper_validation_fail_closed() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_bea_gdp_archive_manifest()
    requests = build_bea_gdp_archive_requests(registry, manifest.release_index)

    assert len(requests) == 320
    assert {request.uri for request in requests} >= {
        BEA_GDP_PREDECESSOR_URI,
        BEA_GDP_TIME_SUPPORT_URI,
        BEA_GDP_SUPPLEMENTAL_URI,
    }
    with pytest.raises(ValueError, match="manifest identity differs"):
        replace(
            manifest, manifest_id="bea-gdp-archive-manifest:sha256:" + "0" * 64
        )
    with pytest.raises(ValueError, match="normalized digest differs"):
        replace(manifest.entries[0], normalized_sha256="0" * 64)
