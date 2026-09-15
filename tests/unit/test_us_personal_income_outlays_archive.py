"""Real-archive qualification tests for BEA Personal Income and Outlays."""

from __future__ import annotations

from dataclasses import replace

import pytest

from histdatacom.market_context import (
    BEA_PIO_ARCHIVE_DATE_CORRECTIONS,
    BEA_PIO_CORE_PRICE,
    BEA_PIO_CURRENT_DOLLAR_PCE,
    BEA_PIO_HEADLINE_PRICE,
    BEA_PIO_PERSONAL_INCOME,
    BEA_PIO_PREDECESSOR_URI,
    BEA_PIO_PROGRAM_KEY,
    BEA_PIO_SOURCE_KEY,
    BEA_PIO_SUPPLEMENTAL_URI,
    BeaPioArchiveManifestV1,
    BeaPioValueBasis,
    EconomicReleaseStage,
    EconomicTimePrecision,
    OfficialSourceFormat,
    bea_pio_coverage_from_manifest,
    build_bea_pio_archive_requests,
    build_bea_pio_index_requests,
    built_in_united_states_backfill_profile,
    load_packaged_bea_pio_archive_manifest,
    load_packaged_bea_pio_index,
    load_packaged_bea_pio_index_snapshots,
    load_packaged_official_source_registry,
    parse_bea_pio_release_index,
)


def _publication(release_date: str):
    manifest = load_packaged_bea_pio_archive_manifest()
    return next(
        item
        for item in manifest.publications
        if item.observation.release_date == release_date
    )


def test_packaged_pio_archive_quantifies_every_measure_occurrence() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(BEA_PIO_SOURCE_KEY)
    manifest = load_packaged_bea_pio_archive_manifest()

    assert source.verification_status.value == "empirically-verified"
    assert source.availability_time_precision is EconomicTimePrecision.DATE_ONLY
    assert set(source.formats) == {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
        OfficialSourceFormat.XLSX,
    }
    assert isinstance(manifest, BeaPioArchiveManifestV1)
    assert len(manifest.publications) == 319
    assert manifest.raw_artifact_count == 334
    assert manifest.total_content_bytes == 21_699_009
    assert manifest.initial_publication_count == 318
    assert manifest.revision_publication_count == 1
    assert manifest.measure_occurrence_count == 1_280
    assert manifest.personal_income_initial_count == 320
    assert manifest.current_dollar_pce_initial_count == 320
    assert manifest.headline_price_initial_count == 314
    assert manifest.core_price_initial_count == 314
    assert manifest.comparable_revision_count == 626
    assert manifest.derived_price_occurrence_count == 42
    assert manifest.historical_update_notice_count == 9
    assert manifest.shutdown_publication_count == 5
    assert manifest.support_artifact_count == 14
    assert manifest.exact_minute_count == 318
    assert manifest.date_only_count == 1
    assert manifest.zone_mismatch_count == 4
    assert manifest.archive_date_mismatch_count == 1
    assert BeaPioArchiveManifestV1.from_json(manifest.to_json()) == manifest


def test_packaged_multi_page_index_replays_exactly() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_bea_pio_archive_manifest()
    snapshots = load_packaged_bea_pio_index_snapshots()
    parsed = parse_bea_pio_release_index(
        snapshots, as_of_date=manifest.release_index.as_of_date
    )

    assert len(build_bea_pio_index_requests(registry)) == 19
    assert len(snapshots) == 19
    assert parsed == manifest.release_index
    assert parsed == load_packaged_bea_pio_index()
    assert len(parsed.releases) == 319
    assert sum(item.archive_listed for item in parsed.releases) == 318
    assert parsed.releases[-1].artifact_uri == BEA_PIO_SUPPLEMENTAL_URI
    requests = build_bea_pio_archive_requests(registry, parsed)
    assert len(requests) == 334
    assert (
        sum(item.source_format is OfficialSourceFormat.PDF for item in requests)
        == 3
    )
    assert (
        sum(
            item.source_format is OfficialSourceFormat.TEXT for item in requests
        )
        == 1
    )
    assert (
        sum(
            item.source_format is OfficialSourceFormat.XLSX for item in requests
        )
        == 11
    )


def test_pio_coverage_is_complete_for_the_builtin_profile() -> None:
    registry = load_packaged_official_source_registry()
    profile = built_in_united_states_backfill_profile(registry)
    manifest = load_packaged_bea_pio_archive_manifest()
    coverage = bea_pio_coverage_from_manifest(manifest)

    assert coverage.expected_occurrence_count == 319
    assert coverage.schedule_count == 319
    assert coverage.initial_actual_count == 319
    assert coverage.previous_as_known_count == 319
    assert coverage.revision_count == 626
    assert coverage.exact_minute_count == 318
    assert len(coverage.artifact_sha256s) == 334
    assert coverage.is_complete_for(profile.by_key[BEA_PIO_PROGRAM_KEY])


def test_first_month_and_price_boundary_preserve_source_lineage() -> None:
    first = _publication("2000-01-31")
    june = _publication("2000-08-01")
    february_2002 = _publication("2002-03-29")

    first_pce = next(
        item
        for item in first.measures
        if item.value.measure_key == BEA_PIO_CURRENT_DOLLAR_PCE
    )
    assert first_pce.value.reference_period == "1999-12"
    assert first_pce.previous_artifact_uri == BEA_PIO_PREDECESSOR_URI
    assert first_pce.previous_as_known_value == 0.5
    assert first_pce.revised_previous_value == 0.7

    june_prices = tuple(
        item
        for item in june.measures
        if item.value.measure_key
        in {BEA_PIO_HEADLINE_PRICE, BEA_PIO_CORE_PRICE}
    )
    assert {item.value.reference_period for item in june_prices} == {"2000-06"}
    assert all(
        item.value.value_basis is BeaPioValueBasis.DERIVED_FROM_INDEX_LEVELS
        for item in june_prices
    )
    february_prices = tuple(
        item
        for item in february_2002.measures
        if item.value.measure_key
        in {BEA_PIO_HEADLINE_PRICE, BEA_PIO_CORE_PRICE}
    )
    assert all(
        item.value.value_basis is BeaPioValueBasis.SOURCE_PUBLISHED
        for item in february_prices
    )
    assert {item.comparison_basis for item in february_prices} == {
        "basis-transition"
    }


def test_shutdown_publications_keep_split_and_revision_only_values() -> None:
    march_2019 = _publication("2019-03-01")
    april_2019 = _publication("2019-04-29")
    update = _publication("2025-12-23")
    combined = _publication("2026-01-22")

    assert [
        item.value.reference_period
        for item in march_2019.measures
        if item.value.measure_key == BEA_PIO_PERSONAL_INCOME
    ] == ["2018-12", "2019-01"]
    assert [
        item.value.reference_period
        for item in april_2019.measures
        if item.value.measure_key == BEA_PIO_CURRENT_DOLLAR_PCE
    ] == ["2019-02", "2019-03"]
    assert len(update.measures) == 12
    assert all(
        item.value.release_stage is EconomicReleaseStage.REVISION
        for item in update.measures
    )
    assert update.observation.release_time_locator == "archive-index:datetime"
    assert update.observation.support_format is OfficialSourceFormat.XLSX
    assert len(combined.measures) == 8
    assert {item.value.reference_period for item in combined.measures} == {
        "2025-10",
        "2025-11",
    }


def test_time_and_archive_anomalies_remain_explicit() -> None:
    manifest = load_packaged_bea_pio_archive_manifest()
    date_only = _publication("2000-02-28").observation
    mismatched_zones = {
        (item.observation.release_date, item.observation.reported_zone)
        for item in manifest.publications
        if item.observation.zone_consistent is False
    }

    assert date_only.time_precision is EconomicTimePrecision.DATE_ONLY
    assert date_only.release_time_locator == "archive-index:date-only"
    assert mismatched_zones == {
        ("2007-11-30", "EDT"),
        ("2008-03-28", "EST"),
        ("2008-11-26", "EDT"),
        ("2019-10-31", "EST"),
    }
    assert BEA_PIO_ARCHIVE_DATE_CORRECTIONS == {
        "https://www.bea.gov/news/2019/personal-income-and-outlays-december-2018-personal-income-january-2019": "2019-03-01"
    }


def test_manifest_rejects_identity_tampering() -> None:
    manifest = load_packaged_bea_pio_archive_manifest()

    with pytest.raises(ValueError, match="identity differs"):
        replace(manifest, profile_id=manifest.profile_id + "-tampered")
