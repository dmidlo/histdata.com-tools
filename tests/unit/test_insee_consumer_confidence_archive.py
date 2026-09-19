"""Qualification tests for the official INSEE consumer-confidence archive."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from histdatacom.market_context import (
    INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID,
    INSEE_CONSUMER_CONFIDENCE_CURRENT_START_PERIOD,
    INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD,
    INSEE_CONSUMER_CONFIDENCE_PARSER_ID,
    INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY,
    INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS,
    INSEE_CONSUMER_CONFIDENCE_RELEASE_START_PERIOD,
    INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI,
    INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY,
    INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID,
    INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD,
    InseeConsumerConfidenceArchiveManifestV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_insee_consumer_confidence_catalog_request,
    build_insee_consumer_confidence_current_series_request,
    build_insee_consumer_confidence_release_requests,
    build_insee_consumer_confidence_search_request,
    load_packaged_insee_consumer_confidence_archive_manifest,
    load_packaged_official_source_registry,
    packaged_insee_consumer_confidence_manifest_path,
)


def _manifest() -> InseeConsumerConfidenceArchiveManifestV1:
    return load_packaged_insee_consumer_confidence_archive_manifest()


def _release(manifest: InseeConsumerConfidenceArchiveManifestV1, period: str):
    return next(
        item for item in manifest.releases if item.reference_period == period
    )


def test_packaged_insee_consumer_confidence_archive_quantifies_lineage() -> (
    None
):
    manifest = _manifest()

    assert INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY == (
        "fr.insee.consumer-confidence"
    )
    assert INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY == (
        "fr.insee.consumer-confidence"
    )
    assert INSEE_CONSUMER_CONFIDENCE_RELEASE_START_PERIOD == "2009-06"
    assert INSEE_CONSUMER_CONFIDENCE_CURRENT_START_PERIOD == "2000-01"
    assert INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD == "2026-08"
    assert INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS == (
        "2009-08",
        "2010-08",
        "2011-08",
        "2012-08",
    )
    assert manifest.search.as_of_date == "2026-08-31"
    assert manifest.search.total_result_count == 215
    assert len(manifest.search.exclusions) == 12
    assert len(manifest.releases) == 203
    assert manifest.raw_artifact_count == 206
    assert manifest.unique_content_sha256_count == 206
    assert manifest.total_content_bytes == 38_294_506
    assert manifest.released_value_count == 202
    assert manifest.compatible_comparison_count == 93
    assert packaged_insee_consumer_confidence_manifest_path().is_file()


def test_insee_consumer_confidence_registry_entry_is_verified() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY)

    assert source.parser_id == INSEE_CONSUMER_CONFIDENCE_PARSER_ID
    assert source.verification_status is (
        OfficialSourceVerificationStatus.EMPIRICALLY_VERIFIED
    )
    assert source.reviewed_on == "2026-09-18"
    assert source.roles == (
        OfficialSourceRole.OFFICIAL_ARCHIVE,
        OfficialSourceRole.RELEASE_CALENDAR,
    )
    assert OfficialSourceRole.PRIMARY_PRODUCER not in source.roles
    assert source.formats == (
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.SDMX_21,
    )
    assert source.source_series_ids == (
        INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID,
    )
    assert source.source_table_ids == ("102414547",)


def test_inventory_accounts_for_gaps_and_every_search_result() -> None:
    manifest = _manifest()
    gaps = set(INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS)
    expected = [
        f"{year}-{month:02d}"
        for year in range(2009, 2027)
        for month in range(1, 13)
        if "2009-06" <= f"{year}-{month:02d}" <= "2026-08"
        and f"{year}-{month:02d}" not in gaps
    ]

    assert Counter(item.reason for item in manifest.search.exclusions) == {
        "missing-subtitle": 11,
        "other-consumer-confidence-product": 1,
    }
    assert (
        len(manifest.search.selected_document_ids)
        + len(manifest.search.exclusions)
        == manifest.search.total_result_count
    )
    assert [item.reference_period for item in manifest.releases] == expected
    assert tuple(item.source_document_id for item in manifest.releases) == (
        manifest.search.selected_document_ids
    )
    assert manifest.coverage_gaps == INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS
    assert manifest.releases[0].source_document_id == 1_561_597
    assert manifest.releases[-1].source_document_id == 9_038_311


def test_values_cover_source_layouts_methods_and_withdrawal() -> None:
    manifest = _manifest()
    arithmetic = _release(manifest, "2009-06")
    factor = _release(manifest, "2011-01")
    composite = _release(manifest, "2017-07")
    withdrawn = _release(manifest, INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD)
    current = _release(manifest, "2026-08")

    assert arithmetic.value is not None
    assert (arithmetic.value.value, arithmetic.value.methodology) == (
        -37.0,
        "arithmetic-mean",
    )
    assert arithmetic.value.locator.startswith("table:")
    assert factor.value is not None
    assert (factor.value.value, factor.value.methodology) == (
        85.0,
        "factor-analysis",
    )
    assert composite.value is not None
    assert (composite.value.value, composite.value.methodology) == (
        104.0,
        "normalized-synthetic-index",
    )
    assert current.value is not None
    assert (current.value.value, current.value.methodology) == (
        86.0,
        "factor-analysis",
    )
    assert withdrawn.source_document_id == (
        INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID
    )
    assert withdrawn.value is None
    assert withdrawn.value_unavailable_reason == (
        "withdrawn-after-significant-error"
    )
    assert withdrawn.replacement_source_uri == (
        INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI
    )
    values = tuple(
        item.value for item in manifest.releases if item.value is not None
    )
    assert {item.metric for item in values} == {
        "household-confidence-summary-indicator"
    }
    assert {item.unit for item in values} == {
        "normalized-index-long-term-average-100"
    }
    assert {item.adjustment for item in values} == {"seasonally-adjusted"}
    assert {item.geography_scope for item in values} == {"france"}


def test_current_series_and_comparisons_keep_latest_state_explicit() -> None:
    manifest = _manifest()
    series = manifest.current_series

    assert series.series_id == INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID
    assert series.title == (
        "Monthly consumer confidence survey - Summary indicator of households' "
        "confidence (first factor among the opinion survey balances) - SA data"
    )
    assert series.semantics == "latest-revised-cross-check"
    assert series.methodology == "factor-analysis"
    assert len(series.observations) == 320
    assert (
        series.observations[0].reference_period,
        series.observations[0].value,
    ) == ("2000-01", 120.0)
    assert (
        series.observations[-1].reference_period,
        series.observations[-1].value,
    ) == ("2026-08", 86.0)
    assert series.last_update == "2026-08-25"
    assert manifest.comparisons[0].to_dict() == {
        "schema_version": (
            "histdatacom.insee-consumer-confidence-comparison.v1"
        ),
        "reference_period": "2011-01",
        "methodology": "factor-analysis",
        "release_value": 85.0,
        "current_value": 90.0,
        "delta": 5.0,
    }
    assert manifest.comparisons[-1].reference_period == "2026-08"
    assert manifest.comparisons[-1].delta == 0.0


def test_requests_bind_exact_methods_formats_and_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    search = build_insee_consumer_confidence_search_request(registry)
    catalog = build_insee_consumer_confidence_catalog_request(registry)
    series = build_insee_consumer_confidence_current_series_request(registry)
    releases = build_insee_consumer_confidence_release_requests(
        registry,
        ({"id": item.source_document_id} for item in manifest.releases),
    )

    assert search.method is OfficialRequestMethod.POST
    assert search.source_format is OfficialSourceFormat.JSON
    assert json.loads(search.body_text or "")["q"] == "consumer confidence"
    assert catalog.method is OfficialRequestMethod.POST
    assert catalog.source_format is OfficialSourceFormat.JSON
    assert json.loads(catalog.body_text or "") == {
        "facetsField": [],
        "filters": [{"field": "bdm_idFamille", "values": ["102414547"]}],
        "q": "summary indicator",
        "rows": 1000,
        "sortFields": [],
        "start": 0,
    }
    assert series.source_format is OfficialSourceFormat.SDMX_21
    assert len(releases) == 203
    assert len({item.uri for item in releases}) == 203
    assert {item.source_format for item in releases} == {
        OfficialSourceFormat.HTML
    }
    assert {
        item.parser_id for item in (search, catalog, series, *releases)
    } == {INSEE_CONSUMER_CONFIDENCE_PARSER_ID}


def test_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        InseeConsumerConfidenceArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["value"]["value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        InseeConsumerConfidenceArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    withdrawn = next(
        item
        for item in payload["releases"]
        if item["reference_period"]
        == INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD
    )
    withdrawn["replacement_source_uri"] = None
    with pytest.raises(ValueError, match="unavailable value differs"):
        InseeConsumerConfidenceArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = (
        "insee-consumer-confidence-archive-manifest:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        InseeConsumerConfidenceArchiveManifestV1.from_json(json.dumps(payload))
