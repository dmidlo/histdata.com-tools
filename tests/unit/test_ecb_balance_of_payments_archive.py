"""Qualification tests for the ECB monthly balance-of-payments archive."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_BALANCE_OF_PAYMENTS_ARCHIVE_START_PERIOD,
    ECB_BALANCE_OF_PAYMENTS_MEASURE_KEY,
    ECB_BALANCE_OF_PAYMENTS_PROGRAM_KEY,
    ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY,
    ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD,
    EcbBalanceOfPaymentsArchiveManifestV1,
    EcbBalanceOfPaymentsArtifactRole,
    OfficialSourceFormat,
    build_ecb_balance_of_payments_foedb_chunk_requests,
    build_ecb_balance_of_payments_foedb_metadata_request,
    build_ecb_balance_of_payments_foedb_versions_request,
    build_ecb_balance_of_payments_index_requests,
    build_ecb_balance_of_payments_release_requests,
    load_packaged_ecb_balance_of_payments_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_balance_of_payments_manifest_path,
)
from histdatacom.market_context.economic_calendar import EconomicTimePrecision


def _manifest() -> EcbBalanceOfPaymentsArchiveManifestV1:
    return load_packaged_ecb_balance_of_payments_archive_manifest()


def test_packaged_ecb_bop_archive_quantifies_complete_monthly_lineage() -> None:
    manifest = _manifest()

    assert ECB_BALANCE_OF_PAYMENTS_PROGRAM_KEY == (
        "ea.ecb.monthly-balance-of-payments"
    )
    assert ECB_BALANCE_OF_PAYMENTS_MEASURE_KEY == "current-account-balance"
    assert ECB_BALANCE_OF_PAYMENTS_SOURCE_KEY == ("ea.ecb.balance-of-payments")
    assert ECB_BALANCE_OF_PAYMENTS_ARCHIVE_START_PERIOD == "1999-10"
    assert ECB_LATEST_PACKAGED_BALANCE_OF_PAYMENTS_PERIOD == "2026-07"
    assert manifest.release_index.as_of_date == "2026-09-18"
    assert manifest.release_index.database_version == "1789724583"
    assert manifest.release_index.database_version_hash == "gA8XCxPS"
    assert manifest.release_index.database_total_records == 20_081
    assert len(manifest.release_index.database_artifacts) == 83
    assert len(manifest.release_index.inventory_artifacts) == 2
    assert manifest.release_count == 320
    assert manifest.legacy_release_count == 177
    assert manifest.current_release_count == 143
    assert manifest.raw_artifact_count == 925
    assert manifest.unique_content_sha256_count == 925
    assert manifest.total_content_bytes == 102_488_831
    assert manifest.published_value_count == 703
    assert manifest.component_value_count == 383
    assert manifest.revision_disclosure_count == 293
    assert manifest.manifest_id == (
        "ecb-balance-of-payments-archive-manifest:sha256:"
        "228a5ffa187efd36b5c9f263a45174a408229282389809d3e75fc11844173774"
    )
    assert packaged_ecb_balance_of_payments_manifest_path().is_file()


def test_ecb_bop_inventory_preserves_gaps_break_and_explicit_exclusions() -> (
    None
):
    manifest = _manifest()
    entries = manifest.release_index.entries
    periods = [item.reference_period for item in entries]

    assert periods[0] == "1999-10"
    assert periods[-1] == "2026-07"
    assert "2000-01" not in periods
    assert "2000-03" not in periods
    assert len(periods) == len(set(periods)) == 320
    assert Counter(item.inventory for item in entries) == {
        "legacy-index": 177,
        "current-index": 143,
    }
    assert Counter(item.bpm_era for item in entries) == {
        "BPM5": 177,
        "BPM6": 143,
    }
    assert entries[176].reference_period == "2014-08"
    assert entries[176].inventory == "legacy-index"
    assert entries[177].reference_period == "2014-09"
    assert entries[177].inventory == "current-index"
    assert Counter(
        item.reason for item in manifest.release_index.exclusions
    ) == {
        "quarterly-bop-iip": 48,
        "annual-bop-iip": 9,
    }
    assert len(manifest.release_index.support_uris) == 25


def test_ecb_bop_archive_retains_artifacts_and_publication_provenance() -> None:
    manifest = _manifest()
    artifacts = [
        artifact
        for release in manifest.releases
        for artifact in release.artifacts
    ]

    assert Counter(item.role for item in artifacts) == {
        EcbBalanceOfPaymentsArtifactRole.RELEASE_HTML: 320,
        EcbBalanceOfPaymentsArtifactRole.SUPPORT_HTML: 485,
        EcbBalanceOfPaymentsArtifactRole.SUPPORT_PDF: 36,
    }
    assert Counter(
        item.release_date_provenance for item in manifest.releases
    ) == {
        "foedb": 143,
        "document-uri": 116,
        "prior-release-schedule": 20,
        "pdf-metadata": 1,
        "unavailable": 40,
    }
    assert manifest.dated_release_count == 280
    assert manifest.unavailable_date_count == 40
    assert manifest.exact_second_count == 1
    assert manifest.exact_minute_count == 110
    assert manifest.date_only_count == 169
    assert manifest.migration_timestamp_rejection_count == 55
    assert all(len(item.content_sha256) == 64 for item in artifacts)
    assert all(item.content_length > 0 for item in artifacts)


def test_ecb_bop_timing_does_not_invent_legacy_publication_clocks() -> None:
    by_period = {
        item.entry.reference_period: item for item in _manifest().releases
    }

    first = by_period["1999-10"]
    assert first.release_date == "1999-12-29"
    assert first.release_date_provenance == "pdf-metadata"
    assert first.time_precision is EconomicTimePrecision.DATE_ONLY
    assert first.migration_timestamp == "2015-05-04"

    missing = by_period["2000-02"]
    assert missing.release_date is None
    assert missing.release_date_provenance == "unavailable"
    assert missing.time_precision is None
    assert missing.published_at_ns is None

    scheduled = by_period["2004-02"]
    assert scheduled.release_date == "2004-04-27"
    assert scheduled.release_date_provenance == "prior-release-schedule"
    assert scheduled.migration_timestamp == "2004-04-21"

    assert by_period["2003-04"].release_date is None
    assert by_period["2003-05"].release_date is None
    assert by_period["2003-04"].next_release_date == "2003-05-27"

    exact_second = by_period["2017-02"]
    assert exact_second.time_precision is EconomicTimePrecision.EXACT_SECOND
    assert exact_second.published_lexical == "2017-04-21T08:35:28+02:00"

    utc_midnight = by_period["2023-02"]
    assert utc_midnight.time_precision is EconomicTimePrecision.DATE_ONLY
    assert utc_midnight.published_at_ns is None

    latest = by_period["2026-07"]
    assert latest.release_date == "2026-09-18"
    assert latest.release_date_provenance == "foedb"
    assert latest.time_precision is EconomicTimePrecision.EXACT_MINUTE
    assert latest.published_lexical == "2026-09-18T10:00+02:00"


def test_ecb_bop_values_preserve_source_units_signs_and_composition() -> None:
    manifest = _manifest()
    by_period = {
        item.entry.reference_period: item for item in manifest.releases
    }

    assert Counter(
        value.component
        for release in manifest.releases
        for value in release.values
    ) == {
        "current-account": 320,
        "goods": 97,
        "services": 97,
        "primary-income": 94,
        "secondary-income": 95,
    }
    assert Counter(
        item.entry.euro_area_composition for item in manifest.releases
    ) == {
        "EA11": 13,
        "EA12": 72,
        "EA13": 12,
        "EA15": 12,
        "EA16": 24,
        "EA17": 36,
        "EA18": 12,
        "EA19": 96,
        "EA20": 36,
        "EA21": 7,
    }
    assert by_period["1999-10"].values[0].value == 3.6
    assert by_period["1999-10"].values[0].adjustment == (
        "not-seasonally-adjusted"
    )
    assert by_period["2014-08"].values[0].value == 18.9
    assert by_period["2014-08"].entry.euro_area_composition == "EA18"

    latest = by_period["2026-07"]
    assert latest.entry.euro_area_composition == "EA21"
    assert [(item.component, item.value) for item in latest.values] == [
        ("current-account", 28.0),
        ("goods", 36.0),
        ("services", 12.0),
        ("secondary-income", -18.0),
        ("primary-income", -2.0),
    ]
    assert {item.currency for item in latest.values} == {"EUR"}
    assert {item.scale for item in latest.values} == {"billion"}


def test_ecb_bop_requests_bind_exact_inventory_and_foedb_version() -> None:
    registry = load_packaged_official_source_registry()
    index = _manifest().release_index
    versions = build_ecb_balance_of_payments_foedb_versions_request(registry)
    metadata = build_ecb_balance_of_payments_foedb_metadata_request(
        registry,
        index.database_version,
        index.database_version_hash,
    )
    chunks = build_ecb_balance_of_payments_foedb_chunk_requests(
        registry,
        index.database_version,
        index.database_version_hash,
        total_records=index.database_total_records,
        chunk_size=index.database_chunk_size,
        chunk_group_size=index.database_chunk_group_size,
    )
    inventories = build_ecb_balance_of_payments_index_requests(registry)
    releases = build_ecb_balance_of_payments_release_requests(registry, index)

    assert versions.source_format is OfficialSourceFormat.JSON
    assert metadata.source_format is OfficialSourceFormat.JSON
    assert len(chunks) == 81
    assert len({item.uri for item in chunks}) == 81
    assert {item.source_format for item in chunks} == {
        OfficialSourceFormat.JSON
    }
    assert len(inventories) == 2
    assert {item.source_format for item in inventories} == {
        OfficialSourceFormat.HTML
    }
    assert len(releases) == 320
    assert len({item.uri for item in releases}) == 320
    assert {item.parser_id for item in (*chunks, *inventories, *releases)} == {
        "official.ecb-balance-of-payments.v1"
    }


def test_ecb_bop_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EcbBalanceOfPaymentsArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="raw-artifact count differs"):
        replace(manifest, raw_artifact_count=manifest.raw_artifact_count - 1)

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["values"][0]["reference_period"] = "1999-11"
    with pytest.raises(ValueError, match="value period differs"):
        EcbBalanceOfPaymentsArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["releases"][0]["revision_disclosure"] = "false"
    with pytest.raises(TypeError, match="must be a boolean"):
        EcbBalanceOfPaymentsArchiveManifestV1.from_json(json.dumps(payload))

    payload = json.loads(manifest.to_json())
    payload["manifest_id"] = (
        "ecb-balance-of-payments-archive-manifest:sha256:" + "0" * 64
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        EcbBalanceOfPaymentsArchiveManifestV1.from_json(json.dumps(payload))
