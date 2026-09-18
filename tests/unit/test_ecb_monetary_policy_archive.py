"""Complete-archive qualification tests for ECB policy decisions."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_FOEDB_VERSIONS_URI,
    ECB_MONETARY_POLICY_SOURCE_KEY,
    EcbMonetaryPolicyArchiveManifestV1,
    OfficialEcbMonetaryPolicyParserV1,
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_ecb_decision_requests,
    build_ecb_foedb_chunk_requests,
    build_ecb_foedb_metadata_request,
    build_ecb_foedb_versions_request,
    load_packaged_ecb_monetary_policy_archive_manifest,
    load_packaged_official_source_registry,
    parse_ecb_foedb_version,
    parse_ecb_monetary_policy_setting,
    resolve_official_source_parser,
)

CAPTURED_AT_NS = 1_789_400_000_000_000_000


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


def _html_request(uri: str) -> OfficialSourceRequestV1:
    registry = load_packaged_official_source_registry()
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _values(setting) -> dict[str, float]:
    return {item.component_key: item.value for item in setting.components}


def test_ecb_archive_source_has_a_dedicated_empirical_parser() -> None:
    registry = load_packaged_official_source_registry()
    source = registry.source(ECB_MONETARY_POLICY_SOURCE_KEY)

    assert source.economy_code == "EA"
    assert source.verification_status.value == "empirically-verified"
    assert source.source_release_ids == (
        "Account of the monetary policy meeting",
        "ECB staff macroeconomic projections for the euro area",
        "Eurosystem staff macroeconomic projections for the euro area",
        "FOEDB type 20: Meeting of <source-authored dates>",
        "Introductory statement to the press conference (with Q&A)",
        "Introductory statement with Q&A",
        "Monetary Policy Decisions",
        "Monetary developments in the euro area",
        "Monetary policy decisions",
        "Monetary policy statement (with Q&A)",
        "Transcript of the Press Briefing",
    )
    assert source.formats == (
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    )
    assert isinstance(
        resolve_official_source_parser(source),
        OfficialEcbMonetaryPolicyParserV1,
    )


def test_packaged_ecb_archive_quantifies_the_complete_decision_series() -> None:
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()

    assert isinstance(manifest, EcbMonetaryPolicyArchiveManifestV1)
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert manifest.release_index.database_version == "1789563196"
    assert manifest.release_index.database_version_hash == "FS9roLbU"
    assert manifest.release_index.database_total_records == 20_073
    assert len(manifest.release_index.database_artifacts) == 83
    assert len(manifest.release_index.publications) == 300
    assert len(manifest.decisions) == 299
    assert manifest.raw_artifact_count == 383
    assert manifest.unique_content_sha256_count == 383
    assert manifest.total_content_bytes == 42_121_110
    assert manifest.exact_minute_count == 72
    assert manifest.date_only_count == 227
    assert manifest.emergency_count == 2
    assert manifest.manifest_id == (
        "ecb-archive-manifest:sha256:"
        "578d0d3f223d9317266d5b54aceb0fd3689c05f062d1b019d82e49587f3f795b"
    )


def test_ecb_decisions_preserve_three_rate_lineage_and_order_changes() -> None:
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()
    first = manifest.decisions[0]
    february_2000 = next(
        item for item in manifest.decisions if item.release_date == "2000-02-03"
    )
    framework_change = next(
        item for item in manifest.decisions if item.release_date == "2024-09-12"
    )
    last = manifest.decisions[-1]

    assert _values(manifest.predecessor.setting) == {
        "deposit-facility": 2.0,
        "main-refinancing-operations": 3.0,
        "marginal-lending-facility": 4.0,
    }
    assert _values(first.previous_setting) == _values(first.new_setting)
    assert _values(february_2000.new_setting) == {
        "deposit-facility": 2.25,
        "main-refinancing-operations": 3.25,
        "marginal-lending-facility": 4.25,
    }
    assert _values(framework_change.new_setting) == {
        "deposit-facility": 3.5,
        "main-refinancing-operations": 3.65,
        "marginal-lending-facility": 3.9,
    }
    assert _values(last.new_setting) == {
        "deposit-facility": 2.5,
        "main-refinancing-operations": 2.65,
        "marginal-lending-facility": 2.9,
    }
    assert all(
        current.previous_setting.semantic_id == previous.new_setting.semantic_id
        for previous, current in zip(
            manifest.decisions[:-1], manifest.decisions[1:], strict=True
        )
    )
    assert Counter(item.direction.value for item in manifest.decisions) == {
        "hold": 238,
        "ease": 32,
        "tighten": 29,
    }


def test_ecb_foedb_times_fail_closed_before_the_exact_minute_era() -> None:
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()
    entries = manifest.release_index.publications
    first_exact = next(item for item in entries if item.exact_minute)
    last_date_only = entries[entries.index(first_exact) - 1]
    latest = entries[-1]

    assert last_date_only.release_date == "2017-09-07"
    assert last_date_only.published_at_ns is None
    assert last_date_only.published_lexical is None
    assert first_exact.release_date == "2017-10-26"
    assert first_exact.published_lexical == "2017-10-26T13:45+02:00"
    assert latest.published_lexical == "2026-09-10T14:15+02:00"
    assert [
        item.release_date
        for item in manifest.decisions
        if item.action_timing.value == "emergency"
    ] == ["2001-09-17", "2008-10-08"]


def test_ecb_decision_selection_preserves_the_2016_capitalization_variant() -> (
    None
):
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()
    publication = next(
        item
        for item in manifest.release_index.publications
        if item.release_date == "2016-12-08"
    )
    decision = next(
        item for item in manifest.decisions if item.release_date == "2016-12-08"
    )

    assert publication.title == "Monetary Policy Decisions"
    assert publication.source_uri.endswith("/2016/html/pr161208.en.html")
    assert _values(decision.previous_setting) == _values(decision.new_setting)


def test_ecb_requests_bind_the_complete_versioned_inventory() -> None:
    registry = load_packaged_official_source_registry()
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()
    versions = build_ecb_foedb_versions_request(registry)

    assert versions.uri == ECB_FOEDB_VERSIONS_URI
    version_snapshot = _snapshot(
        versions,
        b'[{"version":"1789563196","hash":"FS9roLbU"}]',
        content_type="application/json",
    )
    assert parse_ecb_foedb_version(version_snapshot) == (
        "1789563196",
        "FS9roLbU",
    )
    metadata = build_ecb_foedb_metadata_request(
        registry, "1789563196", "FS9roLbU"
    )
    chunks = build_ecb_foedb_chunk_requests(
        registry,
        "1789563196",
        "FS9roLbU",
        total_records=20_073,
        chunk_size=250,
        chunk_group_size=1_000,
    )
    reports = build_ecb_decision_requests(registry, manifest.release_index)

    assert metadata.uri.endswith("/1789563196/FS9roLbU/metadata.json")
    assert len(chunks) == 81
    assert chunks[0].uri.endswith("/data/0/chunk_0.json")
    assert chunks[-1].uri.endswith("/data/0/chunk_80.json")
    assert len(reports) == 300
    assert {item.parser_id for item in (*chunks, *reports)} == {
        "official.ecb-monetary-policy.v1"
    }


def test_ecb_parser_handles_both_source_rate_orders_and_per_cent() -> None:
    first_uri = (
        "https://www.ecb.europa.eu/press/pr/date/2020/html/ecb.mp201210.en.html"
    )
    first = _snapshot(
        _html_request(first_uri),
        b"""<html><body><main><h1>Monetary policy decisions</h1>
        <p>The interest rate on the main refinancing operations and the
        interest rates on the marginal lending facility and the deposit
        facility will remain unchanged at 0.00 per cent, 0.25 per cent and
        -0.50 per cent respectively.</p></main></body></html>""",
        content_type="text/html",
    )
    second_uri = (
        "https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp260910.en.html"
    )
    second = _snapshot(
        _html_request(second_uri),
        b"""<html><body><main><h1>Monetary policy decisions</h1>
        <p>The interest rates on the deposit facility, the main refinancing
        operations and the marginal lending facility will be increased to
        2.50%, 2.65% and 2.90% respectively.</p></main></body></html>""",
        content_type="text/html",
    )

    assert _values(parse_ecb_monetary_policy_setting(first)) == {
        "deposit-facility": -0.5,
        "main-refinancing-operations": 0.0,
        "marginal-lending-facility": 0.25,
    }
    assert _values(parse_ecb_monetary_policy_setting(second)) == {
        "deposit-facility": 2.5,
        "main-refinancing-operations": 2.65,
        "marginal-lending-facility": 2.9,
    }


def test_ecb_manifest_round_trip_and_tamper_detection() -> None:
    manifest = load_packaged_ecb_monetary_policy_archive_manifest()

    assert (
        EcbMonetaryPolicyArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="manifest identity differs"):
        replace(manifest, manifest_id="ecb-archive-manifest:sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="exact-minute count differs"):
        replace(manifest, exact_minute_count=manifest.exact_minute_count - 1)
