"""Qualification tests for the official ECB SPF archive."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_SPF_FIRST_ROUND,
    ECB_SPF_ISSUE_WINDOW_FIRST_ROUND,
    ECB_SPF_LATEST_PACKAGED_ROUND,
    ECB_SPF_PARSER_ID,
    ECB_SPF_PROGRAM_KEY,
    ECB_SPF_SOURCE_KEY,
    EcbSpfArchiveManifestV1,
    EcbSpfAvailabilityPrecision,
    EcbSpfConcept,
    OfficialEcbSpfParserV1,
    OfficialRawSnapshotV1,
    OfficialSourceCapability,
    OfficialSourceFormat,
    OfficialSourceRole,
    build_ecb_spf_round_requests,
    build_ecb_spf_support_requests,
    load_packaged_ecb_spf_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_spf_manifest_path,
    parse_ecb_spf_report_dates,
    parse_ecb_spf_round,
    parse_ecb_spf_round_index,
    resolve_official_source_parser,
)


def _snapshot(
    request, content: bytes, content_type: str
) -> OfficialRawSnapshotV1:
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1_789_689_600_000_000_000,
        completed_at_ns=1_789_689_600_000_000_001,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": content_type},
        content=content,
        content_type=content_type,
    )


def _periods(first: str, last: str) -> tuple[str, ...]:
    year, quarter = int(first[:4]), int(first[-1])
    end = int(last[:4]), int(last[-1])
    result = []
    while (year, quarter) <= end:
        result.append(f"{year}-Q{quarter}")
        quarter += 1
        if quarter == 5:
            year += 1
            quarter = 1
    return tuple(result)


def _index_snapshot() -> OfficialRawSnapshotV1:
    request = build_ecb_spf_support_requests(
        load_packaged_official_source_registry()
    )[0]
    links = "".join(
        f'<a href="table_3_{period[:4]}q{period[-1]}.en.html">{period}</a>'
        for period in _periods(
            ECB_SPF_FIRST_ROUND, ECB_SPF_LATEST_PACKAGED_ROUND
        )
    )
    return _snapshot(request, f"<html>{links}</html>".encode(), "text/html")


def test_packaged_spf_manifest_quantifies_the_complete_archive() -> None:
    manifest = load_packaged_ecb_spf_archive_manifest()

    assert ECB_SPF_PROGRAM_KEY == "ea.ecb.survey-of-professional-forecasters"
    assert manifest.as_of_date == "2026-09-18"
    assert manifest.raw_artifact_count == 115
    assert manifest.unique_content_sha256_count == 115
    assert manifest.total_content_bytes == 15_124_041
    assert manifest.round_count == 111
    assert manifest.issue_window_round_count == 107
    assert manifest.exact_date_count == 47
    assert manifest.survey_round_only_count == 64
    assert manifest.forecast_count == 2_130
    assert manifest.headline_hicp_forecast_count == 630
    assert manifest.core_hicp_forecast_count == 240
    assert manifest.real_gdp_forecast_count == 630
    assert manifest.unemployment_forecast_count == 630
    assert manifest.microdata_member_count == 111
    assert manifest.manifest_id == (
        "ecb-spf-archive-manifest:sha256:"
        "d2d5e0410f7a518a44d3dfc452d69a5bcc80606d7f782e562f225a98c1c84558"
    )
    assert packaged_ecb_spf_manifest_path().is_file()


def test_spf_registry_entry_is_verified_and_dedicated() -> None:
    source = load_packaged_official_source_registry().source(ECB_SPF_SOURCE_KEY)

    assert source.economy_code == "EA"
    assert source.reviewed_on == "2026-09-18"
    assert source.roles == (OfficialSourceRole.OFFICIAL_ARCHIVE,)
    assert source.formats == (
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.ARCHIVE,
        OfficialSourceFormat.PDF,
    )
    assert source.parser_id == ECB_SPF_PARSER_ID
    assert {
        OfficialSourceCapability.FORECAST_SURVEY,
        OfficialSourceCapability.HISTORICAL_VINTAGES,
        OfficialSourceCapability.ARCHIVE_ENUMERATION,
    } <= set(source.capabilities)
    assert isinstance(
        resolve_official_source_parser(source), OfficialEcbSpfParserV1
    )


def test_round_lineage_preserves_evidence_precision_and_concepts() -> None:
    manifest = load_packaged_ecb_spf_archive_manifest()
    first = manifest.rounds[0]
    first_exact = manifest.rounds[64]
    latest = manifest.rounds[-1]

    assert [item.survey_period for item in manifest.rounds] == list(
        _periods(ECB_SPF_FIRST_ROUND, ECB_SPF_LATEST_PACKAGED_ROUND)
    )
    assert ECB_SPF_ISSUE_WINDOW_FIRST_ROUND == "2000-Q1"
    assert (
        first.survey_period,
        first.availability_precision,
        first.availability_date,
        first.page_metadata_date,
    ) == (
        "1999-Q1",
        EcbSpfAvailabilityPrecision.SURVEY_ROUND_ONLY,
        None,
        "2023-11-07",
    )
    assert (
        first_exact.survey_period,
        first_exact.availability_precision,
        first_exact.availability_date,
        first_exact.page_metadata_date,
    ) == (
        "2015-Q1",
        EcbSpfAvailabilityPrecision.EXACT_DATE,
        "2015-01-23",
        "2024-10-08",
    )
    assert (latest.survey_period, latest.availability_date) == (
        "2026-Q3",
        "2026-07-24",
    )
    assert Counter(item.concept for item in first.forecasts) == {
        EcbSpfConcept.HICP_INFLATION: 5,
        EcbSpfConcept.REAL_GDP_GROWTH: 5,
        EcbSpfConcept.UNEMPLOYMENT_RATE: 5,
    }
    assert {item.concept for item in latest.forecasts} == set(EcbSpfConcept)
    assert (
        next(
            item.survey_period
            for item in manifest.rounds
            if any(
                forecast.concept is EcbSpfConcept.CORE_HICP_INFLATION
                for forecast in item.forecasts
            )
        )
        == "2016-Q4"
    )
    assert (
        first.forecasts[0].horizon_label,
        first.forecasts[0].mean_lexical,
        first.forecasts[0].mean_value,
    ) == ("1999", "1.0", 1.0)


def test_index_and_report_parsers_require_gap_free_official_inventories() -> (
    None
):
    registry = load_packaged_official_source_registry()
    index_snapshot = _index_snapshot()
    indexed = parse_ecb_spf_round_index(index_snapshot)
    requests = build_ecb_spf_round_requests(registry, index_snapshot)

    assert len(indexed) == len(requests) == 111
    assert indexed[0][0] == ECB_SPF_FIRST_ROUND
    assert indexed[-1][0] == ECB_SPF_LATEST_PACKAGED_ROUND
    assert [item.page_number for item in requests] == list(range(1, 112))
    assert {item.source_format for item in requests} == {
        OfficialSourceFormat.HTML
    }

    report_request = build_ecb_spf_support_requests(registry)[1]
    dates = _periods("2015-Q1", ECB_SPF_LATEST_PACKAGED_ROUND)
    reports = "".join(
        f'<dt isodate="{period[:4]}-01-23"><a href="ecb.spf{period[:4]}q{period[-1]}.en.html">report</a></dt>'
        for period in dates
    )
    parsed = parse_ecb_spf_report_dates(
        _snapshot(
            report_request, f"<html>{reports}</html>".encode(), "text/html"
        )
    )
    assert len(parsed) == 47
    assert parsed["2015-Q1"] == "2015-01-23"


def test_round_parser_keeps_source_labels_without_inferred_dates() -> None:
    request = build_ecb_spf_round_requests(
        load_packaged_official_source_registry(), _index_snapshot()
    )[0]
    rows = """
      <tr><th></th><th>1999</th><th>2000</th></tr>
      <tr><th>Mean point estimate</th><td>1.0</td><td>1.3</td></tr>
      <tr><th>Standard deviation</th><td>0.2</td><td>0.4</td></tr>
      <tr><th>Number of replies</th><td>52</td><td>51</td></tr>
    """
    html = (
        '<html><meta property="article:published_time" content="2023-11-07">'
        "<h1>1999 Q1</h1>"
        f"<h2>HICP inflation forecasts</h2><table>{rows}</table>"
        f"<h2>Real GDP growth forecasts</h2><table>{rows}</table>"
        f"<h2>Unemployment rate forecasts</h2><table>{rows}</table>"
        "</html>"
    ).encode()

    round_ = parse_ecb_spf_round(
        _snapshot(request, html, "text/html"), report_dates={}
    )

    assert round_.availability_date is None
    assert round_.page_metadata_date == "2023-11-07"
    assert len(round_.forecasts) == 6
    assert round_.forecasts[0].source_locator == (
        "table:1:mean-point-estimate:column:1"
    )
    assert round_.forecasts[0].reply_count == 52


def test_manifest_round_trip_and_tamper_detection() -> None:
    manifest = load_packaged_ecb_spf_archive_manifest()

    assert EcbSpfArchiveManifestV1.from_json(manifest.to_json()) == manifest

    payload = json.loads(manifest.to_json())
    payload["rounds"][0]["forecasts"][0]["mean_value"] = 99.0
    with pytest.raises(ValueError, match="lexical and numeric values differ"):
        EcbSpfArchiveManifestV1.from_json(json.dumps(payload))

    with pytest.raises(ValueError, match="identity differs"):
        EcbSpfArchiveManifestV1.from_dict(
            {
                **manifest.to_dict(),
                "manifest_id": "ecb-spf-archive-manifest:sha256:" + "0" * 64,
            }
        )

    with pytest.raises(ValueError, match="date and precision differ"):
        replace(manifest.rounds[0], availability_date="1999-01-01")
