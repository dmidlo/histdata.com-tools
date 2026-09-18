"""Complete-archive qualification tests for ECB monthly M3 releases."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY,
    ECB_MONETARY_DEVELOPMENTS_PROGRAM_KEY,
    EcbArtifactRole,
    EcbMonetaryDevelopmentsArchiveManifestV1,
    OfficialSourceFormat,
    build_ecb_monetary_developments_requests,
    load_packaged_ecb_monetary_developments_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_monetary_developments_manifest_path,
)


def _manifest() -> EcbMonetaryDevelopmentsArchiveManifestV1:
    return load_packaged_ecb_monetary_developments_archive_manifest()


def test_packaged_ecb_money_archive_quantifies_the_complete_foedb_series() -> (
    None
):
    manifest = _manifest()

    assert ECB_MONETARY_DEVELOPMENTS_PROGRAM_KEY == (
        "ea.ecb.monetary-developments"
    )
    assert ECB_MONETARY_DEVELOPMENTS_MEASURE_KEY == "m3-annual-growth-rate"
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert manifest.release_index.database_version == "1789563196"
    assert manifest.release_index.database_version_hash == "FS9roLbU"
    assert manifest.release_index.database_total_records == 20_073
    assert len(manifest.release_index.database_artifacts) == 83
    assert manifest.release_index.type_record_count == 333
    assert manifest.release_index.pre_window_excluded_count == 12
    assert manifest.release_index.post_boundary_excluded_count == 0
    assert manifest.release_count == 320
    assert manifest.raw_artifact_count == 427
    assert manifest.unique_content_sha256_count == 427
    assert manifest.total_content_bytes == 62_669_957
    assert manifest.pdf_artifact_count == 248
    assert manifest.html_artifact_count == 96
    assert manifest.exact_minute_count == 109
    assert manifest.date_only_count == 211
    assert manifest.adjustment_disclosure_count == 302
    assert manifest.revision_occurrence_count == 126
    assert manifest.reference_period_inferred_count == 1
    assert manifest.release_date_correction_count == 2
    assert manifest.manifest_id == (
        "ecb-monetary-developments-archive-manifest:sha256:"
        "94ddc1754f33c1ffaab2c66084943cd23b4f21cf9bde52ebdf120aec54c09eec"
    )
    assert packaged_ecb_monetary_developments_manifest_path().is_file()


def test_ecb_money_archive_preserves_monthly_continuity_and_date_corrections() -> (
    None
):
    manifest = _manifest()
    index = manifest.release_index
    corrected = [
        item
        for item in index.publications
        if item.release_date_corrected_from_document
    ]

    assert index.predecessor.release_date == "1999-12-28"
    assert index.predecessor.reference_period == "1999-11"
    assert index.publications[0].release_date == "2000-01-28"
    assert index.publications[0].reference_period == "1999-12"
    assert index.publications[-1].release_date == "2026-08-27"
    assert index.publications[-1].reference_period == "2026-07"
    assert [
        (item.release_date, item.reference_period) for item in corrected
    ] == [
        ("2002-01-28", "2001-12"),
        ("2002-11-28", "2002-10"),
    ]
    assert len({item.release_date for item in index.publications}) == 320
    assert len({item.reference_period for item in index.publications}) == 320


def test_ecb_money_archive_preserves_timing_format_and_reference_eras() -> None:
    manifest = _manifest()
    selected = manifest.release_index.selected_publications
    in_window = manifest.release_index.publications
    format_counts = Counter(
        tuple(sorted(uri.rsplit(".", 1)[-1] for uri in item.document_uris))
        for item in in_window
    )
    inferred = [item for item in selected if not item.reference_period_from_uri]
    first_exact = next(item for item in in_window if item.exact_minute)
    last_date_only = in_window[in_window.index(first_exact) - 1]

    assert format_counts == {
        ("pdf",): 224,
        ("html",): 73,
        ("html", "pdf"): 23,
    }
    assert len(inferred) == 1
    assert inferred[0].release_date == "2010-02-25"
    assert inferred[0].reference_period == "2010-01"
    assert inferred[0].document_uris[0].endswith("/m3release.pdf")
    assert last_date_only.release_date == "2017-07-27"
    assert last_date_only.published_at_ns is None
    assert first_exact.release_date == "2017-08-28"
    assert first_exact.published_lexical == "2017-08-28T10:00+02:00"


def test_ecb_money_m3_values_reconstruct_previous_as_known_revisions() -> None:
    manifest = _manifest()
    by_date = {item.release_date: item for item in manifest.releases}
    predecessor = manifest.predecessor

    assert predecessor.reference_period == "1999-11"
    assert predecessor.actual_lexical == "6.2"
    assert predecessor.actual_value == 6.2

    first = by_date["2000-01-28"]
    assert first.reference_period == "1999-12"
    assert first.actual_value == 6.4
    assert first.previous_first_published_value == 6.2
    assert first.previous_as_known_value == 6.2
    assert first.revision_value == 0.0

    revised = by_date["2000-02-25"]
    assert revised.reference_period == "2000-01"
    assert revised.actual_value == 5.0
    assert revised.previous_first_published_value == 6.4
    assert revised.previous_as_known_value == 6.2
    assert revised.revision_value == -0.2
    assert "revised downwards from 6.4%" in revised.source_excerpt

    methodology_break = by_date["2001-11-27"]
    assert methodology_break.previous_first_published_value == 7.6
    assert methodology_break.previous_as_known_value == 6.9
    assert methodology_break.revision_value == -0.7

    inferred = by_date["2010-02-25"]
    assert inferred.reference_period == "2010-01"
    assert inferred.actual_value == 0.1
    assert inferred.previous_first_published_value == -0.2
    assert inferred.previous_as_known_value == -0.3
    assert inferred.revision_value == -0.1

    latest = by_date["2026-08-27"]
    assert latest.actual_value == 3.4
    assert latest.previous_as_known_value == 3.3
    assert latest.revision_value == 0.0


def test_ecb_money_adjustment_disclosure_and_primary_evidence_are_explicit() -> (
    None
):
    manifest = _manifest()
    releases = manifest.releases
    first_disclosure = next(
        item for item in releases if item.adjustment_disclosure
    )
    prior = releases[releases.index(first_disclosure) - 1]

    assert prior.release_date == "2001-06-29"
    assert not prior.adjustment_disclosure
    assert first_disclosure.release_date == "2001-07-26"
    assert first_disclosure.adjustment_disclosure
    assert Counter(item.parser_era for item in releases) == {
        "pdf-extracted-text": 224,
        "html-main-text": 96,
    }
    assert all(
        any(
            artifact.source_uri == item.value_source_uri
            and artifact.source_format is item.value_source_format
            for artifact in item.artifacts
        )
        for item in releases
    )
    assert any(
        len(item.artifacts) == 2
        and item.artifacts[0].role is EcbArtifactRole.MONETARY_DEVELOPMENTS_HTML
        and item.artifacts[1].role is EcbArtifactRole.MONETARY_DEVELOPMENTS_PDF
        for item in releases
    )


def test_ecb_money_requests_bind_every_selected_document_once() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    requests = build_ecb_monetary_developments_requests(
        registry, manifest.release_index
    )

    assert len(requests) == 344
    assert len({item.uri for item in requests}) == 344
    assert Counter(item.source_format for item in requests) == {
        OfficialSourceFormat.PDF: 248,
        OfficialSourceFormat.HTML: 96,
    }
    assert {item.parser_id for item in requests} == {
        "official.ecb-monetary-policy.v1"
    }


def test_ecb_money_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EcbMonetaryDevelopmentsArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(
            manifest,
            revision_occurrence_count=manifest.revision_occurrence_count - 1,
        )

    payload = json.loads(manifest.to_json())
    payload["releases"][1]["adjustment_disclosure"] = "false"
    with pytest.raises(TypeError, match="must be a boolean"):
        EcbMonetaryDevelopmentsArchiveManifestV1.from_json(json.dumps(payload))
