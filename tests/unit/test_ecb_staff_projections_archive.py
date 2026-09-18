"""Complete-archive qualification tests for ECB staff projections."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_STAFF_PROJECTIONS_PROGRAM_KEY,
    EcbArtifactRole,
    EcbProjectionProducer,
    EcbStaffProjectionsArchiveManifestV1,
    EconomicForecastScope,
    build_ecb_projection_requests,
    load_packaged_ecb_staff_projections_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_staff_projections_manifest_path,
)


def _manifest() -> EcbStaffProjectionsArchiveManifestV1:
    return load_packaged_ecb_staff_projections_archive_manifest()


def test_packaged_ecb_projections_quantify_the_exact_foedb_series() -> None:
    manifest = _manifest()

    assert ECB_STAFF_PROJECTIONS_PROGRAM_KEY == "ea.ecb.staff-projections"
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert manifest.release_index.database_version == "1789563196"
    assert manifest.release_index.database_version_hash == "FS9roLbU"
    assert manifest.release_index.database_total_records == 20_073
    assert len(manifest.release_index.database_artifacts) == 83
    assert len(manifest.archive_index.entries) == 90
    assert len(manifest.release_index.publications) == 90
    assert len(manifest.projections) == 90
    assert manifest.raw_artifact_count == 205
    assert manifest.unique_content_sha256_count == 205
    assert manifest.total_content_bytes == 43_393_009
    assert manifest.projection_count == 90
    assert manifest.ecb_staff_count == 45
    assert manifest.eurosystem_staff_count == 45
    assert manifest.pdf_artifact_count == 90
    assert manifest.html_artifact_count == 31
    assert manifest.exact_minute_count == 36
    assert manifest.date_only_count == 54
    assert manifest.manifest_id == (
        "ecb-projection-archive-manifest:sha256:"
        "b06de82a39fb24e287a88ce1bbd3247a50929b0f31a5b62a914db101fb6e66de"
    )
    assert packaged_ecb_staff_projections_manifest_path().is_file()


def test_ecb_projection_archive_and_foedb_reconcile_one_to_one() -> None:
    manifest = _manifest()
    archive = {
        item.projection_round: item for item in manifest.archive_index.entries
    }
    foedb = {
        item.projection_round: item
        for item in manifest.release_index.publications
    }

    assert archive.keys() == foedb.keys()
    assert next(iter(archive)) == "2004-06"
    assert next(reversed(archive)) == "2026-09"
    for projection_round, entry in archive.items():
        publication = foedb[projection_round]
        assert entry.release_date == publication.release_date
        assert entry.source_title == publication.source_title
        assert entry.producer is publication.producer
        assert entry.source_uri in publication.document_uris


def test_ecb_projections_preserve_producer_format_and_horizon_scope() -> None:
    manifest = _manifest()

    assert Counter(item.producer for item in manifest.projections) == {
        EcbProjectionProducer.ECB_STAFF: 45,
        EcbProjectionProducer.EUROSYSTEM_STAFF: 45,
    }
    assert Counter(
        item.horizon_end_year - item.horizon_start_year
        for item in manifest.projections
    ) == {1: 29, 2: 51, 3: 10}
    assert Counter(
        item.horizon_source_format.value for item in manifest.projections
    ) == {"pdf": 59, "html": 31}
    assert all(
        item.forecast_scope is EconomicForecastScope.CENTRAL_BANK_PROJECTION
        and item.economy_code == "EA"
        and item.target_concepts == ("hicp-inflation", "real-gdp-growth")
        for item in manifest.projections
    )
    assert all(
        Counter(artifact.role for artifact in item.artifacts)[
            EcbArtifactRole.PROJECTION_PDF
        ]
        == 1
        for item in manifest.projections
    )


def test_ecb_projection_rounds_keep_early_release_and_clock_eras() -> None:
    manifest = _manifest()
    publications = {
        item.projection_round: item
        for item in manifest.release_index.publications
    }
    projections = {item.projection_round: item for item in manifest.projections}

    assert publications["2006-09"].release_date == "2006-08-31"
    assert publications["2017-09"].published_at_ns is None
    assert publications["2017-09"].published_lexical is None
    assert publications["2017-12"].published_lexical == (
        "2017-12-14T15:30+01:00"
    )
    assert publications["2022-06"].published_lexical == (
        "2022-06-09T15:30+02:00"
    )
    assert publications["2022-09"].published_lexical == (
        "2022-09-08T15:45+02:00"
    )
    assert publications["2026-09"].published_lexical == (
        "2026-09-10T15:45+02:00"
    )
    assert (
        projections["2004-06"].horizon_start_year,
        projections["2004-06"].horizon_end_year,
    ) == (2004, 2005)
    assert (
        projections["2025-12"].horizon_start_year,
        projections["2025-12"].horizon_end_year,
    ) == (2025, 2028)


def test_ecb_projection_requests_bind_every_selected_artifact_once() -> None:
    registry = load_packaged_official_source_registry()
    requests = build_ecb_projection_requests(
        registry, _manifest().release_index
    )

    assert len(requests) == 121
    assert len({item.uri for item in requests}) == 121
    assert Counter(item.source_format.value for item in requests) == {
        "pdf": 90,
        "html": 31,
    }
    assert {item.parser_id for item in requests} == {
        "official.ecb-monetary-policy.v1"
    }


def test_ecb_projection_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EcbStaffProjectionsArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(manifest, projection_count=manifest.projection_count - 1)
    with pytest.raises(ValueError, match="annual horizon is invalid"):
        replace(
            manifest.projections[0],
            horizon_end_year=manifest.projections[0].horizon_start_year,
        )
