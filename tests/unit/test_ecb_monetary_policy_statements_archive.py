"""Complete-archive qualification tests for ECB policy statements."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_MONETARY_POLICY_STATEMENTS_PROGRAM_KEY,
    EcbArtifactRole,
    EcbMonetaryPolicyStatementsArchiveManifestV1,
    build_ecb_statement_requests,
    load_packaged_ecb_monetary_policy_archive_manifest,
    load_packaged_ecb_monetary_policy_statements_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_monetary_policy_statements_manifest_path,
)


def _manifest() -> EcbMonetaryPolicyStatementsArchiveManifestV1:
    return load_packaged_ecb_monetary_policy_statements_archive_manifest()


def test_packaged_ecb_statements_quantify_the_exact_foedb_series() -> None:
    manifest = _manifest()

    assert ECB_MONETARY_POLICY_STATEMENTS_PROGRAM_KEY == (
        "ea.ecb.monetary-policy-statements"
    )
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert manifest.release_index.database_version == "1789563196"
    assert manifest.release_index.database_version_hash == "FS9roLbU"
    assert manifest.release_index.database_total_records == 20_073
    assert len(manifest.release_index.database_artifacts) == 83
    assert len(manifest.release_index.publications) == 270
    assert len(manifest.statements) == 270
    assert manifest.raw_artifact_count == 353
    assert manifest.unique_content_sha256_count == 353
    assert manifest.total_content_bytes == 48_734_109
    assert manifest.exact_minute_count == 74
    assert manifest.date_only_count == 196
    assert manifest.linked_decision_count == 270
    assert manifest.decision_publication_count == 299
    assert manifest.decision_without_statement_count == 29
    assert manifest.manifest_id == (
        "ecb-statement-archive-manifest:sha256:"
        "aae4d588f93df5fa4ed4fcc77db825463839d6abb1a1e7cbc7670fa7ea723bb0"
    )
    assert manifest.decision_archive_manifest_id == (
        "ecb-archive-manifest:sha256:"
        "a8a9ffafd6f7686f3c08fa194c59e1a601b9e69feb5f68be4ed901c4373de007"
    )
    assert packaged_ecb_monetary_policy_statements_manifest_path().is_file()


def test_ecb_statements_preserve_title_and_heading_eras() -> None:
    manifest = _manifest()

    assert Counter(
        item.source_title for item in manifest.release_index.publications
    ) == {
        "Introductory statement with Q&A": 124,
        "Introductory statement to the press conference (with Q&A)": 99,
        "Monetary policy statement (with Q&A)": 42,
        "Introductory statement with the Q&A": 3,
        "Introductory statement to the press conference (with Q& A)": 1,
        "Transcript of the Press Briefing": 1,
    }
    assert Counter(item.source_heading for item in manifest.statements) == {
        "PRESS CONFERENCE": 76,
        "Introductory statement with Q&A": 67,
        "Introductory statement to the press conference (with Q&A)": 65,
        "Introductory statement": 33,
        "ECB Press conference: Introductory statement": 13,
        "Introductory statement to the press conference": 11,
        "Introductory statement with the Q&A": 3,
        "Introductory statement to the press conference (with Q& A)": 1,
        "Transcript of the Press Briefing": 1,
    }
    assert all(
        item.artifact.role is EcbArtifactRole.STATEMENT_HTML
        for item in manifest.statements
    )
    assert (
        len({item.artifact.source_uri for item in manifest.statements}) == 270
    )
    assert manifest.statements[0].release_date == "2000-01-05"
    assert manifest.statements[-1].release_date == "2026-09-10"


def test_ecb_statements_link_one_to_one_and_preserve_true_absences() -> None:
    manifest = _manifest()
    decisions = load_packaged_ecb_monetary_policy_archive_manifest().decisions
    decisions_by_id = {item.publication_id: item for item in decisions}
    linked = [
        item.linked_decision_publication_id for item in manifest.statements
    ]
    missing_dates = {
        decisions_by_id[item].release_date
        for item in manifest.decision_without_statement_publication_ids
    }

    assert len(set(linked)) == 270
    assert set(linked).isdisjoint(
        manifest.decision_without_statement_publication_ids
    )
    assert set(linked) | set(
        manifest.decision_without_statement_publication_ids
    ) == (set(decisions_by_id))
    assert missing_dates == {
        "2000-01-20",
        "2000-02-17",
        "2000-03-16",
        "2000-04-27",
        "2000-05-25",
        "2000-06-21",
        "2000-07-20",
        "2000-08-03",
        "2000-08-31",
        "2000-11-16",
        "2000-11-30",
        "2001-01-04",
        "2001-01-18",
        "2001-02-15",
        "2001-03-15",
        "2001-03-29",
        "2001-04-26",
        "2001-05-23",
        "2001-07-19",
        "2001-08-02",
        "2001-09-13",
        "2001-09-17",
        "2001-09-27",
        "2001-10-25",
        "2002-08-01",
        "2003-07-31",
        "2004-08-05",
        "2005-08-04",
        "2008-10-08",
    }


def test_ecb_statement_times_preserve_placeholder_and_clock_eras() -> None:
    publications = {
        item.release_date: item
        for item in _manifest().release_index.publications
    }

    assert publications["2017-06-08"].published_lexical == (
        "2017-06-08T14:45+02:00"
    )
    assert publications["2017-07-20"].published_at_ns is None
    assert publications["2017-07-20"].published_lexical is None
    assert publications["2017-09-07"].published_lexical == (
        "2017-09-07T14:45+02:00"
    )
    assert publications["2022-06-09"].published_lexical == (
        "2022-06-09T14:45+02:00"
    )
    assert publications["2022-07-21"].published_lexical == (
        "2022-07-21T15:00+02:00"
    )
    assert publications["2026-09-10"].published_lexical == (
        "2026-09-10T15:00+02:00"
    )


def test_ecb_statement_requests_bind_every_selected_uri_once() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    requests = build_ecb_statement_requests(registry, manifest.release_index)

    assert len(requests) == 270
    assert len({item.uri for item in requests}) == 270
    assert {item.parser_id for item in requests} == {
        "official.ecb-monetary-policy.v1"
    }
    assert {item.source_format.value for item in requests} == {"html"}


def test_ecb_statement_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EcbMonetaryPolicyStatementsArchiveManifestV1.from_json(
            manifest.to_json()
        )
        == manifest
    )
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(
            manifest, linked_decision_count=manifest.linked_decision_count - 1
        )
    with pytest.raises(ValueError, match="overlaps absences"):
        replace(
            manifest,
            decision_without_statement_publication_ids=(
                manifest.statements[0].linked_decision_publication_id,
                *manifest.decision_without_statement_publication_ids,
            ),
            decision_publication_count=manifest.decision_publication_count + 1,
            decision_without_statement_count=(
                manifest.decision_without_statement_count + 1
            ),
        )
