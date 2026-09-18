"""Complete-archive qualification tests for ECB policy meeting accounts."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

import pytest

from histdatacom.market_context import (
    ECB_MONETARY_POLICY_ACCOUNTS_PROGRAM_KEY,
    EcbAccountKind,
    EcbArtifactRole,
    EcbMonetaryPolicyAccountsArchiveManifestV1,
    build_ecb_account_requests,
    load_packaged_ecb_monetary_policy_accounts_archive_manifest,
    load_packaged_ecb_monetary_policy_archive_manifest,
    load_packaged_official_source_registry,
    packaged_ecb_monetary_policy_accounts_manifest_path,
)


def _manifest() -> EcbMonetaryPolicyAccountsArchiveManifestV1:
    return load_packaged_ecb_monetary_policy_accounts_archive_manifest()


def test_packaged_ecb_accounts_quantify_the_complete_foedb_series() -> None:
    manifest = _manifest()

    assert ECB_MONETARY_POLICY_ACCOUNTS_PROGRAM_KEY == (
        "ea.ecb.monetary-policy-accounts"
    )
    assert manifest.release_index.as_of_date == "2026-09-15"
    assert manifest.release_index.database_version == "1789563196"
    assert manifest.release_index.database_version_hash == "FS9roLbU"
    assert manifest.release_index.database_total_records == 20_073
    assert len(manifest.release_index.database_artifacts) == 83
    assert len(manifest.release_index.publications) == 96
    assert len(manifest.accounts) == 96
    assert manifest.raw_artifact_count == 179
    assert manifest.unique_content_sha256_count == 179
    assert manifest.total_content_bytes == 26_253_142
    assert manifest.exact_minute_count == 77
    assert manifest.date_only_count == 19
    assert manifest.linked_decision_count == 93
    assert manifest.exception_count == 3
    assert manifest.inferred_meeting_year_count == 1
    assert manifest.minimum_release_lag_days == 21
    assert manifest.maximum_release_lag_days == 64
    assert manifest.manifest_id == (
        "ecb-account-archive-manifest:sha256:"
        "27dbdea9925531f42c63e0073de3d10caaf3594869c7a101b1feb57af1349faa"
    )
    assert manifest.decision_archive_manifest_id == (
        "ecb-archive-manifest:sha256:"
        "c7d89e635d9c36d78bf75c558643d4a9623849aabd77bc7890e20b23c717dd22"
    )
    assert packaged_ecb_monetary_policy_accounts_manifest_path().is_file()


def test_ecb_accounts_preserve_all_title_eras_and_official_pages() -> None:
    manifest = _manifest()
    publications = manifest.release_index.publications
    title_eras = Counter(
        (
            "meeting-date"
            if item.source_title.startswith("Meeting of ")
            else item.source_title
        )
        for item in publications
    )

    assert title_eras == {
        "Account of the monetary policy meeting": 35,
        (
            "Account of the monetary policy meeting of the Governing "
            "Council of the European Central Bank"
        ): 6,
        "meeting-date": 55,
    }
    assert all(
        item.artifact.role is EcbArtifactRole.ACCOUNT_HTML
        for item in manifest.accounts
    )
    assert len({item.artifact.source_uri for item in manifest.accounts}) == 96
    assert manifest.accounts[0].release_date == "2015-02-19"
    assert manifest.accounts[-1].release_date == "2026-08-27"


def test_ecb_accounts_link_only_ordinary_policy_meetings() -> None:
    accounts = _manifest().accounts
    decisions = load_packaged_ecb_monetary_policy_archive_manifest().decisions
    decision_ids = {item.publication_id for item in decisions}
    kinds = Counter(item.account_kind for item in accounts)
    exceptions = {
        item.meeting_end_date: item
        for item in accounts
        if item.account_kind is not EcbAccountKind.MONETARY_POLICY
    }

    assert kinds == {
        EcbAccountKind.MONETARY_POLICY: 93,
        EcbAccountKind.STRATEGY_REVIEW: 2,
        EcbAccountKind.EMERGENCY_ACTION: 1,
    }
    assert set(exceptions) == {"2020-03-18", "2021-07-07", "2025-06-25"}
    assert (
        exceptions["2020-03-18"].account_kind is EcbAccountKind.EMERGENCY_ACTION
    )
    assert all(
        item.linked_decision_publication_id is None
        for item in exceptions.values()
    )
    assert all(
        item.linked_decision_publication_id in decision_ids
        for item in accounts
        if item.account_kind is EcbAccountKind.MONETARY_POLICY
    )


def test_ecb_account_meeting_dates_and_release_lags_are_source_derived() -> (
    None
):
    accounts = _manifest().accounts
    inferred = [item for item in accounts if item.meeting_year_inferred]
    minimum = [
        (item.release_date, item.meeting_end_date)
        for item in accounts
        if item.release_lag_days == 21
    ]
    maximum = next(item for item in accounts if item.release_lag_days == 64)

    assert len(inferred) == 1
    assert inferred[0].release_date == "2018-05-24"
    assert inferred[0].meeting_start_date == "2018-04-25"
    assert inferred[0].meeting_end_date == "2018-04-26"
    assert minimum == [
        ("2017-05-18", "2017-04-27"),
        ("2020-06-25", "2020-06-04"),
    ]
    assert maximum.meeting_end_date == "2025-06-25"
    assert maximum.account_kind is EcbAccountKind.STRATEGY_REVIEW


def test_ecb_account_publication_times_preserve_precision_and_exception() -> (
    None
):
    publications = _manifest().release_index.publications
    first_exact = next(item for item in publications if item.exact_minute)
    last_date_only = publications[publications.index(first_exact) - 1]
    same_day = [
        item for item in publications if item.release_date == "2025-08-28"
    ]

    assert last_date_only.release_date == "2017-05-18"
    assert last_date_only.published_at_ns is None
    assert last_date_only.published_lexical is None
    assert first_exact.release_date == "2017-07-06"
    assert first_exact.published_lexical == "2017-07-06T13:30+02:00"
    assert [item.published_lexical for item in same_day] == [
        "2025-08-28T13:25+02:00",
        "2025-08-28T13:30+02:00",
    ]


def test_ecb_account_requests_bind_every_selected_uri_once() -> None:
    registry = load_packaged_official_source_registry()
    manifest = _manifest()
    requests = build_ecb_account_requests(registry, manifest.release_index)

    assert len(requests) == 96
    assert len({item.uri for item in requests}) == 96
    assert {item.parser_id for item in requests} == {
        "official.ecb-monetary-policy.v1"
    }
    assert {item.source_format.value for item in requests} == {"html"}


def test_ecb_account_manifest_round_trip_and_tamper_detection() -> None:
    manifest = _manifest()

    assert (
        EcbMonetaryPolicyAccountsArchiveManifestV1.from_json(manifest.to_json())
        == manifest
    )
    with pytest.raises(ValueError, match="summary counts differ"):
        replace(
            manifest, linked_decision_count=manifest.linked_decision_count - 1
        )

    payload = json.loads(manifest.to_json())
    payload["accounts"][0]["meeting_year_inferred"] = "false"
    with pytest.raises(TypeError, match="must be a boolean"):
        EcbMonetaryPolicyAccountsArchiveManifestV1.from_json(
            json.dumps(payload)
        )
