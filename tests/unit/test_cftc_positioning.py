from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import replace
from datetime import date, datetime, time, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from histdatacom.data_analytics.cli import build_parser
from histdatacom.market_context.positioning import (
    CFTC_2025_BACKLOG_URI,
    CFTC_HISTORICAL_ARCHIVE_TEMPLATE,
    CFTC_HISTORICAL_ARCHIVES,
    CFTC_HISTORICAL_COMPRESSED_URI,
    CFTC_LEGACY_DATASET_ID,
    CFTC_POSITIONING_ADAPTER_VERSION,
    CFTC_PRE_METADATA_TEMPLATE,
    CFTC_PRE_RESOURCE_TEMPLATE,
    CFTC_RELEASE_SCHEDULE_URI,
    CFTC_SPECIAL_ANNOUNCEMENTS_URI,
    CFTC_TFF_DATASET_ID,
    CFTC_WEB_POLICY_URI,
    CME_EURGBP_RULE_URI,
    CME_FX_QUOTE_URI,
    CftcAvailabilityConfidence,
    CftcMappingKind,
    CftcPositioningConsumer,
    CftcPositioningConsumerBindingV1,
    CftcPositioningCorpusBuildV1,
    CftcPositioningFetchProfileV1,
    CftcPositioningBenchmarkSmokeV1,
    CftcPositioningPreflightV1,
    CftcPositioningQueryStatus,
    CftcPositioningQueryV1,
    CftcPositioningRawSourceV1,
    CftcReportFamily,
    CftcReportScope,
    CftcRestatementStatus,
    apply_cftc_positioning_to_benchmark_events,
    apply_cftc_positioning_to_motif_condition,
    bind_cftc_positioning_query,
    build_cftc_positioning_benchmark_smoke,
    build_cftc_positioning_corpus_from_sources,
    cftc_positioning_information_inputs,
    compare_cftc_positioning_corpora,
    preflight_cftc_positioning_corpus,
    query_cftc_positioning_corpus,
    read_cftc_positioning_corpus,
    replay_cftc_positioning_corpus,
    require_cftc_positioning_corpus,
    validate_cftc_positioning_consumer_binding,
    write_cftc_positioning_benchmark_smoke,
    write_cftc_positioning_consumer_binding,
    write_cftc_positioning_corpus,
)
from histdatacom.synthetic import (
    InformationSplitKind,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionInformationSplitV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    audit_reconstruction_information,
    reconstruction_information_window_plan_id,
)
from histdatacom.synthetic.benchmark import BenchmarkEventV1
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import ReferenceMotifConditionV1

RETRIEVED_AT_NS = 1_767_225_600_000_000_000


def _profile() -> CftcPositioningFetchProfileV1:
    return CftcPositioningFetchProfileV1(
        start_date="2005-01-01",
        end_date="2025-12-23",
        page_size=1000,
        max_pages_per_dataset=2,
        max_staleness_days=14,
    )


def _source(
    key: str,
    kind: str,
    uri: str,
    content: bytes,
    *,
    dataset_id: str | None = None,
    family: CftcReportFamily | None = None,
    scope: CftcReportScope | None = None,
    parameters: dict[str, str] | None = None,
    redistribution_allowed: bool = True,
) -> CftcPositioningRawSourceV1:
    return CftcPositioningRawSourceV1(
        source_key=key,
        source_kind=kind,
        source_uri=uri,
        retrieved_at_ns=RETRIEVED_AT_NS,
        content=content,
        content_type=(
            "application/zip"
            if kind == "historical_archive"
            else (
                "application/json"
                if kind in {"pre_data", "pre_metadata"}
                else "text/html"
            )
        ),
        query_parameters=parameters or {},
        dataset_id=dataset_id,
        report_family=family,
        report_scope=scope,
        redistribution_allowed=redistribution_allowed,
        limitations=("fixture source",),
    )


def _oi(
    family: CftcReportFamily,
    scope: CftcReportScope,
    code: str,
    report_date: str,
) -> int:
    return (
        (1000 if family is CftcReportFamily.LEGACY else 2000)
        + (100 if scope is CftcReportScope.COMBINED else 0)
        + int(code[-2:])
        + int(report_date[-2:])
    )


def _row(
    family: CftcReportFamily,
    scope: CftcReportScope,
    code: str,
    report_date: str,
) -> dict[str, str]:
    row = {
        "id": f"{family.value}-{scope.value}-{code}-{report_date}",
        "report_date_as_yyyy_mm_dd": f"{report_date}T00:00:00.000",
        "cftc_contract_market_code": code,
        "futonly_or_combined": (
            "FutOnly" if scope is CftcReportScope.FUTURES_ONLY else "Combined"
        ),
        "contract_market_name": f"CONTRACT {code}",
        "market_and_exchange_names": f"MARKET {code}",
        "open_interest_all": str(_oi(family, scope, code, report_date)),
    }
    if family is CftcReportFamily.LEGACY:
        row.update(
            {
                "noncomm_positions_long_all": str(500 + int(report_date[-2:])),
                "noncomm_positions_short_all": "300",
                "comm_positions_long_all": "250",
                "comm_positions_short_all": "350",
            }
        )
    else:
        row.update(
            {
                "lev_money_positions_long_all": str(
                    600 + int(report_date[-2:])
                ),
                "lev_money_positions_short_all": "275",
                "dealer_positions_long_all": "200",
                "dealer_positions_short_all": "325",
            }
        )
    return row


def _pre_parameters(profile: CftcPositioningFetchProfileV1) -> dict[str, str]:
    quoted_codes = ",".join(f"'{item}'" for item in profile.contract_codes)
    return {
        "$where": (
            f"cftc_contract_market_code in ({quoted_codes}) "
            "AND report_date_as_yyyy_mm_dd between "
            f"'{profile.start_date}T00:00:00.000' and "
            f"'{profile.end_date}T23:59:59.999'"
        ),
        "$order": (
            "report_date_as_yyyy_mm_dd,cftc_contract_market_code,futonly_or_combined,id"
        ),
        "$limit": str(profile.page_size),
        "$offset": "0",
    }


def _archive_content(
    family: CftcReportFamily,
    scope: CftcReportScope,
    *,
    market_name: str = "MARKET 099741",
) -> bytes:
    report_date = "2014-06-10"
    csv_content = (
        "CFTC_Contract_Market_Code,As_of_Date_In_Form_YYYYMMDD,"
        "Open_Interest_All,Market_and_Exchange_Names\n"
        f"099741,{report_date},{_oi(family, scope, '099741', report_date)},"
        f'"{market_name}"\n'
    ).encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("history.txt", csv_content)
    return buffer.getvalue()


def _sources(
    *,
    conflicting_duplicate: bool = False,
    identical_duplicate: bool = False,
) -> tuple[CftcPositioningRawSourceV1, ...]:
    profile = _profile()
    values: list[CftcPositioningRawSourceV1] = []
    report_dates = (
        "2005-01-04",
        "2010-05-18",
        "2013-12-31",
        "2014-06-10",
        "2015-06-16",
        "2015-06-30",
        "2025-09-30",
        "2025-10-07",
    )
    for dataset_id, family in (
        (CFTC_LEGACY_DATASET_ID, CftcReportFamily.LEGACY),
        (CFTC_TFF_DATASET_ID, CftcReportFamily.TFF),
    ):
        values.append(
            _source(
                f"pre.{dataset_id}.metadata",
                "pre_metadata",
                CFTC_PRE_METADATA_TEMPLATE.format(dataset_id=dataset_id),
                json.dumps({"id": dataset_id}).encode(),
                dataset_id=dataset_id,
                family=family,
            )
        )
        rows = [
            _row(family, scope, code, report_date)
            for report_date in report_dates
            for scope in (
                CftcReportScope.FUTURES_ONLY,
                CftcReportScope.COMBINED,
            )
            for code in ("096742", "099741", "299741")
            if code != "299741" or report_date >= "2014-06-10"
            if family is not CftcReportFamily.TFF or report_date >= "2006-06-13"
        ]
        if family is CftcReportFamily.LEGACY and (
            conflicting_duplicate or identical_duplicate
        ):
            duplicate = dict(rows[0])
            duplicate["id"] = "duplicate-row"
            if conflicting_duplicate:
                duplicate["open_interest_all"] = "999999"
            rows.append(duplicate)
        values.append(
            _source(
                f"pre.{dataset_id}.page-0000",
                "pre_data",
                CFTC_PRE_RESOURCE_TEMPLATE.format(dataset_id=dataset_id),
                json.dumps(rows, sort_keys=True).encode(),
                dataset_id=dataset_id,
                family=family,
                parameters=_pre_parameters(profile),
            )
        )
    official = (
        (
            "official.release-schedule",
            "release_schedule",
            CFTC_RELEASE_SCHEDULE_URI,
            b"Commitments of Traders release schedule",
        ),
        (
            "official.special-announcements",
            "special_announcements",
            CFTC_SPECIAL_ANNOUNCEMENTS_URI,
            b"Commitments of Traders historical special announcements",
        ),
        (
            "official.historical-compressed-index",
            "historical_index",
            CFTC_HISTORICAL_COMPRESSED_URI,
            b"CFTC historical compressed files",
        ),
        (
            "official.2025-backlog",
            "backlog_schedule",
            CFTC_2025_BACKLOG_URI,
            b"2025 CFTC Commitments of Traders publication dates",
        ),
        (
            "official.web-policy",
            "license_policy",
            CFTC_WEB_POLICY_URI,
            b"CFTC web policy public domain",
        ),
        (
            "cme.fx-quote-conventions",
            "quote_orientation",
            CME_FX_QUOTE_URI,
            b"CME FX quote conventions",
        ),
        (
            "cme.eurgbp-rule-301",
            "quote_orientation",
            CME_EURGBP_RULE_URI,
            b"CME EURGBP Rule 301",
        ),
    )
    for key, kind, uri, content in official:
        values.append(
            _source(
                key,
                kind,
                uri,
                content,
                redistribution_allowed=not key.startswith("cme."),
            )
        )
    for family_text, scope_text, archive_name in CFTC_HISTORICAL_ARCHIVES:
        family = CftcReportFamily(family_text)
        scope = CftcReportScope(scope_text)
        values.append(
            _source(
                f"official.archive.{archive_name.lower()}",
                "historical_archive",
                CFTC_HISTORICAL_ARCHIVE_TEMPLATE.format(
                    archive_name=archive_name
                ),
                _archive_content(family, scope),
                family=family,
                scope=scope,
            )
        )
    return tuple(values)


@pytest.fixture  # type: ignore[untyped-decorator]
def corpus_build() -> CftcPositioningCorpusBuildV1:
    return build_cftc_positioning_corpus_from_sources(
        _sources(), profile=_profile()
    )


def _window_ns(value: str) -> int:
    return int(
        datetime.combine(
            date.fromisoformat(value), time(), tzinfo=timezone.utc
        ).timestamp()
        * 1_000_000_000
    )


def test_build_keeps_family_scope_and_archive_evidence_separate(
    corpus_build: CftcPositioningCorpusBuildV1,
) -> None:
    corpus = corpus_build.corpus
    assert len(corpus.snapshots) == 80
    assert len(corpus.archive_consistency) == 4
    assert all(
        item.selected_row_count == 1 for item in corpus.archive_consistency
    )
    assert all(
        item.matched_pre_rows == 1 for item in corpus.archive_consistency
    )
    assert all(
        item.open_interest_mismatch_count == 0
        for item in corpus.archive_consistency
    )
    assert any(item.missing_week_count > 0 for item in corpus.coverage)
    assert all(
        item["adapter_version"] == CFTC_POSITIONING_ADAPTER_VERSION
        for item in corpus.sources
    )
    mappings = {
        (item.symbol, item.mapping_kind.value): item
        for item in corpus.symbol_mappings
    }
    assert mappings[("EURUSD", "direct")].quote_convention == "USD per EUR"
    assert mappings[("GBPUSD", "direct")].quote_convention == "USD per GBP"
    assert mappings[("EURGBP", "direct")].quote_convention == "GBP per EUR"
    keys = {item.logical_key for item in corpus.snapshots}
    assert "legacy:futures_only:099741:2025-10-07" in keys
    assert "tff:combined:099741:2025-10-07" in keys


def test_duplicate_rows_are_counted_and_conflicts_fail_closed() -> None:
    build = build_cftc_positioning_corpus_from_sources(
        _sources(identical_duplicate=True), profile=_profile()
    )
    assert build.corpus.duplicate_key_count == 1
    with pytest.raises(ValueError, match="conflicting CFTC rows"):
        build_cftc_positioning_corpus_from_sources(
            _sources(conflicting_duplicate=True), profile=_profile()
        )


def test_archive_name_changes_and_family_transition_remain_explicit() -> None:
    sources = list(_sources())
    for index, source in enumerate(sources):
        if (
            source.source_kind == "historical_archive"
            and source.report_family is CftcReportFamily.LEGACY
            and source.report_scope is CftcReportScope.FUTURES_ONLY
        ):
            sources[index] = replace(
                source,
                content=_archive_content(
                    CftcReportFamily.LEGACY,
                    CftcReportScope.FUTURES_ONLY,
                    market_name="HISTORICAL MARKET NAME",
                ),
            )
            break
    corpus = build_cftc_positioning_corpus_from_sources(
        sources, profile=_profile()
    ).corpus
    evidence = next(
        item
        for item in corpus.archive_consistency
        if item.report_family is CftcReportFamily.LEGACY
        and item.report_scope is CftcReportScope.FUTURES_ONLY
    )
    assert evidence.contract_name_change_count == 1
    start = _window_ns("2005-01-05")
    transition = query_cftc_positioning_corpus(
        corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY, CftcReportFamily.TFF),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert transition.status is CftcPositioningQueryStatus.MISSING
    assert "tff:not-yet-published" in transition.reason


def test_release_evidence_distinguishes_delayed_nominal_and_corrected(
    corpus_build: CftcPositioningCorpusBuildV1,
) -> None:
    by_date = {
        item.report_date: item
        for item in corpus_build.corpus.snapshots
        if item.report_family is CftcReportFamily.LEGACY
        and item.report_scope is CftcReportScope.FUTURES_ONLY
        and item.contract_code == "099741"
    }
    delayed = by_date["2025-10-07"].release_evidence
    assert delayed.confidence is CftcAvailabilityConfidence.VERIFIED
    expected = datetime(
        2025, 11, 21, 15, 30, tzinfo=ZoneInfo("America/New_York")
    )
    assert delayed.publication_at_ns == int(
        expected.timestamp() * 1_000_000_000
    )
    nominal = by_date["2014-06-10"].release_evidence
    assert nominal.confidence is CftcAvailabilityConfidence.NOMINAL
    expected_nominal = datetime(
        2014, 6, 13, 15, 30, tzinfo=ZoneInfo("America/New_York")
    )
    assert nominal.publication_at_ns == int(
        expected_nominal.timestamp() * 1_000_000_000
    )
    corrected = by_date["2010-05-18"]
    assert (
        corrected.release_evidence.confidence
        is CftcAvailabilityConfidence.RESTATEMENT_QUALIFIED
    )
    assert (
        corrected.restatement_status
        is CftcRestatementStatus.RESTATED_CURRENT_STATE
    )
    premature = by_date["2015-06-16"].release_evidence
    assert (
        premature.confidence is CftcAvailabilityConfidence.CORRECTION_QUALIFIED
    )
    holiday = by_date["2015-06-30"].release_evidence
    assert holiday.confidence is CftcAvailabilityConfidence.CORRECTION_QUALIFIED
    assert holiday.restatement_detected_at_ns == int(
        datetime(
            2015, 7, 6, 15, 30, tzinfo=ZoneInfo("America/New_York")
        ).timestamp()
        * 1_000_000_000
    )


def test_ex_post_query_and_pre_2014_eurgbp_leg_mapping(
    corpus_build: CftcPositioningCorpusBuildV1,
) -> None:
    corpus = corpus_build.corpus
    start = _window_ns("2025-10-08")
    query = query_cftc_positioning_corpus(
        corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
    )
    assert query.status is CftcPositioningQueryStatus.READY
    assert len(query.snapshots) == 12
    assert query.mapping_kinds["EURGBP"] == CftcMappingKind.DIRECT.value
    assert query.derived_values
    assert any(key.endswith(".net_change") for key in query.derived_values)

    historical = query_cftc_positioning_corpus(
        corpus,
        start_ns=_window_ns("2014-01-02"),
        end_ns=_window_ns("2014-01-03"),
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("EURGBP",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert historical.status is CftcPositioningQueryStatus.READY
    assert (
        historical.mapping_kinds["EURGBP"]
        == CftcMappingKind.DERIVED_TWO_LEG.value
    )
    assert {item.contract_code for item in historical.snapshots} == {
        "096742",
        "099741",
    }

    stale_start = _window_ns("2025-11-01")
    stale = query_cftc_positioning_corpus(
        corpus,
        start_ns=stale_start,
        end_ns=stale_start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert stale.status is CftcPositioningQueryStatus.STALE
    assert len(stale.snapshots) == 1
    assert next(iter(stale.age_seconds.values())) > 14 * 86_400
    assert "age_seconds=" in stale.reason


def test_strict_ex_ante_fails_on_time_and_current_state_then_accepts_vintage(
    corpus_build: CftcPositioningCorpusBuildV1,
) -> None:
    corpus = corpus_build.corpus
    start = _window_ns("2025-11-24")
    before_release = query_cftc_positioning_corpus(
        corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        as_of_ns=_window_ns("2025-11-18"),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
        max_staleness_days=60,
    )
    assert before_release.status is CftcPositioningQueryStatus.NOT_AVAILABLE

    current_state = query_cftc_positioning_corpus(
        corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        as_of_ns=_window_ns("2025-11-22"),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
        max_staleness_days=60,
    )
    assert (
        current_state.status
        is CftcPositioningQueryStatus.RESTATEMENT_INCOMPLETE
    )

    snapshots = tuple(
        (
            replace(
                item,
                restatement_status=CftcRestatementStatus.ORIGINAL_VERIFIED,
                snapshot_id="",
            )
            if item.report_date == "2025-10-07"
            else item
        )
        for item in corpus.snapshots
    )
    vintage_corpus = replace(corpus, snapshots=snapshots, corpus_id="")
    vintage = query_cftc_positioning_corpus(
        vintage_corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        as_of_ns=_window_ns("2025-11-22"),
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
        max_staleness_days=60,
    )
    assert vintage.status is CftcPositioningQueryStatus.READY
    assert all(
        value == 0.0
        for key, value in vintage.derived_values.items()
        if key.endswith((".net_change", ".rolling_52w_zscore"))
    )

    nominal_start = _window_ns("2014-06-14")
    nominal_query = query_cftc_positioning_corpus(
        corpus,
        start_ns=nominal_start,
        end_ns=nominal_start + 3_600_000_000_000,
        as_of_ns=nominal_start,
        information_mode=InformationMode.EX_ANTE_SIMULATION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert nominal_query.status is CftcPositioningQueryStatus.NOT_AVAILABLE


def test_content_addressed_write_read_replay_and_diff(
    corpus_build: CftcPositioningCorpusBuildV1, tmp_path: Path
) -> None:
    artifacts = write_cftc_positioning_corpus(corpus_build, tmp_path)
    corpus_path = Path(artifacts["corpus"].path)
    loaded = read_cftc_positioning_corpus(corpus_path)
    replayed = replay_cftc_positioning_corpus(corpus_path)
    assert loaded.corpus_id == corpus_build.corpus.corpus_id
    assert replayed.corpus.corpus_id == corpus_build.corpus.corpus_id
    diff = compare_cftc_positioning_corpora(loaded, replayed.corpus)
    assert not diff.added_keys
    assert not diff.removed_keys
    assert not diff.changed_keys
    assert not list(tmp_path.rglob("*.tmp-*"))

    bad_path = tmp_path / f"cftc-positioning-corpus-{'0' * 64}.json"
    bad_path.write_bytes(corpus_path.read_bytes())
    with pytest.raises(ValueError, match="hash differs"):
        read_cftc_positioning_corpus(bad_path)


def test_information_and_all_consumer_seams(
    corpus_build: CftcPositioningCorpusBuildV1, tmp_path: Path
) -> None:
    start = _window_ns("2025-10-08")
    query = query_cftc_positioning_corpus(
        corpus_build.corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    provisional_inputs = cftc_positioning_information_inputs(
        query,
        run_id="run-468",
        used_at_ns=start,
        split_kind=InformationSplitKind.VALIDATION,
    )
    assert len(provisional_inputs) == 1
    assert provisional_inputs[0].allowed_lookahead_ns > 0
    policy = ReconstructionInformationPolicyV1(
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        max_allowed_lookahead_ns=provisional_inputs[0].allowed_lookahead_ns,
    )
    run = ReconstructionRunV1(
        symbols=("EURUSD",),
        source_version_ids=("source:held-out",),
        configuration_ids=(policy.policy_id, "cftc:fixture"),
        ensemble_member_ids=("member-000",),
        base_seed=468,
    )
    window = ReconstructionWindowV1(
        run_id=run.run_id,
        ensemble_member_id="member-000",
        symbols=run.symbols,
        core_start_ns=start,
        core_end_ns=start + 3_600_000_000_000,
    )
    inputs = cftc_positioning_information_inputs(
        query,
        run_id=run.run_id,
        used_at_ns=start,
        split_kind=InformationSplitKind.VALIDATION,
    )
    report_ns = inputs[0].event_time_ns
    day_ns = 86_400_000_000_000
    splits = (
        ReconstructionInformationSplitV1(
            InformationSplitKind.TRAIN,
            report_ns - 30 * day_ns,
            report_ns - 20 * day_ns,
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.CALIBRATION,
            report_ns - 20 * day_ns,
            report_ns - 10 * day_ns,
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.VALIDATION,
            report_ns - 10 * day_ns,
            start + 3_600_000_000_000,
        ),
    )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id((window,)),
        inputs=inputs,
        splits=splits,
    )
    audit = audit_reconstruction_information(
        manifest, policy, run=run, windows=(window,)
    )
    assert audit.accepted, [item.to_dict() for item in audit.findings]

    benchmark_binding = bind_cftc_positioning_query(
        query,
        consumer=CftcPositioningConsumer.BENCHMARK,
        consumer_artifact_id="benchmark-468",
        run_id=run.run_id,
        window_id=window.window_id,
        information_inputs=inputs,
    )
    assert CftcPositioningQueryV1.from_dict(query.to_dict()) == query
    assert (
        CftcPositioningConsumerBindingV1.from_dict(benchmark_binding.to_dict())
        == benchmark_binding
    )
    assert Path(
        write_cftc_positioning_consumer_binding(
            benchmark_binding, tmp_path
        ).path
    ).exists()
    event = BenchmarkEventV1(
        source_event_id="source-1",
        symbol="EURUSD",
        event_time_ns=start,
        event_sequence=0,
        bid=1.1,
        ask=1.1002,
        epoch_id="epoch-1",
        session="london",
        event_state="ordinary",
        sparsity="dense",
    )
    projected = apply_cftc_positioning_to_benchmark_events(
        (event,), benchmark_binding
    )
    assert "cftc_positioning:weekly" in projected[0].event_state
    assert projected[0].benchmark_event_id != event.benchmark_event_id

    smoke = build_cftc_positioning_benchmark_smoke(
        query,
        benchmark_binding,
        (event,),
        source_artifact_id="held-out-month",
        source_sha256=hashlib.sha256(b"held-out").hexdigest(),
        reload_events=(event,),
    )
    assert smoke.deterministic_reload
    assert CftcPositioningBenchmarkSmokeV1.from_dict(smoke.to_dict()) == smoke
    assert Path(
        write_cftc_positioning_benchmark_smoke(smoke, tmp_path).path
    ).exists()

    motif_binding = bind_cftc_positioning_query(
        query,
        consumer=CftcPositioningConsumer.MOTIF_SELECTION,
        consumer_artifact_id="motif-468",
        run_id=run.run_id,
        window_id=window.window_id,
        information_inputs=inputs,
    )
    motif = ReferenceMotifConditionV1(
        symbol="EURUSD",
        feed_epoch_id="epoch-1",
        session_state="london",
    )
    projected_motif = apply_cftc_positioning_to_motif_condition(
        motif, motif_binding
    )
    assert motif_binding.state_label in projected_motif.event_tags
    assert projected_motif.metrics == motif.metrics
    assert motif_binding.metrics

    for consumer in (
        CftcPositioningConsumer.PLANNING,
        CftcPositioningConsumer.CARVING,
    ):
        binding = bind_cftc_positioning_query(
            query,
            consumer=consumer,
            consumer_artifact_id=f"{consumer.value}-468",
            run_id=run.run_id,
            window_id=window.window_id,
            information_inputs=inputs,
        )
        artifact = SimpleNamespace(
            batch_id=f"{consumer.value}-468",
            run_id=run.run_id,
            window_id=window.window_id,
        )
        validate_cftc_positioning_consumer_binding(artifact, binding)


def test_preflight_and_cli_surface(
    corpus_build: CftcPositioningCorpusBuildV1,
) -> None:
    start = _window_ns("2025-10-08")
    decision = require_cftc_positioning_corpus(
        corpus_build.corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert decision.ready
    assert CftcPositioningPreflightV1.from_dict(decision.to_dict()) == decision
    unsupported = preflight_cftc_positioning_corpus(
        corpus_build.corpus,
        start_ns=start,
        end_ns=start + 3_600_000_000_000,
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        symbols=("USDJPY",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
    )
    assert not unsupported.ready
    assert "unsupported positioning symbols: USDJPY" in unsupported.reasons
    args = build_parser().parse_args(
        [
            "cftc-positioning-corpus",
            "--artifact-dir",
            "artifacts",
            "--start-date",
            "2010-01-01",
            "--end-date",
            "2025-12-31",
            "--previous-corpus",
            "prior.json",
        ]
    )
    assert args.analytics_command == "cftc-positioning-corpus"
    assert args.previous_corpus == "prior.json"
