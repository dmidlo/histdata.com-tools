"""Tests for the authoritative reconstruction scientific ledger."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

import histdatacom
from histdatacom.market_context import (
    CftcPositioningQueryStatus,
    CftcPositioningQueryV1,
    CftcReportFamily,
    CftcReportScope,
    MarketContextCalendarStateV1,
    MarketContextEventV1,
    MarketContextKind,
    MarketContextMissingReason,
    MarketContextPrecision,
    MarketContextQueryStatus,
    MarketContextQueryV1,
    MarketContextSourceV1,
    MarketContextView,
)
from histdatacom.reconstruction import ReconstructionClient
from histdatacom.reconstruction_science import (
    RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL,
    RECONSTRUCTION_SCIENTIFIC_NONCLAIM,
    ReconstructionConditioningStateV1,
    ReconstructionContextCompleteness,
    ReconstructionContextMissingnessCategory,
    ReconstructionScientificLedgerV1,
    classify_cftc_positioning_query,
    classify_market_context_query,
    current_histdata_reconstruction_scientific_ledger,
    read_reconstruction_scientific_ledger,
)
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.strategy_sensitivity import (
    STRATEGY_INVALID_FOR_BACKTEST_LABEL,
)

EVENT_TIME_NS = 1_657_715_400_000_000_000
DAY_NS = 86_400_000_000_000


def _calendar(*, complete: bool = True) -> MarketContextCalendarStateV1:
    return MarketContextCalendarStateV1(
        timestamp_utc_ns=EVENT_TIME_NS,
        session_state="active",
        clock_sessions=("new_york",),
        active_sessions=("new_york",),
        overlaps=(),
        special_tags=("ordinary",),
        holiday_tags=(),
        event_tags=(),
        calendar_tags=("ordinary",),
        profile_source="calendar-profile:fixture",
        profile_version="1.0.0",
        profile_complete=complete,
        limitations=("deterministic unit-test fixture",),
    )


def _missing_market_query(*, complete: bool = True) -> MarketContextQueryV1:
    return MarketContextQueryV1(
        timeline_id="market-context-timeline:fixture",
        view=MarketContextView.EX_POST,
        start_ns=EVENT_TIME_NS,
        end_ns=EVENT_TIME_NS + DAY_NS,
        as_of_ns=None,
        events=(),
        status=MarketContextQueryStatus.MISSING,
        missing_reason=(
            MarketContextMissingReason.NO_MATCHING_EVENT
            if complete
            else MarketContextMissingReason.TIMELINE_INCOMPLETE
        ),
        calendar_state=_calendar(complete=complete),
        requested_symbols=("EURUSD",),
        limitations=("deterministic unit-test fixture",),
    )


def _matched_market_query() -> MarketContextQueryV1:
    source = MarketContextSourceV1(
        name="Operator-approved fixture",
        source_version="fixture-v1",
        retrieved_at_ns=EVENT_TIME_NS + 3,
        content_sha256="a" * 64,
        adapter_name="fixture-adapter",
        adapter_version="1.0",
        license_name="Fixture license",
        redistribution_allowed=False,
        redistribution_constraints=("fixture only",),
        limitations=("fixture is not a licensed production feed",),
        source_uri="https://example.invalid/context",
        metadata={"retrieval_method": "fixture"},
    )
    event = MarketContextEventV1(
        canonical_key="us.cpi.fixture",
        kind=MarketContextKind.MACRO_RELEASE,
        title="US CPI fixture",
        source=source,
        source_event_time="2022-07-13T08:30:00-04:00",
        source_timezone="America/New_York",
        event_time_ns=EVENT_TIME_NS,
        first_known_at_ns=EVENT_TIME_NS + 1,
        available_at_ns=EVENT_TIME_NS + 2,
        pre_event_ns=0,
        post_event_ns=DAY_NS,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        confidence=0.8,
        precision=MarketContextPrecision.APPROXIMATE,
        ambiguity_reason="publication second is not retained",
        limitations=("forecast and previous fields are absent",),
        vintage_id="fixture-v1",
        actual_value=1.0,
    )
    return MarketContextQueryV1(
        timeline_id="market-context-timeline:fixture",
        view=MarketContextView.EX_POST,
        start_ns=EVENT_TIME_NS,
        end_ns=EVENT_TIME_NS + DAY_NS,
        as_of_ns=None,
        events=(event,),
        status=MarketContextQueryStatus.MATCHED,
        missing_reason=None,
        calendar_state=_calendar(),
        requested_symbols=("EURUSD",),
        limitations=("deterministic unit-test fixture",),
    )


def _limited_cftc_query() -> CftcPositioningQueryV1:
    return CftcPositioningQueryV1(
        corpus_id="cftc-positioning-corpus:fixture",
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        start_ns=EVENT_TIME_NS,
        end_ns=EVENT_TIME_NS + DAY_NS,
        as_of_ns=None,
        symbols=("EURUSD",),
        report_families=(CftcReportFamily.LEGACY,),
        report_scopes=(CftcReportScope.FUTURES_ONLY,),
        snapshots=(),
        symbol_snapshot_ids={},
        mapping_kinds={},
        derived_values={},
        status=CftcPositioningQueryStatus.PRE_COVERAGE,
        reason="window_precedes_cftc_coverage",
        age_seconds={},
    )


def test_current_ledger_freezes_exact_estimand_assumptions_and_taxonomy() -> (
    None
):
    ledger = current_histdata_reconstruction_scientific_ledger()

    assert ledger.estimand.observation_equation == r"Y=O_{\phi,e}(X)"
    assert "q_\\theta" in ledger.estimand.conditional_equation
    assert ledger.estimand.final_product_law == r"(R\circ K)_\# q_\theta"
    assert ledger.estimand.nonclaim == RECONSTRUCTION_SCIENTIFIC_NONCLAIM
    assert len(ledger.assumptions) == 7
    assert {item.category for item in ledger.context_missingness} == set(
        ReconstructionContextMissingnessCategory
    )
    assert all(
        not item.provider_row_absence_proves_no_market_event
        for item in ledger.context_missingness
    )
    assert ledger.generated_row_origin == "synthetic"
    assert ledger.generated_row_forbidden_claims == (
        "broker history",
        "observed",
        "recovered truth",
    )
    assert (
        ledger.invalid_for_backtest_label
        == STRATEGY_INVALID_FOR_BACKTEST_LABEL
        == RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
    )


def test_ledger_round_trip_public_api_and_retained_reader(
    tmp_path: Path,
) -> None:
    ledger = current_histdata_reconstruction_scientific_ledger()
    path = tmp_path / "ledger.json"
    path.write_text(ledger.to_json() + "\n", encoding="utf-8")

    assert (
        ReconstructionScientificLedgerV1.from_json(ledger.to_json()) == ledger
    )
    assert read_reconstruction_scientific_ledger(path) == ledger
    assert ReconstructionClient().scientific_ledger() == ledger
    assert ReconstructionClient().scientific_ledger(path) == ledger
    assert (
        histdatacom.current_histdata_reconstruction_scientific_ledger()
        == ledger
    )
    assert histdatacom.read_reconstruction_scientific_ledger(path) == ledger


def test_estimand_assumption_and_missingness_semantics_change_identity() -> (
    None
):
    ledger = current_histdata_reconstruction_scientific_ledger()
    changed_estimand = replace(
        ledger.estimand,
        target=ledger.estimand.target + " Changed.",
        estimand_id="",
    )
    estimand_ledger = replace(ledger, estimand=changed_estimand, ledger_id="")
    changed_assumption = replace(
        ledger.assumptions[0],
        statement=ledger.assumptions[0].statement + " Changed.",
        assumption_id="",
    )
    assumption_ledger = replace(
        ledger,
        assumptions=(changed_assumption, *ledger.assumptions[1:]),
        ledger_id="",
    )
    changed_definition = replace(
        ledger.context_missingness[0],
        statement=ledger.context_missingness[0].statement + " Changed.",
        definition_id="",
    )
    definition_ledger = replace(
        ledger,
        context_missingness=(
            changed_definition,
            *ledger.context_missingness[1:],
        ),
        ledger_id="",
    )

    assert (
        len(
            {
                ledger.ledger_id,
                estimand_ledger.ledger_id,
                assumption_ledger.ledger_id,
                definition_ledger.ledger_id,
            }
        )
        == 4
    )
    with pytest.raises(ValueError, match="assumption set is incomplete"):
        replace(ledger, assumptions=ledger.assumptions[:-1], ledger_id="")


def test_complete_no_match_and_incomplete_coverage_remain_distinct() -> None:
    complete = classify_market_context_query(_missing_market_query())
    incomplete = classify_market_context_query(
        _missing_market_query(complete=False)
    )

    assert complete.completeness is ReconstructionContextCompleteness.COMPLETE
    assert complete.categories == (
        ReconstructionContextMissingnessCategory.COMPLETE_CALENDAR_NO_MATCHING_EVENT,
    )
    assert (
        incomplete.completeness is ReconstructionContextCompleteness.INCOMPLETE
    )
    assert incomplete.categories == (
        ReconstructionContextMissingnessCategory.INCOMPLETE_CORPUS_COVERAGE,
    )
    assert complete.invalid_for_backtest_reason == (
        RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
    )
    assert (
        ReconstructionConditioningStateV1.from_dict(complete.to_dict())
        == complete
    )


def test_matched_context_preserves_field_timing_and_precision_missingness() -> (
    None
):
    state = classify_market_context_query(_matched_market_query())

    assert state.completeness is ReconstructionContextCompleteness.PARTIAL
    assert set(state.categories) == {
        ReconstructionContextMissingnessCategory.MATCHED_MISSING_CONTEMPORANEOUS_FIELDS,
        ReconstructionContextMissingnessCategory.EVENT_KNOWN_ONLY_EX_POST,
        ReconstructionContextMissingnessCategory.UNCERTAIN_FIRST_KNOWN_OR_PUBLICATION_TIME,
    }
    assert state.missing_fields == (
        "expected_value",
        "previous_value",
        "revised_previous_value",
    )


def test_cftc_limited_context_requires_explicit_qualified_unconditioned_state() -> (
    None
):
    state = classify_cftc_positioning_query(
        _limited_cftc_query(), qualified_unconditioned=True
    )

    assert state.qualified_unconditioned
    assert state.completeness is (
        ReconstructionContextCompleteness.QUALIFIED_UNCONDITIONED
    )
    assert set(state.categories) == {
        ReconstructionContextMissingnessCategory.CFTC_LIMITED_AVAILABILITY,
        ReconstructionContextMissingnessCategory.EXPLICIT_QUALIFIED_UNCONDITIONED_MODE,
    }
    assert state.invalid_for_backtest_reason == (
        RECONSTRUCTION_INVALID_FOR_BACKTEST_LABEL
    )


def test_generated_rows_cannot_serialize_observed_or_claim_fields() -> None:
    event = SyntheticEventV1.generated(
        symbol="EURUSD",
        event_time_ns=2,
        event_sequence=0,
        bid=1.0,
        ask=1.001,
        run_id="run:fixture",
        ensemble_member_id="member:fixture",
        source_version_id="dataset-version:fixture",
        left_anchor_event_id="event:left",
        right_anchor_event_id="event:right",
        generator_id="generator:fixture",
        generator_version="1.0.0",
        generator_config_id="config:fixture",
        constraint_set_id="constraints:fixture",
    )
    payload = event.to_dict()

    assert payload["origin"] == "synthetic"
    assert not {
        "broker_history",
        "historical_truth",
        "recovered_truth",
    }.intersection(payload)
    payload["origin"] = "observed"
    with pytest.raises(ValueError, match="observed event requires"):
        SyntheticEventV1.from_dict(payload)


def test_retained_v24_policy_is_readable_but_requires_current_replan() -> None:
    policy = (
        current_histdata_reconstruction_scientific_ledger().legacy_replay_policy
    )

    assert policy.retained_release_line == "2.4.x"
    assert policy.scientific_binding_status == "legacy-unbound"
    assert policy.identity_replayable
    assert not policy.execution_allowed_without_replan
    assert "regenerate" in policy.migration_action.lower()
