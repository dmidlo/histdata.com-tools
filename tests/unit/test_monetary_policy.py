"""Tests for provider-neutral monetary-policy semantics."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from histdatacom import market_context
from histdatacom.market_context.monetary_policy import (
    MonetaryPolicyActionTiming,
    MonetaryPolicyDecisionDirection,
    MonetaryPolicyDecisionV1,
    MonetaryPolicyEventPhase,
    MonetaryPolicyExpectationKind,
    MonetaryPolicyExpectationV1,
    MonetaryPolicyFramework,
    MonetaryPolicyFrameworkAuditV1,
    MonetaryPolicyFrameworkProfileV1,
    MonetaryPolicyMeetingBundleV1,
    MonetaryPolicyMeetingV1,
    MonetaryPolicyPhaseEventV1,
    MonetaryPolicyScheduleEvidenceKind,
    MonetaryPolicySettingComponentV1,
    MonetaryPolicySettingKind,
    MonetaryPolicySettingV1,
    MonetaryPolicySurpriseKind,
    MonetaryPolicyVoteBucketV1,
    MonetaryPolicyVoteTallyV1,
    audit_monetary_policy_frameworks,
    built_in_monetary_policy_profiles,
    compute_monetary_policy_rate_surprise,
    declare_monetary_policy_text_surprise,
    require_monetary_policy_framework_coverage,
)
from histdatacom.market_context.official_sources import (
    load_packaged_official_source_registry,
)


def _ns(value: str) -> int:
    parsed = datetime.fromisoformat(value)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed.astimezone(timezone.utc) - epoch
    return (
        delta.days * 86_400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1_000


def _component(key: str, value: float) -> MonetaryPolicySettingComponentV1:
    return MonetaryPolicySettingComponentV1(
        component_key=key,
        label=key.replace("-", " ").title(),
        value=value,
        unit="percent",
        source_lexical=str(value),
    )


def _scalar(value: float) -> MonetaryPolicySettingV1:
    return MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.SCALAR_RATE,
        unit="percent",
        definition_version="policy-rate.v1",
        source_lexical=str(value),
        scalar_value=value,
    )


def _target_range(lower: float, upper: float) -> MonetaryPolicySettingV1:
    return MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.TARGET_RANGE,
        unit="percent",
        definition_version="federal-funds-target-range.v1",
        source_lexical=f"{lower}-{upper}",
        lower_bound=lower,
        upper_bound=upper,
    )


def _rate_set(deposit: float, refinancing: float) -> MonetaryPolicySettingV1:
    return MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.RATE_SET,
        unit="percent",
        definition_version="ecb-key-rates.v1",
        source_lexical=f"deposit={deposit};mro={refinancing}",
        components=(
            _component("main-refinancing", refinancing),
            _component("deposit-facility", deposit),
        ),
    )


def _meeting(
    *,
    timing: MonetaryPolicyActionTiming = MonetaryPolicyActionTiming.SCHEDULED,
    framework: MonetaryPolicyFramework = MonetaryPolicyFramework.TARGET_RANGE,
) -> MonetaryPolicyMeetingV1:
    scheduled = timing is MonetaryPolicyActionTiming.SCHEDULED
    return MonetaryPolicyMeetingV1(
        meeting_key="us.fomc.2026-09-16",
        economy_code="US",
        institution="Federal Reserve Board / FOMC",
        framework=framework,
        action_timing=timing,
        first_known_at_ns=_ns(
            "2026-01-01T12:00:00+00:00"
            if scheduled
            else "2026-09-16T14:00:00+00:00"
        ),
        schedule_evidence_kind=(
            MonetaryPolicyScheduleEvidenceKind.ARCHIVED_CALENDAR
            if scheduled
            else MonetaryPolicyScheduleEvidenceKind.ARCHIVED_RELEASE
        ),
        schedule_source_key="us.federal-reserve.fomc",
        schedule_snapshot_id="official-snapshot:sha256:" + "a" * 64,
        schedule_source_uri=(
            "https://www.federalreserve.gov/monetarypolicy/" "fomccalendars.htm"
        ),
        source_timezone="America/New_York",
        limitations=("Vote details may be published with the statement.",),
        scheduled_start_ns=(
            _ns("2026-09-16T13:00:00+00:00") if scheduled else None
        ),
        scheduled_lexical=("2026-09-16T09:00:00-04:00" if scheduled else None),
    )


def _event(
    meeting: MonetaryPolicyMeetingV1,
    phase: MonetaryPolicyEventPhase,
    *,
    hour: int = 14,
) -> MonetaryPolicyPhaseEventV1:
    lexical = f"2026-09-16T{hour - 4:02d}:00:00-04:00"
    return MonetaryPolicyPhaseEventV1(
        meeting_id=meeting.meeting_id,
        event_key=f"us.fomc.2026-09-16.{phase.value}",
        phase=phase,
        published_at_ns=_ns(f"2026-09-16T{hour:02d}:00:00+00:00"),
        published_lexical=lexical,
        available_at_ns=_ns(f"2026-09-16T{hour:02d}:00:01+00:00"),
        source_timezone="America/New_York",
        source_key="us.federal-reserve.fomc",
        source_snapshot_id="official-snapshot:sha256:" + "b" * 64,
        source_uri="https://www.federalreserve.gov/newsevents.htm",
        content_sha256="c" * 64,
        limitations=("Publication time is source-observed.",),
    )


def _expectation(
    meeting: MonetaryPolicyMeetingV1,
    setting: MonetaryPolicySettingV1 | None,
    *,
    kind: MonetaryPolicyExpectationKind = (
        MonetaryPolicyExpectationKind.EVENT_CONSENSUS
    ),
    target_phase: MonetaryPolicyEventPhase = (
        MonetaryPolicyEventPhase.DECISION
    ),
) -> MonetaryPolicyExpectationV1:
    unavailable = kind is MonetaryPolicyExpectationKind.UNAVAILABLE
    return MonetaryPolicyExpectationV1(
        meeting_id=meeting.meeting_id,
        target_phase=target_phase,
        kind=kind,
        expected_setting=None if unavailable else setting,
        collection_start_ns=(
            None if unavailable else _ns("2026-09-14T12:00:00+00:00")
        ),
        collection_cutoff_ns=(
            None if unavailable else _ns("2026-09-16T12:00:00+00:00")
        ),
        available_at_ns=(
            None if unavailable else _ns("2026-09-16T12:30:00+00:00")
        ),
        source_key=None if unavailable else "us.survey.event-expectation",
        source_snapshot_id=(
            None if unavailable else "official-snapshot:sha256:" + "d" * 64
        ),
        source_uri=(
            None
            if unavailable
            else "https://example.gov/pre-decision-expectation"
        ),
        unavailable_reason=(
            "No qualified exact event expectation." if unavailable else None
        ),
        limitations=("Only exact event targets are calendar eligible.",),
    )


def _decision(
    meeting: MonetaryPolicyMeetingV1,
    event: MonetaryPolicyPhaseEventV1,
    previous: MonetaryPolicySettingV1,
    new: MonetaryPolicySettingV1,
    expectation: MonetaryPolicyExpectationV1,
    *,
    direction: MonetaryPolicyDecisionDirection,
    statement_event_id: str | None = None,
) -> MonetaryPolicyDecisionV1:
    return MonetaryPolicyDecisionV1(
        meeting_id=meeting.meeting_id,
        decision_event_id=event.phase_event_id,
        decided_at_ns=event.published_at_ns,
        framework=meeting.framework,
        previous_setting=previous,
        previous_setting_as_known_at_ns=(_ns("2026-09-16T12:59:59+00:00")),
        previous_setting_evidence_id=(
            "monetary-policy-decision:sha256:" + "e" * 64
        ),
        new_setting=new,
        expectation=expectation,
        direction=direction,
        decision_code=direction.value,
        statement_event_id=statement_event_id,
    )


def test_setting_shapes_preserve_scalar_range_set_yield_and_category() -> None:
    scalar = _scalar(4.25)
    target = _target_range(5.25, 5.50)
    rates = _rate_set(2.0, 2.15)
    yield_target = MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.YIELD_TARGET,
        unit="percent",
        definition_version="boj-ten-year-target.v1",
        source_lexical="around zero percent",
        scalar_value=0.0,
        target_tenor="10-year JGB",
    )
    category = MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.CATEGORICAL,
        unit="categorical",
        definition_version="mas-slope.v1",
        source_lexical="slightly increase the slope",
        categorical_code="increase-slope",
        categorical_label="Slightly increase slope",
    )

    assert MonetaryPolicySettingV1.from_json(scalar.to_json()) == scalar
    assert target.scalar_value is None
    assert (target.lower_bound, target.upper_bound) == (5.25, 5.5)
    assert [item.component_key for item in rates.components] == [
        "deposit-facility",
        "main-refinancing",
    ]
    assert yield_target.target_tenor == "10-year JGB"
    assert category.categorical_code == "increase-slope"


def test_setting_shapes_fail_closed() -> None:
    invalid_settings: list[dict[str, object]] = [
        {
            "kind": MonetaryPolicySettingKind.SCALAR_RATE,
            "scalar_value": None,
        },
        {
            "kind": MonetaryPolicySettingKind.TARGET_RANGE,
            "lower_bound": 5.5,
            "upper_bound": 5.25,
        },
        {
            "kind": MonetaryPolicySettingKind.RATE_SET,
            "components": (_component("one", 1.0),),
        },
        {
            "kind": MonetaryPolicySettingKind.CATEGORICAL,
            "categorical_code": "hold",
        },
    ]
    for kwargs in invalid_settings:
        with pytest.raises(ValueError):
            MonetaryPolicySettingV1(
                unit="percent",
                definition_version="test.v1",
                source_lexical="source",
                **kwargs,  # type: ignore[arg-type]
            )


def test_scheduled_and_emergency_meetings_require_matching_evidence() -> None:
    scheduled = _meeting()
    assert MonetaryPolicyMeetingV1.from_dict(scheduled.to_dict()) == scheduled

    emergency = _meeting(timing=MonetaryPolicyActionTiming.EMERGENCY)
    assert emergency.scheduled_start_ns is None
    with pytest.raises(ValueError, match="unsupported schedule evidence kind"):
        replace(
            emergency,
            schedule_evidence_kind="retrospective-list",  # type: ignore[arg-type]
            meeting_id="",
        )
    with pytest.raises(ValueError, match="unscheduled action"):
        replace(
            emergency,
            scheduled_start_ns=_ns("2026-09-16T13:00:00+00:00"),
            scheduled_lexical="2026-09-16T09:00:00-04:00",
            meeting_id="",
        )


def test_phase_events_retain_distinct_identity_and_exact_local_time() -> None:
    meeting = _meeting()
    decision = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    statement = _event(meeting, MonetaryPolicyEventPhase.STATEMENT)

    assert decision.phase_event_id != statement.phase_event_id
    assert decision.published_lexical.endswith("-04:00")
    assert MonetaryPolicyPhaseEventV1.from_dict(decision.to_dict()) == decision
    with pytest.raises(ValueError, match="offset differs"):
        replace(
            decision,
            published_lexical="2026-09-16T14:00:00+00:00",
            phase_event_id="",
        )


def test_expectations_separate_event_consensus_period_targets_and_gaps() -> (
    None
):
    meeting = _meeting()
    exact = _expectation(meeting, _target_range(5.25, 5.5))
    period = _expectation(
        meeting,
        _target_range(5.25, 5.5),
        kind=MonetaryPolicyExpectationKind.OFFICIAL_SURVEY_PERIOD_TARGET,
    )
    unavailable = _expectation(
        meeting, None, kind=MonetaryPolicyExpectationKind.UNAVAILABLE
    )

    assert exact.calendar_style_eligible
    assert not period.calendar_style_eligible
    assert unavailable.expected_setting is None
    assert (
        MonetaryPolicyExpectationV1.from_dict(exact.to_dict()).expectation_id
        == exact.expectation_id
    )
    with pytest.raises(ValueError, match="chronology"):
        replace(
            exact,
            collection_start_ns=exact.available_at_ns,
            collection_cutoff_ns=exact.collection_start_ns,
            expectation_id="",
        )


def test_decision_enforces_previous_actual_and_ex_ante_expectation() -> None:
    meeting = _meeting()
    event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    previous = _target_range(5.0, 5.25)
    new = _target_range(5.25, 5.5)
    expectation = _expectation(meeting, previous)
    decision = _decision(
        meeting,
        event,
        previous,
        new,
        expectation,
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )

    assert MonetaryPolicyDecisionV1.from_dict(decision.to_dict()) == decision
    with pytest.raises(ValueError, match="not available ex ante"):
        replace(
            decision,
            expectation=replace(
                expectation,
                available_at_ns=event.published_at_ns + 1,
                expectation_id="",
            ),
            decision_id="",
        )
    with pytest.raises(ValueError, match="previous setting was not known"):
        replace(
            decision,
            previous_setting_as_known_at_ns=event.published_at_ns + 1,
            decision_id="",
        )
    with pytest.raises(ValueError, match="must change"):
        _decision(
            meeting,
            event,
            previous,
            previous,
            expectation,
            direction=MonetaryPolicyDecisionDirection.TIGHTEN,
        )
    with pytest.raises(ValueError, match="invalid for framework"):
        _decision(
            meeting,
            event,
            _scalar(5.0),
            _scalar(5.25),
            _expectation(meeting, _scalar(5.0)),
            direction=MonetaryPolicyDecisionDirection.TIGHTEN,
        )


def test_guidance_only_change_links_statement_without_changing_rate() -> None:
    meeting = _meeting()
    event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    statement = _event(meeting, MonetaryPolicyEventPhase.FORWARD_GUIDANCE)
    setting = _target_range(5.25, 5.5)
    same_setting_new_wording = replace(
        setting,
        source_lexical="5.25 percent to 5.50 percent",
        setting_id="",
    )
    assert same_setting_new_wording.setting_id != setting.setting_id
    assert same_setting_new_wording.semantic_id == setting.semantic_id
    decision = _decision(
        meeting,
        event,
        setting,
        same_setting_new_wording,
        _expectation(meeting, setting),
        direction=MonetaryPolicyDecisionDirection.GUIDANCE_ONLY,
        statement_event_id=statement.phase_event_id,
    )
    assert decision.statement_event_id == statement.phase_event_id


def test_rate_surprises_preserve_scalar_range_component_and_category() -> None:
    meeting = _meeting()
    event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    range_decision = _decision(
        meeting,
        event,
        _target_range(5.0, 5.25),
        _target_range(5.5, 5.75),
        _expectation(meeting, _target_range(5.25, 5.5)),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )
    range_surprise = compute_monetary_policy_rate_surprise(range_decision)
    assert range_surprise.kind is MonetaryPolicySurpriseKind.RANGE_VECTOR
    assert (range_surprise.lower_delta, range_surprise.upper_delta) == (
        0.25,
        0.25,
    )
    assert type(range_surprise).from_dict(range_surprise.to_dict()) == (
        range_surprise
    )

    scalar_meeting = _meeting(framework=MonetaryPolicyFramework.POLICY_RATE)
    scalar_event = _event(scalar_meeting, MonetaryPolicyEventPhase.DECISION)
    scalar_decision = _decision(
        scalar_meeting,
        scalar_event,
        _scalar(4.0),
        _scalar(4.5),
        _expectation(scalar_meeting, _scalar(4.25)),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )
    scalar_surprise = compute_monetary_policy_rate_surprise(scalar_decision)
    assert scalar_surprise.scalar_delta == 0.25

    rate_set_meeting = _meeting(framework=MonetaryPolicyFramework.RATE_SET)
    rate_set_event = _event(rate_set_meeting, MonetaryPolicyEventPhase.DECISION)
    rate_set_decision = _decision(
        rate_set_meeting,
        rate_set_event,
        _rate_set(1.75, 1.9),
        _rate_set(2.25, 2.4),
        _expectation(rate_set_meeting, _rate_set(2.0, 2.15)),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )
    component_surprise = compute_monetary_policy_rate_surprise(
        rate_set_decision
    )
    assert (
        component_surprise.kind is MonetaryPolicySurpriseKind.COMPONENT_VECTOR
    )
    assert dict(component_surprise.component_deltas) == {
        "deposit-facility": 0.25,
        "main-refinancing": 0.25,
    }


def test_ineligible_projection_does_not_become_event_consensus() -> None:
    meeting = _meeting()
    event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    previous = _target_range(5.0, 5.25)
    decision = _decision(
        meeting,
        event,
        previous,
        _target_range(5.25, 5.5),
        _expectation(
            meeting,
            previous,
            kind=MonetaryPolicyExpectationKind.CENTRAL_BANK_PROJECTION,
        ),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )
    surprise = compute_monetary_policy_rate_surprise(decision)
    assert surprise.kind is MonetaryPolicySurpriseKind.UNAVAILABLE


def test_text_surprise_is_separate_from_rate_surprise() -> None:
    meeting = _meeting()
    statement = _event(meeting, MonetaryPolicyEventPhase.STATEMENT)
    surprise = declare_monetary_policy_text_surprise(
        statement,
        method_version="statement-latent-state.v1",
        latent_score=-0.4,
        explanation="Prior-only textual model score.",
    )
    assert surprise.kind is MonetaryPolicySurpriseKind.TEXTUAL_LATENT
    assert surprise.decision_id is None
    assert surprise.scalar_delta is None
    with pytest.raises(ValueError, match="textual policy phase"):
        declare_monetary_policy_text_surprise(
            _event(meeting, MonetaryPolicyEventPhase.DECISION),
            method_version="statement-latent-state.v1",
            latent_score=0.0,
            explanation="Invalid phase.",
        )


def test_vote_tally_reconciles_and_links_to_distinct_vote_phase() -> None:
    meeting = _meeting()
    event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    vote_event = _event(meeting, MonetaryPolicyEventPhase.VOTE_SPLIT)
    previous = _target_range(5.0, 5.25)
    decision = _decision(
        meeting,
        event,
        previous,
        _target_range(5.25, 5.5),
        _expectation(meeting, previous),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
    )
    tally = MonetaryPolicyVoteTallyV1(
        meeting_id=meeting.meeting_id,
        decision_id=decision.decision_id,
        vote_event_id=vote_event.phase_event_id,
        eligible_voters=12,
        not_voting=0,
        buckets=(
            MonetaryPolicyVoteBucketV1(
                position_key="for",
                label="For the action",
                count=11,
                preferred_setting_id=decision.new_setting.setting_id,
            ),
            MonetaryPolicyVoteBucketV1(
                position_key="against",
                label="Against the action",
                count=1,
            ),
        ),
    )
    assert MonetaryPolicyVoteTallyV1.from_dict(tally.to_dict()) == tally
    with pytest.raises(ValueError, match="reconcile"):
        replace(tally, eligible_voters=13, tally_id="")


def test_bundle_links_phases_decision_vote_and_round_trips() -> None:
    meeting = _meeting()
    decision_event = _event(meeting, MonetaryPolicyEventPhase.DECISION)
    statement = _event(meeting, MonetaryPolicyEventPhase.STATEMENT)
    vote_event = _event(meeting, MonetaryPolicyEventPhase.VOTE_SPLIT)
    press = _event(meeting, MonetaryPolicyEventPhase.PRESS_CONFERENCE, hour=15)
    minutes = _event(meeting, MonetaryPolicyEventPhase.MINUTES, hour=16)
    previous = _target_range(5.0, 5.25)
    decision = _decision(
        meeting,
        decision_event,
        previous,
        _target_range(5.25, 5.5),
        _expectation(meeting, previous),
        direction=MonetaryPolicyDecisionDirection.TIGHTEN,
        statement_event_id=statement.phase_event_id,
    )
    tally = MonetaryPolicyVoteTallyV1(
        meeting_id=meeting.meeting_id,
        decision_id=decision.decision_id,
        vote_event_id=vote_event.phase_event_id,
        eligible_voters=2,
        not_voting=0,
        buckets=(MonetaryPolicyVoteBucketV1("for", "For", 2),),
    )
    bundle = MonetaryPolicyMeetingBundleV1(
        meeting=meeting,
        phase_events=(decision_event, statement, vote_event, press, minutes),
        decisions=(decision,),
        vote_tallies=(tally,),
    )
    assert MonetaryPolicyMeetingBundleV1.from_json(bundle.to_json()) == bundle

    cancelled = _event(meeting, MonetaryPolicyEventPhase.CANCELLATION)
    with pytest.raises(ValueError, match="cancelled"):
        replace(
            bundle,
            phase_events=(cancelled, decision_event, statement),
            vote_tallies=(),
            bundle_id="",
        )


def test_emergency_bundle_cannot_be_inferred_from_a_retrospective_list() -> (
    None
):
    meeting = _meeting(timing=MonetaryPolicyActionTiming.EMERGENCY)
    action = _event(meeting, MonetaryPolicyEventPhase.EMERGENCY_ACTION)
    previous = _target_range(5.0, 5.25)
    decision = _decision(
        meeting,
        action,
        previous,
        _target_range(4.5, 4.75),
        _expectation(
            meeting,
            None,
            kind=MonetaryPolicyExpectationKind.UNAVAILABLE,
            target_phase=MonetaryPolicyEventPhase.EMERGENCY_ACTION,
        ),
        direction=MonetaryPolicyDecisionDirection.EASE,
    )
    bundle = MonetaryPolicyMeetingBundleV1(
        meeting=meeting,
        phase_events=(action,),
        decisions=(decision,),
        vote_tallies=(),
    )
    assert bundle.meeting.schedule_evidence_kind in {
        MonetaryPolicyScheduleEvidenceKind.CONTEMPORANEOUS_RELEASE,
        MonetaryPolicyScheduleEvidenceKind.ARCHIVED_RELEASE,
    }
    backdated = replace(
        meeting,
        first_known_at_ns=_ns("2026-09-16T13:59:59+00:00"),
        meeting_id="",
    )
    backdated_action = replace(
        action, meeting_id=backdated.meeting_id, phase_event_id=""
    )
    backdated_expectation = replace(
        decision.expectation,
        meeting_id=backdated.meeting_id,
        expectation_id="",
    )
    backdated_decision = replace(
        decision,
        meeting_id=backdated.meeting_id,
        decision_event_id=backdated_action.phase_event_id,
        expectation=backdated_expectation,
        decision_id="",
    )
    with pytest.raises(ValueError, match="publication-bound"):
        MonetaryPolicyMeetingBundleV1(
            meeting=backdated,
            phase_events=(backdated_action,),
            decisions=(backdated_decision,),
            vote_tallies=(),
        )


def test_profiles_cover_every_official_owner_and_special_framework() -> None:
    registry = load_packaged_official_source_registry()
    profiles = built_in_monetary_policy_profiles(registry)
    by_economy = {item.economy_code: item for item in profiles}
    audit = require_monetary_policy_framework_coverage(registry, profiles)

    assert len(profiles) == len(registry.scoped_economies) == 21
    assert audit.complete
    assert MonetaryPolicyFrameworkAuditV1.from_dict(audit.to_dict()) == audit
    assert (
        MonetaryPolicyFrameworkProfileV1.from_dict(by_economy["US"].to_dict())
        == by_economy["US"]
    )
    assert by_economy["US"].framework is MonetaryPolicyFramework.TARGET_RANGE
    assert by_economy["EA"].framework is MonetaryPolicyFramework.RATE_SET
    assert by_economy["DE"].source_key == "de.ecb.monetary-policy"
    assert (
        by_economy["JP"].framework
        is MonetaryPolicyFramework.YIELD_CURVE_CONTROL
    )
    assert by_economy["HK"].framework is MonetaryPolicyFramework.CURRENCY_BOARD
    assert (
        by_economy["SG"].framework is MonetaryPolicyFramework.EXCHANGE_RATE_BAND
    )
    assert all(item.supports_unscheduled_actions for item in profiles)
    assert all(
        MonetaryPolicyEventPhase.EMERGENCY_ACTION in item.supported_phases
        for item in profiles
    )


def test_profile_audit_fails_closed_on_missing_or_scalarized_special_case() -> (
    None
):
    registry = load_packaged_official_source_registry()
    profiles = list(built_in_monetary_policy_profiles(registry))
    without_us = tuple(item for item in profiles if item.economy_code != "US")
    missing = audit_monetary_policy_frameworks(registry, without_us)
    assert not missing.complete
    assert missing.missing_economies == ("US",)

    singapore = next(item for item in profiles if item.economy_code == "SG")
    scalarized = replace(
        singapore,
        framework=MonetaryPolicyFramework.POLICY_RATE,
        accepted_setting_kinds=(MonetaryPolicySettingKind.SCALAR_RATE,),
        profile_id="",
    )
    altered = tuple(
        scalarized if item.economy_code == "SG" else item for item in profiles
    )
    audit = audit_monetary_policy_frameworks(registry, altered)
    assert audit.framework_mismatches == ("SG",)
    with pytest.raises(ValueError, match="coverage is incomplete"):
        require_monetary_policy_framework_coverage(registry, altered)


def test_identity_tampering_is_rejected() -> None:
    setting = _target_range(5.25, 5.5)
    payload = setting.to_dict()
    payload["upper_bound"] = 5.75
    with pytest.raises(ValueError, match="deterministic identity"):
        MonetaryPolicySettingV1.from_dict(payload)


def test_unrecognized_enums_fail_closed() -> None:
    with pytest.raises(
        ValueError, match="unsupported monetary-policy framework"
    ):
        MonetaryPolicyFramework.from_value("universal-interest-rate")
    with pytest.raises(
        ValueError, match="unsupported monetary-policy event phase"
    ):
        MonetaryPolicyEventPhase.from_value("calendar-row")


def test_public_market_context_facade_exports_monetary_policy_contracts() -> (
    None
):
    assert market_context.MonetaryPolicyMeetingV1 is MonetaryPolicyMeetingV1
    assert (
        market_context.compute_monetary_policy_rate_surprise
        is compute_monetary_policy_rate_surprise
    )
    assert "MonetaryPolicyFrameworkAuditV1" in market_context.__all__
