from __future__ import annotations

from histdatacom.market_context.official_sources import (
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    UnitedStatesBackfillAuditV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesForecastStrategy,
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_backfill_closure import (
    load_packaged_united_states_backfill_audit,
    load_packaged_united_states_backfill_coverages,
)


def test_packaged_us_backfill_coverages_share_conservative_window() -> None:
    coverages = load_packaged_united_states_backfill_coverages()

    assert len(coverages) == 22
    assert len({item.program_key for item in coverages}) == 22
    assert {item.window_end_date for item in coverages} == {"2026-09-14"}


def test_packaged_us_backfill_marks_every_unavailable_forecast() -> None:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    by_key = {
        item.program_key: item
        for item in load_packaged_united_states_backfill_coverages()
    }

    assert all(
        UnitedStatesCoverageGapReason.NO_EVENT_FORECAST
        in by_key[program.program_key].gap_reasons
        for program in profile.programs
        if program.forecast_strategy is UnitedStatesForecastStrategy.UNAVAILABLE
    )


def test_packaged_us_backfill_audit_is_passing_and_stable() -> None:
    audit = load_packaged_united_states_backfill_audit()

    assert audit.program_count == 22
    assert audit.expected_occurrence_count == 7981
    assert audit.schedule_count == 7981
    assert audit.initial_actual_count == 7979
    assert audit.previous_as_known_count == 7584
    assert audit.revision_count == 7971
    assert audit.exact_minute_count == 7274
    assert audit.forecast_count == 823
    assert not audit.incomplete_program_keys
    assert not audit.blocking_gap_program_keys
    assert all(passed for _, passed in audit.checks)
    assert UnitedStatesBackfillAuditV1.from_json(audit.to_json()) == audit
