"""Tests for domain calendar and market-session data-quality tags."""

from __future__ import annotations

from pathlib import Path

from histdatacom.data_quality import (
    DOMAIN_CALENDAR_SESSION_RULE_ID,
    QUALITY_PROFILE_SCHEMA_VERSION,
    QualitySeverity,
    QualityStatus,
    calendar_policy_metadata,
    calendar_profile_from_mapping,
    classify_histdata_source_timestamp,
    discover_quality_targets,
    quality_rules_for_groups,
    run_quality_assessment,
)
from histdatacom.histdata_ascii import M1
from tests.fixtures.histdata_ascii.quality_cases import (
    CLEAN_M1_CASE,
    HistDataAsciiCase,
    write_ascii_case,
    write_zip_case,
)


def test_calendar_classifier_uses_fixed_est_no_dst_on_dst_boundary() -> None:
    """Session projection must not localize HistData rows as New York DST."""
    classification = classify_histdata_source_timestamp(
        "20220313 023000",
        M1,
    )

    assert classification.timestamp_utc_ms == 1647156600000
    assert classification.utc_timestamp == "2022-03-13T07:30:00Z"
    assert classification.source_timestamp_iso == "2022-03-13T02:30:00-05:00"
    assert classification.session_state == "weekend_closure"
    assert classification.clock_sessions == ("asia", "london")
    assert classification.active_sessions == ()
    assert "weekend_closure" in classification.calendar_tags


def test_calendar_policy_documents_optional_static_holiday_scope() -> None:
    """Reports should make static holiday/fix limitations explicit."""
    policy = calendar_policy_metadata()

    assert policy["source_timezone"] == "EST-no-DST"
    assert policy["source_utc_offset"] == "-05:00"
    assert (
        policy["holiday_calendar_source"] == "static_month_day_major_holidays"
    )
    assert policy["holiday_calendar_complete"] is False
    assert "exchange-specific" in str(policy["holiday_calendar_limitations"])
    assert policy["month_end_policy"] == "source_calendar_date"
    assert policy["calendar_profile"]["static_advisory"] is True
    assert policy["calendar_profile"]["complete"] is False


def test_domain_calendar_rule_reports_session_and_overlap_counts(
    tmp_path: Path,
) -> None:
    """Coarse UTC session tags should be available in domain reports."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_session_windows",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_SESSIONS.csv",
            rows=(
                "20120201 000000;1.306600;1.306610;1.306590;1.306600;0",
                "20120201 030000;1.306600;1.306610;1.306590;1.306600;0",
                "20120201 080000;1.306600;1.306610;1.306590;1.306600;0",
                "20120201 130000;1.306600;1.306610;1.306590;1.306600;0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report, "DOMAIN_CALENDAR_SESSION_SUMMARY")
    profile_notice = _finding(report, "DOMAIN_CALENDAR_PROFILE_INCOMPLETE")
    assert report.status is QualityStatus.CLEAN
    assert summary.rule_id == DOMAIN_CALENDAR_SESSION_RULE_ID
    assert profile_notice.severity is QualitySeverity.INFO
    assert profile_notice.metadata["missing_optional_calendar_data"] is True
    assert summary.metadata["parsed_row_count"] == 4
    assert summary.metadata["invalid_timestamp_count"] == 0
    assert summary.metadata["session_state_counts"] == {"market_open": 4}
    assert summary.metadata["clock_session_counts"] == {
        "asia": 2,
        "london": 2,
        "new_york": 2,
    }
    assert summary.metadata["active_session_counts"] == {
        "asia": 2,
        "london": 2,
        "new_york": 2,
    }
    assert summary.metadata["overlap_counts"] == {
        "asia_london_overlap": 1,
        "london_new_york_overlap": 1,
        "multi_session_overlap": 2,
    }
    assert summary.metadata["samples"][1]["calendar_tags"] == [
        "asia",
        "london",
        "asia_london_overlap",
        "multi_session_overlap",
        "market_open",
    ]


def test_domain_calendar_rule_reports_open_close_rollover_and_fix_tags(
    tmp_path: Path,
) -> None:
    """Special source/UTC windows should be counted for later baselines."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_calendar_regimes",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_REGIMES.csv",
            rows=(
                "20120205 170000;1.306600;1.306610;1.306590;1.306600;0",
                "20120203 165900;1.306600;1.306610;1.306590;1.306600;0",
                "20120201 110000;1.306600;1.306610;1.306590;1.306600;0",
                "20120331 110000;1.306600;1.306610;1.306590;1.306600;0",
                "20221225 120000;1.306600;1.306610;1.306590;1.306600;0",
                "20221231 110000;1.306600;1.306610;1.306590;1.306600;0",
            ),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report, "DOMAIN_CALENDAR_SESSION_SUMMARY")
    special = summary.metadata["special_tag_counts"]
    assert report.status is QualityStatus.CLEAN
    assert special["sunday_open"] == 1
    assert special["friday_close"] == 1
    assert special["daily_rollover"] == 2
    assert special["london_4pm_fix_window"] == 3
    assert special["month_end_fix_window"] == 2
    assert special["quarter_end_fix_window"] == 2
    assert special["year_end_fix_window"] == 1
    assert special["month_end"] == 2
    assert special["quarter_end"] == 2
    assert special["year_end"] == 1
    assert summary.metadata["holiday_tag_counts"] == {
        "major_holiday:christmas_day": 1
    }
    assert summary.metadata["calendar_policy"]["holiday_calendar_complete"] is (
        False
    )
    assert _finding(report, "DOMAIN_CALENDAR_PROFILE_INCOMPLETE").severity is (
        QualitySeverity.INFO
    )


def test_domain_calendar_rule_uses_configured_complete_profile(
    tmp_path: Path,
) -> None:
    """Configured profiles should add movable holidays and event windows."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_configured_calendar_profile",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_202203_PROFILED.csv",
            rows=(
                "20220415 120000;1.306600;1.306610;1.306590;1.306600;0",
                "20221227 120000;1.306600;1.306610;1.306590;1.306600;0",
                "20200316 120000;1.306600;1.306610;1.306590;1.306600;0",
            ),
        ),
    )

    report = _report_for_path(path, profile=_complete_calendar_profile())

    summary = _finding(report, "DOMAIN_CALENDAR_SESSION_SUMMARY")
    policy = summary.metadata["calendar_policy"]
    assert report.status is QualityStatus.CLEAN
    assert not any(
        finding.code == "DOMAIN_CALENDAR_PROFILE_INCOMPLETE"
        for finding in report.findings
    )
    assert policy["holiday_calendar_source"] == "operator-config"
    assert policy["holiday_calendar_complete"] is True
    assert policy["calendar_profile"]["version"] == "2026.06"
    assert summary.metadata["holiday_tag_counts"] == {
        "market_holiday:good_friday": 1
    }
    assert summary.metadata["event_tag_counts"] == {
        "crisis:covid_shock": 1,
        "thin_liquidity:christmas_new_year": 1,
    }
    assert (
        "market_holiday:good_friday"
        in summary.metadata["samples"][0]["calendar_tags"]
    )
    assert (
        "thin_liquidity:christmas_new_year"
        in summary.metadata["samples"][1]["event_tags"]
    )
    assert (
        "crisis:covid_shock" in summary.metadata["samples"][2]["calendar_tags"]
    )


def test_calendar_classifier_accepts_asset_scoped_profile() -> None:
    """Profile tags can be scoped to supported asset classes."""
    profile_config = _complete_calendar_profile()["rules"][
        "domain.calendar_sessions"
    ]["calendar_profile"]
    profile = calendar_profile_from_mapping(profile_config)

    fx = classify_histdata_source_timestamp(
        "20220415 120000",
        M1,
        calendar_profile=profile,
        asset_class="fx",
    )
    metal = classify_histdata_source_timestamp(
        "20220415 120000",
        M1,
        calendar_profile=profile,
        asset_class="metal",
    )

    assert "market_holiday:good_friday" in fx.holiday_tags
    assert metal.holiday_tags == ()


def test_domain_calendar_rule_warns_on_unparseable_timestamps(
    tmp_path: Path,
) -> None:
    """Domain calendar tagging should surface unavailable row tags."""
    path = write_ascii_case(
        tmp_path,
        HistDataAsciiCase(
            name="m1_calendar_bad_timestamp",
            timeframe=M1,
            filename="DAT_ASCII_EURUSD_M1_201202_BAD_CALENDAR.csv",
            rows=("not-a-date;1.306600;1.306610;1.306590;1.306600;0",),
        ),
    )

    report = _report_for_path(path)

    summary = _finding(report, "DOMAIN_CALENDAR_SESSION_SUMMARY")
    warning = _finding(report, "DOMAIN_CALENDAR_TIMESTAMP_UNPARSEABLE")
    assert report.status is QualityStatus.WARNING
    assert warning.severity is QualitySeverity.WARNING
    assert warning.location.row_number == 1
    assert warning.location.timestamp_source == "not-a-date"
    assert summary.metadata["invalid_timestamp_count"] == 1
    assert warning.metadata["samples"][0]["timestamp_source"] == "not-a-date"


def test_domain_calendar_rule_retains_zip_source_member(
    tmp_path: Path,
) -> None:
    """ZIP calendar findings should retain member context."""
    archive = write_zip_case(
        tmp_path,
        CLEAN_M1_CASE,
        zip_filename="DAT_ASCII_EURUSD_M1_201202.zip",
    )

    report = _report_for_path(archive)

    summary = _finding(report, "DOMAIN_CALENDAR_SESSION_SUMMARY")
    assert report.status is QualityStatus.CLEAN
    assert summary.metadata["source_member"] == CLEAN_M1_CASE.filename
    assert summary.metadata["samples"][0]["source_member"] == (
        CLEAN_M1_CASE.filename
    )


def _report_for_path(path: Path, *, profile: dict | None = None):
    discovery = discover_quality_targets((path,))
    return run_quality_assessment(
        discovery.targets,
        quality_rules_for_groups(("domain",), profile=profile),
    )


def _complete_calendar_profile() -> dict:
    return {
        "schema_version": QUALITY_PROFILE_SCHEMA_VERSION,
        "name": "complete-calendar",
        "rules": {
            "domain.calendar_sessions": {
                "calendar_profile": {
                    "name": "operator-complete-calendar",
                    "source": "operator-config",
                    "version": "2026.06",
                    "complete": True,
                    "date_tags": [
                        {
                            "name": "good_friday",
                            "tag": "market_holiday:good_friday",
                            "rule": "good_friday",
                            "asset_classes": ["fx"],
                            "description": "Movable Good Friday market holiday.",
                        }
                    ],
                    "window_tags": [
                        {
                            "name": "christmas_new_year_thin_liquidity",
                            "tag": "thin_liquidity:christmas_new_year",
                            "category": "thin_liquidity",
                            "start_month": 12,
                            "start_day": 24,
                            "end_month": 1,
                            "end_day": 2,
                            "description": "Christmas/New Year thin liquidity.",
                        },
                        {
                            "name": "covid_shock",
                            "tag": "crisis:covid_shock",
                            "category": "crisis",
                            "start_date": "2020-03-01",
                            "end_date": "2020-03-31",
                            "description": "Configured crisis-period tag.",
                        },
                    ],
                }
            }
        },
    }


def _finding(report, code: str):
    matches = tuple(
        finding for finding in report.findings if finding.code == code
    )
    assert len(matches) == 1
    return matches[0]
