"""Data analytics operations for HistData artifacts."""

from __future__ import annotations

from histdatacom.data_analytics.feed_regimes import (
    ANALYTICS_REPORT_SCHEMA_VERSION,
    AnalyticsDiscoveryResult,
    AnalyticsTarget,
    FeedPeriodProfile,
    FeedRegimeEra,
    FeedRegimeReport,
    analyze_feed_regimes,
    discover_analytics_targets,
    feed_regime_report_to_json,
    format_feed_regime_console_summary,
    write_feed_regime_report,
)

__all__ = [
    "ANALYTICS_REPORT_SCHEMA_VERSION",
    "AnalyticsDiscoveryResult",
    "AnalyticsTarget",
    "FeedPeriodProfile",
    "FeedRegimeEra",
    "FeedRegimeReport",
    "analyze_feed_regimes",
    "discover_analytics_targets",
    "feed_regime_report_to_json",
    "format_feed_regime_console_summary",
    "write_feed_regime_report",
]
