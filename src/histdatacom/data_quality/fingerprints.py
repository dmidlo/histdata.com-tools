"""Structural contracts for deterministic time-series fingerprints."""

from __future__ import annotations

from dataclasses import dataclass, field

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityRule,
    QualityTarget,
)
from histdatacom.runtime_contracts import JSONValue

TIME_SERIES_FINGERPRINT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint.v1"
)
TIME_SERIES_FINGERPRINT_METADATA_KEY = "time_series_fingerprint"
SERIES_FINGERPRINT_RULE_ID = "fingerprint.series"
CROSS_SERIES_FINGERPRINT_RULE_ID = "fingerprint.cross_series"

DEFAULT_FINGERPRINT_QUANTILES = (
    0.01,
    0.05,
    0.25,
    0.5,
    0.75,
    0.95,
    0.99,
)
DEFAULT_FINGERPRINT_LAGS = (1, 2, 3, 5, 10, 30, 60, 240, 1440)
DEFAULT_FINGERPRINT_ROLLING_WINDOWS = (60, 240, 1440)
DEFAULT_FINGERPRINT_HISTOGRAM_BINS = 32
DEFAULT_FINGERPRINT_MAX_ROWS = 1_000_000
DEFAULT_FINGERPRINT_ROUNDING_DIGITS = 12


@dataclass(frozen=True, slots=True)
class HistDataFingerprintProfile:
    """Operator-tunable limits for deterministic fingerprint summaries."""

    quantiles: tuple[float, ...] = DEFAULT_FINGERPRINT_QUANTILES
    lags: tuple[int, ...] = DEFAULT_FINGERPRINT_LAGS
    rolling_windows: tuple[int, ...] = DEFAULT_FINGERPRINT_ROLLING_WINDOWS
    histogram_bins: int = DEFAULT_FINGERPRINT_HISTOGRAM_BINS
    max_rows: int = DEFAULT_FINGERPRINT_MAX_ROWS
    rounding_digits: int = DEFAULT_FINGERPRINT_ROUNDING_DIGITS

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "quantiles": list(self.quantiles),
            "lags": list(self.lags),
            "rolling_windows": list(self.rolling_windows),
            "histogram_bins": self.histogram_bins,
            "max_rows": self.max_rows,
            "rounding_digits": self.rounding_digits,
        }


@dataclass(slots=True)
class HistDataSeriesFingerprintRule:
    """Expose the target-scoped fingerprint rule surface.

    Statistical fingerprint payloads are intentionally implemented in later
    rule revisions. The initial rule keeps group selection executable without
    emitting placeholder findings that could be mistaken for real summaries.
    """

    profile: HistDataFingerprintProfile = field(
        default_factory=HistDataFingerprintProfile
    )
    rule_id: str = SERIES_FINGERPRINT_RULE_ID
    description: str = (
        "Reserve the target-scoped time-series fingerprint rule surface."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return no findings until canonical payloads are implemented."""
        _ = target
        return ()


def fingerprint_quality_rules(
    profile: HistDataFingerprintProfile | None = None,
) -> tuple[QualityRule, ...]:
    """Return target-scoped fingerprint quality rules."""
    rule: QualityRule = HistDataSeriesFingerprintRule(
        profile=profile or HistDataFingerprintProfile()
    )
    return (rule,)
