"""Process-local trader integration seams; canonical owners retain all wires.

These protocols neither certify sources nor implement a strategy. The calendar
protocol is deliberately the existing canonical protocol, not a second one.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from histdatacom.datasets.catalog import DatasetCatalog
from histdatacom.datasets.contracts import (
    DatasetQueryScopeV1,
    DatasetResolutionV1,
    DatasetVerificationV1,
    DatasetVersionManifestV1,
)
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarAsKnownReaderV1,
)
from histdatacom.market_context.positioning import (
    CftcPositioningQueryV1,
    CftcReportFamily,
    CftcReportScope,
)
from histdatacom.synthetic.bar_features import (
    BarAbsenceDeclarationV1,
    BarAvailabilityDeclarationV1,
    BarFeaturePolicyV1,
    CausalBarSnapshotV1,
)

__all__ = [
    "EconomicCalendarAsKnownReaderV1",
    "TraderBarActivityReaderV1",
    "TraderDatasetReaderV1",
    "TraderPositioningReaderV1",
]


@runtime_checkable
class TraderPositioningReaderV1(Protocol):
    """Strict as-known weekly futures state, never interpolated spot volume."""

    def as_known_at(
        self,
        *,
        start_ns: int,
        end_ns: int,
        decision_at_ns: int,
        symbols: Sequence[str],
        report_families: Sequence[CftcReportFamily],
        report_scopes: Sequence[CftcReportScope],
        max_staleness_days: int | None = None,
    ) -> CftcPositioningQueryV1:
        """Return the native query, including its exact typed refusal state."""


@runtime_checkable
class TraderDatasetReaderV1(Protocol):
    """Register immutable entries, resolve once, then replay exact lineage."""

    def register(self, addition: DatasetCatalog) -> TraderDatasetReaderV1:
        """Return a new reader; conflicting entries never replace old ones."""

    def resolve(
        self, reference: str, *, query_scope: DatasetQueryScopeV1
    ) -> DatasetResolutionV1:
        """Resolve an exact version or mutable alias once."""

    def replay(self, receipt: DatasetResolutionV1) -> DatasetResolutionV1:
        """Use the retained version, not today's alias target."""

    def verify(self, receipt: DatasetResolutionV1) -> DatasetVerificationV1:
        """Verify bytes, without treating hashes as availability evidence."""

    def lineage(
        self, receipt: DatasetResolutionV1
    ) -> tuple[DatasetVersionManifestV1, ...]:
        """Return the complete bounded ancestor closure, including the root."""


@runtime_checkable
class TraderBarActivityReaderV1(Protocol):
    """Native closed-bar geometry and quote-activity features with missingness.

    The existing BarFeatureSourceV1 implements this interface directly. Activity
    counts/rates keep their canonical units; neither ticks nor HistData placeholders
    become traded volume. A declaration is not historical-knowledge verification.
    """

    def snapshot(
        self,
        *,
        symbol: str,
        decision_time_ns: int,
        policy: BarFeaturePolicyV1,
        availability: Sequence[BarAvailabilityDeclarationV1] = (),
        absences: Sequence[BarAbsenceDeclarationV1] = (),
    ) -> CausalBarSnapshotV1:
        """Replay the source product; preserve every canonical missing state."""


def bounded_clock(value: int, name: str) -> None:
    """Reject booleans, coercion and clocks outside the signed int64 domain."""
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError(f"{name} must be a nonnegative int64")
