"""Shared bounded-output limit normalization for data-quality reports."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TypeVar

from histdatacom.runtime_contracts import JSONValue

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class BoundedReportLimit:
    """Normalized limit semantics for bounded report surfaces."""

    requested_limit: int | None
    default_limit: int
    minimum_limit: int
    maximum_limit: int | None
    effective_limit: int
    unbounded: bool

    @classmethod
    def normalize(
        cls,
        requested_limit: int | None,
        *,
        default_limit: int,
        minimum_limit: int = 0,
        maximum_limit: int | None = None,
        allow_unbounded: bool = True,
    ) -> "BoundedReportLimit":
        """Return normalized limit metadata for a bounded output surface."""
        raw_limit = (
            default_limit if requested_limit is None else requested_limit
        )
        raw_limit = int(raw_limit)
        minimum_limit = int(minimum_limit)
        default_limit = int(default_limit)
        maximum = None if maximum_limit is None else int(maximum_limit)
        if maximum is not None and maximum < minimum_limit:
            maximum = minimum_limit
        if allow_unbounded and raw_limit < 0:
            return cls(
                requested_limit=requested_limit,
                default_limit=default_limit,
                minimum_limit=minimum_limit,
                maximum_limit=maximum,
                effective_limit=-1,
                unbounded=True,
            )
        effective_limit = max(minimum_limit, raw_limit)
        if maximum is not None:
            effective_limit = min(effective_limit, maximum)
        return cls(
            requested_limit=requested_limit,
            default_limit=default_limit,
            minimum_limit=minimum_limit,
            maximum_limit=maximum,
            effective_limit=effective_limit,
            unbounded=False,
        )

    @property
    def requested_or_default_limit(self) -> int:
        """Return the caller-requested limit, or the default when omitted."""
        if self.requested_limit is None:
            return self.default_limit
        return self.requested_limit

    def slice(self, values: Sequence[_T]) -> list[_T]:
        """Return values capped by the effective limit."""
        if self.unbounded:
            return list(values)
        return list(values[: self.effective_limit])

    def included_count(self, total_count: int) -> int:
        """Return the number of items included for a total item count."""
        total = max(0, int(total_count))
        if self.unbounded:
            return total
        return min(total, self.effective_limit)

    def count_payload(self, total_count: int) -> dict[str, JSONValue]:
        """Return count metadata with requested and effective limit semantics."""
        total = max(0, int(total_count))
        included = self.included_count(total)
        payload = self.limit_payload()
        payload.update(
            {
                "total_count": total,
                "included_count": included,
                "omitted_count": max(0, total - included),
                "truncated": total > included,
            }
        )
        return payload

    def limit_payload(self) -> dict[str, JSONValue]:
        """Return limit metadata without item counts."""
        return {
            "limit": self.effective_limit,
            "effective_limit": self.effective_limit,
            "requested_limit": self.requested_limit,
            "default_limit": self.default_limit,
            "minimum_limit": self.minimum_limit,
            "maximum_limit": self.maximum_limit,
            "unbounded": self.unbounded,
        }


def bounded_report_limit(
    requested_limit: int | None,
    *,
    default_limit: int,
    minimum_limit: int = 0,
    maximum_limit: int | None = None,
    allow_unbounded: bool = True,
) -> BoundedReportLimit:
    """Normalize bounded-report limit inputs."""
    return BoundedReportLimit.normalize(
        requested_limit,
        default_limit=default_limit,
        minimum_limit=minimum_limit,
        maximum_limit=maximum_limit,
        allow_unbounded=allow_unbounded,
    )
