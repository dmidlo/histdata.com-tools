"""Rule selection helpers for data-quality assessment groups."""

from __future__ import annotations

from collections.abc import Iterable

from histdatacom.data_quality.contracts import QualityRule
from histdatacom.data_quality.discovery import normalize_quality_check_groups
from histdatacom.data_quality.inventory import inventory_quality_rules


def quality_rules_for_groups(
    groups: Iterable[str] | None,
) -> tuple[QualityRule, ...]:
    """Return concrete quality rules for normalized check group selections."""
    normalized = normalize_quality_check_groups(groups)
    rules: list[QualityRule] = []
    if "all" in normalized or "inventory" in normalized:
        rules.extend(inventory_quality_rules())
    return tuple(rules)
