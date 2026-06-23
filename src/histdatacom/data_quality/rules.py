"""Rule selection helpers for data-quality assessment groups."""

from __future__ import annotations

from collections.abc import Iterable

from histdatacom.data_quality.contracts import QualityRule, QualityRunRule
from histdatacom.data_quality.bars import (
    bars_quality_rules,
    bars_quality_run_rules,
)
from histdatacom.data_quality.calendar import calendar_quality_rules
from histdatacom.data_quality.discovery import normalize_quality_check_groups
from histdatacom.data_quality.ingestion import ingestion_quality_rules
from histdatacom.data_quality.inventory import inventory_quality_rules
from histdatacom.data_quality.manifest import manifest_quality_run_rules
from histdatacom.data_quality.symbols import (
    domain_quality_rules,
    domain_quality_run_rules,
)
from histdatacom.data_quality.time import (
    time_quality_rules,
    time_quality_run_rules,
)
from histdatacom.data_quality.ticks import ticks_quality_rules


def quality_rules_for_groups(
    groups: Iterable[str] | None,
) -> tuple[QualityRule, ...]:
    """Return concrete quality rules for normalized check group selections."""
    normalized = normalize_quality_check_groups(groups)
    rules: list[QualityRule] = []
    if "all" in normalized or "inventory" in normalized:
        rules.extend(inventory_quality_rules())
    if "all" in normalized or "ingestion" in normalized:
        rules.extend(ingestion_quality_rules())
    if "all" in normalized or "time" in normalized:
        rules.extend(time_quality_rules())
    if "all" in normalized or "bars" in normalized:
        rules.extend(bars_quality_rules())
    if "all" in normalized or "ticks" in normalized:
        rules.extend(ticks_quality_rules())
    if "all" in normalized or "domain" in normalized:
        rules.extend(domain_quality_rules())
        rules.extend(calendar_quality_rules())
    return tuple(rules)


def quality_run_rules_for_groups(
    groups: Iterable[str] | None,
) -> tuple[QualityRunRule, ...]:
    """Return run-scoped quality rules for normalized check selections."""
    normalized = normalize_quality_check_groups(groups)
    rules: list[QualityRunRule] = []
    if "all" in normalized or "inventory" in normalized:
        rules.extend(manifest_quality_run_rules())
    if "all" in normalized or "time" in normalized:
        rules.extend(time_quality_run_rules())
    if "all" in normalized or "bars" in normalized:
        rules.extend(bars_quality_run_rules())
    if "all" in normalized or "domain" in normalized:
        rules.extend(domain_quality_run_rules())
    return tuple(rules)
