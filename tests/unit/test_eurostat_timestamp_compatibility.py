"""Retained Eurostat offsets must parse identically on Python 3.10+."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from histdatacom.market_context._source_timestamps import (
    parse_source_iso_datetime,
)

_FAMILIES = (
    "construction_output",
    "gdp",
    "hicp",
    "industrial_production",
    "international_trade_goods",
    "labour_cost",
    "retail_trade",
    "unemployment",
)


def _module(family: str) -> Any:
    return import_module(
        f"histdatacom.market_context.eurostat_{family}_archive"
    )


@pytest.mark.parametrize(
    ("suffix", "offset_minutes"),
    [
        ("Z", 0),
        ("+0000", 0),
        ("-0000", 0),
        ("+00:00", 0),
        ("+0200", 120),
        ("+02:00", 120),
        ("+0545", 345),
        ("+05:45", 345),
        ("-0530", -330),
        ("-05:30", -330),
    ],
)
def test_numeric_offsets_and_utc_are_runtime_independent(
    suffix: str, offset_minutes: int
) -> None:
    parsed = parse_source_iso_datetime(f"2026-09-18T11:22:33.123456{suffix}")
    assert parsed.utcoffset() == timedelta(minutes=offset_minutes)
    assert (parsed.year, parsed.month, parsed.day) == (2026, 9, 18)
    assert (parsed.hour, parsed.minute, parsed.second) == (11, 22, 33)
    assert parsed.microsecond == 123456


@pytest.mark.parametrize(
    "value",
    [
        "",
        "not-a-timestamp",
        "2026-02-30T11:00:00+0200",
        "2026-09-18T25:00:00+0200",
        "2026-09-18T11:00:00+2500",
        "2026-09-18T11:00:00+25:00",
        "2026-09-18T11:00:00+02:XX",
        "2026-09-18T11:00:00+0200garbage",
        "2026-09-1800",
        "2026-09-1811",
    ],
)
def test_invalid_source_timestamps_remain_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_source_iso_datetime(value)


@pytest.mark.parametrize("family", [f for f in _FAMILIES if f != "hicp"])
@pytest.mark.parametrize("suffix", ["+0200", "+02:00", "-0530", "Z"])
def test_offset_required_callers_preserve_the_source_lexeme(
    family: str, suffix: str
) -> None:
    original = f"2026-09-18T11:00:00{suffix}"
    assert _module(family)._iso_datetime(original, "updated_at") == original


@pytest.mark.parametrize("family", [f for f in _FAMILIES if f != "hicp"])
def test_offset_required_callers_still_reject_naive_times(family: str) -> None:
    with pytest.raises(ValueError, match="must include an offset"):
        _module(family)._iso_datetime("2026-09-18T11:00:00", "updated_at")


@pytest.mark.parametrize("family", [f for f in _FAMILIES if f != "hicp"])
@pytest.mark.parametrize(
    "value", ["2026-09-18Z11:22:33+0200", "2026-09-18Z11:22:33Z"]
)
def test_legacy_callers_keep_rejecting_embedded_z(
    family: str, value: str
) -> None:
    with pytest.raises(ValueError, match="must be an ISO timestamp"):
        _module(family)._iso_datetime(value, "updated_at")


@pytest.mark.parametrize("value", ["2026-09-1800", "2026-09-1811"])
def test_hicp_does_not_launder_malformed_dates(value: str) -> None:
    module = _module("hicp")
    dataset = (
        module.load_packaged_eurostat_hicp_archive_manifest().legacy_dataset
    )
    with pytest.raises(ValueError, match="update time is invalid"):
        replace(dataset, updated_at=value, dataset_receipt_id="")


@pytest.mark.parametrize("family", _FAMILIES)
def test_packaged_eurostat_manifest_round_trip_is_byte_identical(
    family: str,
) -> None:
    module = _module(family)
    loader = getattr(
        module, f"load_packaged_eurostat_{family}_archive_manifest"
    )
    manifest = loader()
    asset = (
        Path(module.__file__).parent
        / "assets"
        / f"eurostat_{family}_archive_v1.json"
    )
    assert manifest.to_json() == asset.read_text().strip()


@pytest.mark.parametrize("suffix", ["+0200", "+02:00", "-0530", "Z", ""])
def test_hicp_preserves_its_existing_optional_offset_policy(
    suffix: str,
) -> None:
    module = _module("hicp")
    dataset = (
        module.load_packaged_eurostat_hicp_archive_manifest().legacy_dataset
    )
    original = f"2026-02-06T23:00:00{suffix}"
    restored = replace(dataset, updated_at=original, dataset_receipt_id="")
    assert restored.updated_at == original
    assert type(restored).from_dict(restored.to_dict()) == restored
