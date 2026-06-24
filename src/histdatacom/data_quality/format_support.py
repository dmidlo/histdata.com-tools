"""Quality-support metadata for HistData formats and timeframes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from histdatacom.runtime_contracts import JSONValue

DEEP_QUALITY_DIMENSIONS = (("ascii", "M1"), ("ascii", "T"))
HISTDATA_FORMAT_SUPPORT_RULE_ID = "inventory.format_support"

_FORMAT_CODE_TO_VALUE = {
    "ASCII": "ascii",
    "MT": "metatrader",
    "NT": "ninjatrader",
    "MS": "metastock",
    "XLSX": "excel",
}
_FORMAT_VALUE_TO_CODE = {
    value: code for code, value in _FORMAT_CODE_TO_VALUE.items()
}
_SUPPORTED_TIMEFRAMES = {
    "ascii": ("M1", "T"),
    "metatrader": ("M1",),
    "ninjatrader": ("M1", "T_ASK", "T_BID", "T_LAST"),
    "metastock": ("M1",),
    "excel": ("M1",),
}
_TIMEFRAME_SCHEMAS = {
    ("ascii", "M1"): "ascii-m1-bid-ohlcv",
    ("ascii", "T"): "ascii-tick-bid-ask-volume",
    ("metatrader", "M1"): "metatrader-m1-bid-ohlcv",
    ("ninjatrader", "M1"): "ninjatrader-m1-bid-ohlcv",
    ("ninjatrader", "T_LAST"): "ninjatrader-tick-last-volume",
    ("ninjatrader", "T_BID"): "ninjatrader-tick-bid-volume",
    ("ninjatrader", "T_ASK"): "ninjatrader-tick-ask-volume",
    ("metastock", "M1"): "metastock-m1-bid-ohlcv",
    ("excel", "M1"): "excel-m1-workbook",
}
_PAYLOAD_EXTENSIONS = {
    "ASCII": "csv",
    "MT": "csv",
    "NT": "csv",
    "MS": "csv",
    "XLSX": "xlsx",
}


@dataclass(frozen=True, slots=True)
class HistDataFormatSupport:
    """Resolved quality-support profile for one target dimension."""

    data_format: str
    format_code: str
    timeframe: str
    status: str
    level: str
    inventory_supported: bool
    parser_supported: bool
    canonical_cache_supported: bool
    payload_extension: str
    schema: str
    supported_timeframes: tuple[str, ...]
    supported_check_groups: tuple[str, ...]
    message: str

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-safe support metadata for quality reports."""
        return {
            "data_format": self.data_format,
            "format_code": self.format_code,
            "timeframe": self.timeframe,
            "status": self.status,
            "level": self.level,
            "inventory_supported": self.inventory_supported,
            "parser_supported": self.parser_supported,
            "canonical_cache_supported": self.canonical_cache_supported,
            "payload_extension": self.payload_extension,
            "schema": self.schema,
            "supported_timeframes": list(self.supported_timeframes),
            "supported_check_groups": list(self.supported_check_groups),
            "message": self.message,
        }


def data_format_from_code(format_code: str) -> str:
    """Return the public format value for a HistData filename code."""
    return _FORMAT_CODE_TO_VALUE.get(str(format_code).upper(), "")


def format_code_for_data_format(data_format: str) -> str:
    """Return the HistData filename code for a public format value."""
    normalized = normalize_data_format(data_format)
    if normalized in _FORMAT_VALUE_TO_CODE:
        return _FORMAT_VALUE_TO_CODE[normalized]
    upper = str(data_format or "").strip().upper()
    if upper in _FORMAT_CODE_TO_VALUE:
        return upper
    return ""


def known_histdata_format_codes() -> tuple[str, ...]:
    """Return recognized HistData filename format codes."""
    return tuple(_FORMAT_CODE_TO_VALUE)


def known_histdata_timeframes() -> tuple[str, ...]:
    """Return recognized HistData filename timeframe keys."""
    values = {
        timeframe
        for timeframes in _SUPPORTED_TIMEFRAMES.values()
        for timeframe in timeframes
    }
    return tuple(sorted(values))


def normalize_data_format(data_format: str) -> str:
    """Normalize a public data-format value or filename code."""
    raw = str(data_format or "").strip()
    lowered = raw.lower()
    if lowered in _FORMAT_VALUE_TO_CODE:
        return lowered
    return _FORMAT_CODE_TO_VALUE.get(raw.upper(), lowered)


def normalize_timeframe(timeframe: str) -> str:
    """Normalize a HistData timeframe key for quality support lookups."""
    return str(timeframe or "").strip().upper()


def payload_extension_for_format(data_format: str) -> str:
    """Return expected extracted payload extension for a HistData format."""
    code = format_code_for_data_format(data_format)
    return _PAYLOAD_EXTENSIONS.get(code, "csv")


def quality_support_from_metadata(
    metadata: Mapping[str, Any] | None,
) -> HistDataFormatSupport:
    """Return quality-support profile from target metadata."""
    metadata = metadata or {}
    kind = str(metadata.get("kind", "") or "")
    if not kind:
        kind = str(metadata.get("target_kind", "") or "")
    return quality_support_for_target(
        data_format=str(metadata.get("data_format", "") or ""),
        timeframe=str(metadata.get("timeframe", "") or ""),
        kind=kind,
    )


def quality_support_for_target(
    *,
    data_format: str,
    timeframe: str,
    kind: str = "",
) -> HistDataFormatSupport:
    """Return the quality-support profile for a target dimension."""
    normalized_format = normalize_data_format(data_format)
    normalized_timeframe = normalize_timeframe(timeframe)
    format_code = format_code_for_data_format(normalized_format)
    payload_extension = payload_extension_for_format(normalized_format)
    supported_timeframes = _SUPPORTED_TIMEFRAMES.get(normalized_format, ())
    schema = _TIMEFRAME_SCHEMAS.get(
        (normalized_format, normalized_timeframe),
        "",
    )
    kind_value = str(kind or "").strip().lower()

    if (
        normalized_format == "ascii"
        and normalized_timeframe in supported_timeframes
        and kind_value == "cache"
    ):
        return HistDataFormatSupport(
            data_format=normalized_format,
            format_code=format_code,
            timeframe=normalized_timeframe,
            status="cache-supported",
            level="canonical-cache",
            inventory_supported=True,
            parser_supported=True,
            canonical_cache_supported=True,
            payload_extension=payload_extension,
            schema=schema or "ascii-polars-cache",
            supported_timeframes=supported_timeframes,
            supported_check_groups=(
                "ingestion",
                "time",
                "bars",
                "ticks",
                "domain",
                "modeling",
            ),
            message=(
                "Canonical ASCII cache metadata and typed columns are "
                "quality-supported."
            ),
        )

    if (normalized_format, normalized_timeframe) in DEEP_QUALITY_DIMENSIONS:
        return HistDataFormatSupport(
            data_format=normalized_format,
            format_code=format_code,
            timeframe=normalized_timeframe,
            status="deep-supported",
            level="parser",
            inventory_supported=True,
            parser_supported=True,
            canonical_cache_supported=True,
            payload_extension=payload_extension,
            schema=schema,
            supported_timeframes=supported_timeframes,
            supported_check_groups=(
                "inventory",
                "ingestion",
                "time",
                "bars",
                "ticks",
                "domain",
                "modeling",
            ),
            message="Parser-level data-quality checks are supported.",
        )

    if (
        normalized_format in _SUPPORTED_TIMEFRAMES
        and normalized_timeframe in supported_timeframes
    ):
        return HistDataFormatSupport(
            data_format=normalized_format,
            format_code=format_code,
            timeframe=normalized_timeframe,
            status="inventory-only",
            level="inventory",
            inventory_supported=True,
            parser_supported=False,
            canonical_cache_supported=False,
            payload_extension=payload_extension,
            schema=schema,
            supported_timeframes=supported_timeframes,
            supported_check_groups=("inventory",),
            message=(
                "This HistData format/timeframe is recognized for filename "
                "and ZIP-member inventory only; parser-level content checks "
                "are not implemented yet."
            ),
        )

    if normalized_format in _SUPPORTED_TIMEFRAMES:
        message = (
            "This HistData format is recognized, but the timeframe is not an "
            "advertised package-supported combination."
        )
    elif normalized_format:
        message = "This HistData format is not recognized by quality support."
    else:
        message = "HistData format metadata could not be derived."

    return HistDataFormatSupport(
        data_format=normalized_format,
        format_code=format_code,
        timeframe=normalized_timeframe,
        status="unsupported",
        level="unsupported",
        inventory_supported=False,
        parser_supported=False,
        canonical_cache_supported=False,
        payload_extension=payload_extension,
        schema=schema,
        supported_timeframes=supported_timeframes,
        supported_check_groups=(),
        message=message,
    )
