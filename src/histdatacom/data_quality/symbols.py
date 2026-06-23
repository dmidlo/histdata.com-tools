"""Symbol metadata used by data-quality domain checks."""

from __future__ import annotations

from dataclasses import dataclass

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
)
from histdatacom.fx_enums import Pairs
from histdatacom.histdata_ascii import M1, TICK
from histdatacom.runtime_contracts import JSONValue

DOMAIN_SYMBOL_METADATA_RULE_ID = "domain.symbol_metadata"

ASSET_CLASS_FX = "fx"
ASSET_CLASS_METAL = "metal"
ASSET_CLASS_OIL = "oil"
ASSET_CLASS_INDEX = "index"
ASSET_CLASS_UNKNOWN = "unknown"

FX_NON_JPY_PRECISION_RULE_NAME = "fx_non_jpy_six_decimal_bid"
FX_JPY_PRECISION_RULE_NAME = "fx_jpy_three_decimal_bid"

M1_BID_PRICE_COLUMNS = ("open", "high", "low", "close")
TICK_BID_ASK_PRICE_COLUMNS = ("bid", "ask")

CURRENCY_CODES = frozenset(
    {
        "AUD",
        "CAD",
        "CHF",
        "CZK",
        "DKK",
        "EUR",
        "GBP",
        "HKD",
        "HUF",
        "JPY",
        "MXN",
        "NOK",
        "NZD",
        "PLN",
        "SEK",
        "SGD",
        "TRY",
        "USD",
        "ZAR",
    }
)
METAL_BASES = frozenset({"XAG", "XAU"})
OIL_BASES = frozenset({"BCO", "WTI"})
INDEX_SYMBOLS = frozenset(
    {
        "AUXAUD",
        "ETXEUR",
        "FRXEUR",
        "GRXEUR",
        "HKXHKD",
        "JPXJPY",
        "NSXUSD",
        "SPXUSD",
        "UDXUSD",
        "UKXGBP",
    }
)


@dataclass(frozen=True, slots=True)
class HistDataSymbolPrecisionRule:
    """Instrument-aware price precision expectations."""

    name: str
    expected_decimal_places: tuple[int, ...]
    pip_size: str
    tick_size: str
    quote_side: str = "bid"

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible rule metadata."""
        return {
            "name": self.name,
            "expected_decimal_places": list(self.expected_decimal_places),
            "pip_size": self.pip_size,
            "tick_size": self.tick_size,
            "quote_side": self.quote_side,
        }


@dataclass(frozen=True, slots=True)
class HistDataSymbolMetadata:
    """Normalized HistData symbol metadata for quality checks."""

    symbol: str
    normalized_symbol: str
    asset_class: str
    base: str = ""
    quote: str = ""
    pair_key: str = ""
    source: str = "unknown"
    precision_rule: HistDataSymbolPrecisionRule | None = None
    aliases: tuple[str, ...] = ()

    @property
    def known(self) -> bool:
        """Return whether the symbol was recognized."""
        return self.asset_class != ASSET_CLASS_UNKNOWN

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return JSON-compatible symbol metadata."""
        return {
            "symbol": self.symbol,
            "normalized_symbol": self.normalized_symbol,
            "asset_class": self.asset_class,
            "base": self.base,
            "quote": self.quote,
            "pair_key": self.pair_key,
            "source": self.source,
            "known": self.known,
            "aliases": list(self.aliases),
            "pip_size": _precision_value(self.precision_rule, "pip_size"),
            "tick_size": _precision_value(self.precision_rule, "tick_size"),
            "quote_side": _precision_value(self.precision_rule, "quote_side"),
            "m1_bid_only": True,
            "precision_rule": (
                None
                if self.precision_rule is None
                else self.precision_rule.to_metadata()
            ),
        }


FX_NON_JPY_PRECISION_RULE = HistDataSymbolPrecisionRule(
    name=FX_NON_JPY_PRECISION_RULE_NAME,
    expected_decimal_places=(6,),
    pip_size="0.0001",
    tick_size="0.000001",
)
FX_JPY_PRECISION_RULE = HistDataSymbolPrecisionRule(
    name=FX_JPY_PRECISION_RULE_NAME,
    expected_decimal_places=(3,),
    pip_size="0.01",
    tick_size="0.001",
)


def normalize_histdata_symbol(symbol: str) -> str:
    """Normalize HistData symbols to compact uppercase text."""
    return "".join(
        character
        for character in str(symbol or "").upper()
        if character.isalnum()
    )


def symbol_metadata_for(symbol: str) -> HistDataSymbolMetadata:
    """Return normalized metadata and precision expectations for a symbol."""
    normalized = normalize_histdata_symbol(symbol)
    if not normalized:
        return _unknown_symbol_metadata(symbol, normalized)

    pair_value = _PAIR_VALUES_BY_SYMBOL.get(normalized)
    base, quote = _split_pair_value(pair_value, normalized)
    pair_key = _PAIR_KEYS_BY_SYMBOL.get(normalized, "")
    source = "fx_enums.Pairs" if pair_value is not None else "inferred"

    if base in CURRENCY_CODES and quote in CURRENCY_CODES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_FX,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            precision_rule=(
                FX_JPY_PRECISION_RULE
                if quote == "JPY"
                else FX_NON_JPY_PRECISION_RULE
            ),
            aliases=_aliases_for_symbol(normalized),
        )

    if base in METAL_BASES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_METAL,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    if base in OIL_BASES:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_OIL,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    if normalized in INDEX_SYMBOLS:
        return HistDataSymbolMetadata(
            symbol=str(symbol),
            normalized_symbol=normalized,
            asset_class=ASSET_CLASS_INDEX,
            base=base,
            quote=quote,
            pair_key=pair_key,
            source=source,
            aliases=_aliases_for_symbol(normalized),
        )

    return _unknown_symbol_metadata(symbol, normalized)


@dataclass(slots=True)
class HistDataSymbolMetadataRule:
    """Emit normalized symbol metadata and quote-convention assumptions."""

    warning_severity: QualitySeverity = QualitySeverity.WARNING
    rule_id: str = DOMAIN_SYMBOL_METADATA_RULE_ID
    description: str = (
        "Normalize HistData symbols into asset classes, base/quote metadata, "
        "alias context, and quote-convention assumptions."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return domain metadata findings for one target."""
        symbol = _target_symbol(target)
        if not symbol:
            return ()

        symbol_metadata = symbol_metadata_for(symbol)
        metadata = {
            "symbol_metadata": symbol_metadata.to_metadata(),
            "quote_convention": _quote_convention_metadata(symbol_metadata),
            "format_assumptions": _format_assumptions_metadata(target),
        }
        findings: list[QualityFinding] = [
            _domain_finding(
                target,
                code="DOMAIN_SYMBOL_METADATA_SUMMARY",
                message="Normalized symbol metadata and quote-convention "
                "profile.",
                severity=QualitySeverity.INFO,
                rule_id=self.rule_id,
                metadata=metadata,
            )
        ]
        if not symbol_metadata.known:
            findings.append(
                _domain_finding(
                    target,
                    code="DOMAIN_SYMBOL_METADATA_UNKNOWN",
                    message="Symbol metadata is unknown; downstream domain "
                    "checks should treat asset-class assumptions as "
                    "unavailable.",
                    severity=self.warning_severity,
                    rule_id=self.rule_id,
                    metadata=metadata,
                )
            )
        return tuple(findings)


def domain_quality_rules() -> tuple[QualityRule, ...]:
    """Return domain quality rules in deterministic execution order."""
    symbol_rule: QualityRule = HistDataSymbolMetadataRule()
    return (symbol_rule,)


def _unknown_symbol_metadata(
    symbol: str,
    normalized: str,
) -> HistDataSymbolMetadata:
    return HistDataSymbolMetadata(
        symbol=str(symbol),
        normalized_symbol=normalized,
        asset_class=ASSET_CLASS_UNKNOWN,
    )


def _quote_convention_metadata(
    metadata: HistDataSymbolMetadata,
) -> dict[str, JSONValue]:
    price_unit = (
        f"{metadata.quote} per {metadata.base}"
        if (metadata.base and metadata.quote)
        else ""
    )
    return {
        "asset_class": metadata.asset_class,
        "base": metadata.base,
        "quote": metadata.quote,
        "pair_direction": (
            "base_quote" if metadata.base and metadata.quote else ""
        ),
        "price_unit": price_unit,
        "fx_base_currency": (
            metadata.base if metadata.asset_class == ASSET_CLASS_FX else ""
        ),
        "fx_quote_currency": (
            metadata.quote if metadata.asset_class == ASSET_CLASS_FX else ""
        ),
        "m1_quote_side": "bid",
        "m1_bid_only": True,
        "tick_quote_sides": list(TICK_BID_ASK_PRICE_COLUMNS),
        "tick_spread_definition": "ask - bid",
    }


def _format_assumptions_metadata(target: QualityTarget) -> dict[str, JSONValue]:
    timeframe = target.timeframe
    return {
        "data_format": target.data_format,
        "timeframe": timeframe,
        "m1_bid_ohlc": timeframe == M1,
        "m1_bid_only": timeframe == M1,
        "m1_price_columns": list(M1_BID_PRICE_COLUMNS),
        "tick_bid_ask": timeframe == TICK,
        "tick_price_columns": list(TICK_BID_ASK_PRICE_COLUMNS),
        "active_quote_side": _active_quote_side(timeframe),
    }


def _active_quote_side(timeframe: str) -> str:
    if timeframe == M1:
        return "bid"
    if timeframe == TICK:
        return "bid/ask"
    return ""


def _target_symbol(target: QualityTarget) -> str:
    return str(target.symbol or target.metadata.get("symbol", "") or "")


def _domain_finding(
    target: QualityTarget,
    *,
    code: str,
    message: str,
    severity: QualitySeverity,
    rule_id: str,
    metadata: dict[str, JSONValue],
) -> QualityFinding:
    return QualityFinding(
        severity=QualitySeverity.from_value(severity),
        code=code,
        message=message,
        rule_id=rule_id,
        target=target,
        location=QualityLocation(
            path=target.path,
            column="symbol",
            metadata={"symbol": _target_symbol(target)},
        ),
        metadata=dict(metadata),
    )


def _precision_value(
    rule: HistDataSymbolPrecisionRule | None,
    field_name: str,
) -> str:
    if rule is None:
        return ""
    return str(getattr(rule, field_name))


def _split_pair_value(
    pair_value: str | None,
    normalized: str,
) -> tuple[str, str]:
    if pair_value:
        base, quote = pair_value.split("_", maxsplit=1)
        return base, quote
    if len(normalized) >= 6:
        return normalized[:3], normalized[3:6]
    return "", ""


def _aliases_for_symbol(normalized: str) -> tuple[str, ...]:
    return _PAIR_ALIASES_BY_SYMBOL.get(normalized, ())


_PAIR_VALUES_BY_SYMBOL: dict[str, str] = {}
_PAIR_KEYS_BY_SYMBOL: dict[str, str] = {}
_PAIR_ALIASES_BY_SYMBOL: dict[str, tuple[str, ...]] = {}
for _pair in Pairs:
    _normalized_key = normalize_histdata_symbol(_pair.name)
    _normalized_value = normalize_histdata_symbol(_pair.value)
    _PAIR_VALUES_BY_SYMBOL[_normalized_key] = _pair.value
    _PAIR_VALUES_BY_SYMBOL[_normalized_value] = _pair.value
    _PAIR_KEYS_BY_SYMBOL[_normalized_key] = _pair.name
    _PAIR_KEYS_BY_SYMBOL[_normalized_value] = _pair.name
    _aliases = tuple(
        dict.fromkeys(
            (
                _pair.name.upper(),
                _pair.value,
                _normalized_value,
            )
        )
    )
    _PAIR_ALIASES_BY_SYMBOL[_normalized_key] = _aliases
    _PAIR_ALIASES_BY_SYMBOL[_normalized_value] = _aliases
