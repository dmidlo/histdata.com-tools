"""Symbol metadata used by data-quality domain checks."""

from __future__ import annotations

from dataclasses import dataclass

from histdatacom.fx_enums import Pairs
from histdatacom.runtime_contracts import JSONValue

ASSET_CLASS_FX = "fx"
ASSET_CLASS_METAL = "metal"
ASSET_CLASS_OIL = "oil"
ASSET_CLASS_INDEX = "index"
ASSET_CLASS_UNKNOWN = "unknown"

FX_NON_JPY_PRECISION_RULE_NAME = "fx_non_jpy_six_decimal_bid"
FX_JPY_PRECISION_RULE_NAME = "fx_jpy_three_decimal_bid"

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
        )

    return _unknown_symbol_metadata(symbol, normalized)


def _unknown_symbol_metadata(
    symbol: str,
    normalized: str,
) -> HistDataSymbolMetadata:
    return HistDataSymbolMetadata(
        symbol=str(symbol),
        normalized_symbol=normalized,
        asset_class=ASSET_CLASS_UNKNOWN,
    )


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


_PAIR_VALUES_BY_SYMBOL: dict[str, str] = {}
_PAIR_KEYS_BY_SYMBOL: dict[str, str] = {}
for _pair in Pairs:
    _normalized_key = normalize_histdata_symbol(_pair.name)
    _normalized_value = normalize_histdata_symbol(_pair.value)
    _PAIR_VALUES_BY_SYMBOL[_normalized_key] = _pair.value
    _PAIR_VALUES_BY_SYMBOL[_normalized_value] = _pair.value
    _PAIR_KEYS_BY_SYMBOL[_normalized_key] = _pair.name
    _PAIR_KEYS_BY_SYMBOL[_normalized_value] = _pair.name
