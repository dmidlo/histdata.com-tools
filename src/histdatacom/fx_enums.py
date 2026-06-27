"""Configure Enums used by histdatacom."""

# pylint: disable=invalid-name
from collections.abc import Iterable
from enum import Enum

# Majors 7
# eurusd usdjpy gbpusd usdcad usdchf audusd nzdusd

# Minors 7
# eurgbp euraud gbpchf audnzd audcad audchf gbpaud usdmxn

# Crosses 10
# eurchf eurcad eurnzd eurjpy gbpjpy chfjpy cadjpy
# audjpy nzdjpy gbpcad nzdcad sgdjpy gbpnzd cadchf

# Exotics 7
# eurtry usdtry usdsek usdnok usddkk usdzar usdhkd
# usdsgd eurpln eurhuf nzdchf usdhuf usdpln eurczk
# eursek usdczk zarjpy eurdkk eurnok usddkk

# metals-commodities
# xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp

# indices
# grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd
# nsxusd ukxgbp etxeur


class Pairs(Enum):  # noqa:H601
    """Enumerate list of public pairs published on histdata.com.

    Args:
        Enum (Enum): Parent Class

    Returns:
        (set): set of keys or values with:
                Pairs.list_keys()
                Pairs.list_values()
    """

    eurusd = "EUR_USD"
    eurchf = "EUR_CHF"
    eurgbp = "EUR_GBP"
    eurjpy = "EUR_JPY"
    euraud = "EUR_AUD"
    usdcad = "USD_CAD"
    usdchf = "USD_CHF"
    usdjpy = "USD_JPY"
    usdmxn = "USD_MXN"
    gbpchf = "GBP_CHF"
    gbpjpy = "GBP_JPY"
    gbpusd = "GBP_USD"
    audjpy = "AUD_JPY"
    audusd = "AUD_USD"
    chfjpy = "CHF_JPY"
    nzdjpy = "NZD_JPY"
    nzdusd = "NZD_USD"
    xauusd = "XAU_USD"
    eurcad = "EUR_CAD"
    audcad = "AUD_CAD"
    cadjpy = "CAD_JPY"
    eurnzd = "EUR_NZD"
    grxeur = "GRX_EUR"
    nzdcad = "NZD_CAD"
    sgdjpy = "SGD_JPY"
    usdhkd = "USD_HKD"
    usdnok = "USD_NOK"
    usdtry = "USD_TRY"
    xauaud = "XAU_AUD"
    audchf = "AUD_CHF"
    auxaud = "AUX_AUD"
    eurhuf = "EUR_HUF"
    eurpln = "EUR_PLN"
    frxeur = "FRX_EUR"
    hkxhkd = "HKX_HKD"
    nzdchf = "NZD_CHF"
    spxusd = "SPX_USD"
    usdhuf = "USD_HUF"
    usdpln = "USD_PLN"
    usdzar = "USD_ZAR"
    xauchf = "XAU_CHF"
    zarjpy = "ZAR_JPY"
    bcousd = "BCO_USD"
    etxeur = "ETX_EUR"
    eurczk = "EUR_CZK"
    eursek = "EUR_SEK"
    gbpaud = "GBP_AUD"
    gbpnzd = "GBP_NZD"
    jpxjpy = "JPX_JPY"
    udxusd = "UDX_USD"
    usdczk = "USD_CZK"
    usdsek = "USD_SEK"
    wtiusd = "WTI_USD"
    xaueur = "XAU_EUR"
    audnzd = "AUD_NZD"
    cadchf = "CAD_CHF"
    eurdkk = "EUR_DKK"
    eurnok = "EUR_NOK"
    eurtry = "EUR_TRY"
    gbpcad = "GBP_CAD"
    nsxusd = "NSX_USD"
    ukxgbp = "UKX_GBP"
    usddkk = "USD_DKK"
    usdsgd = "USD_SGD"
    xagusd = "XAG_USD"
    xaugbp = "XAU_GBP"

    @classmethod
    def list_keys(cls) -> set:
        """Return a set containing the keys of the Pairs enum.

        Returns:
            Pairs (set): set of str(keys)
        """
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls) -> set:
        """Return a set containing the values of the Pairs enum.

        Returns:
            Pairs (set): set of str(values)
        """
        return {member.value for _, member in cls.__members__.items()}


PAIR_GROUPS: dict[str, tuple[str, ...]] = {
    "majors": (
        "eurusd",
        "usdjpy",
        "gbpusd",
        "usdcad",
        "usdchf",
        "audusd",
        "nzdusd",
    ),
    "minors": (
        "eurgbp",
        "euraud",
        "eurchf",
        "eurcad",
        "eurnzd",
        "eurjpy",
        "gbpchf",
        "gbpjpy",
        "gbpaud",
        "gbpcad",
        "gbpnzd",
        "audjpy",
        "audcad",
        "audchf",
        "audnzd",
        "nzdjpy",
        "nzdcad",
        "nzdchf",
        "cadjpy",
        "cadchf",
        "chfjpy",
    ),
    "crosses": (
        "eurchf",
        "eurcad",
        "eurnzd",
        "eurjpy",
        "gbpjpy",
        "gbpcad",
        "gbpnzd",
        "audjpy",
        "audcad",
        "audchf",
        "audnzd",
        "nzdjpy",
        "nzdcad",
        "nzdchf",
        "cadjpy",
        "cadchf",
        "chfjpy",
        "sgdjpy",
    ),
    "exotics": (
        "eurtry",
        "usdtry",
        "usdsek",
        "usdnok",
        "usddkk",
        "usdzar",
        "usdhkd",
        "usdsgd",
        "eurpln",
        "eurhuf",
        "usdhuf",
        "usdpln",
        "eurczk",
        "eursek",
        "usdczk",
        "zarjpy",
        "eurdkk",
        "eurnok",
        "usdmxn",
    ),
    "metals": (
        "xauusd",
        "xauaud",
        "xauchf",
        "xaueur",
        "xagusd",
        "xaugbp",
    ),
    "commodities": (
        "bcousd",
        "wtiusd",
    ),
    "indices": (
        "grxeur",
        "auxaud",
        "frxeur",
        "hkxhkd",
        "spxusd",
        "jpxjpy",
        "udxusd",
        "nsxusd",
        "ukxgbp",
        "etxeur",
    ),
}
PAIR_GROUP_ALIASES: dict[str, str] = {
    "major": "majors",
    "minor": "minors",
    "cross": "crosses",
    "exotic": "exotics",
    "metal": "metals",
    "commodity": "commodities",
    "commods": "commodities",
    "index": "indices",
    "indexes": "indices",
}


def pair_group_names(*, include_aliases: bool = False) -> tuple[str, ...]:
    """Return supported instrument group names."""
    names = set(PAIR_GROUPS)
    if include_aliases:
        names.update(PAIR_GROUP_ALIASES)
    return tuple(sorted(names))


def normalize_pair_group(group: object) -> str:
    """Return the canonical instrument group name."""
    normalized = str(group).strip().lower().replace("_", "-")
    normalized = normalized.replace("-", "")
    canonical = PAIR_GROUP_ALIASES.get(normalized, normalized)
    if canonical not in PAIR_GROUPS:
        supported = ", ".join(pair_group_names(include_aliases=True))
        raise ValueError(
            f"unsupported pair group '{group}'. Supported groups: {supported}"
        )
    return canonical


def expand_pair_groups(groups: Iterable[object]) -> tuple[str, ...]:
    """Expand instrument groups into sorted HistData pair keys."""
    expanded: set[str] = set()
    for group in groups or ():
        expanded.update(PAIR_GROUPS[normalize_pair_group(group)])
    return tuple(sorted(expanded))


def expand_pair_selection(
    pairs: Iterable[object] | None,
    groups: Iterable[object] | None,
) -> tuple[str, ...]:
    """Return explicit pairs unioned with selected instrument groups."""
    normalized_pairs = {str(pair).lower() for pair in pairs or ()}
    normalized_groups = tuple(groups or ())
    if not normalized_groups:
        return tuple(sorted(normalized_pairs))

    grouped_pairs = set(expand_pair_groups(normalized_groups))
    if not normalized_pairs or normalized_pairs == Pairs.list_keys():
        return tuple(sorted(grouped_pairs))
    return tuple(sorted(normalized_pairs | grouped_pairs))


class Format(Enum):  # noqa:H601
    """Enumerate list of public formats published on histdata.com.

    Args:
        Enum (Enum): Parent Class

    Returns:
        (set): set of keys or values with:
                Format.list_keys()
                Format.list_values()
    """

    MT = "metatrader"
    NT = "ninjatrader"
    MS = "metastock"
    ASCII = "ascii"
    XLSX = "excel"

    @classmethod
    def list_keys(cls) -> set:
        """Return a set containing the keys of the Format enum.

        Returns:
            Pairs (set): set of str(keys)
        """
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls) -> set:
        """Return a set containing the values of the Format enum.

        Returns:
            Format (set): set of str(values)
        """
        return {member.value for _, member in cls.__members__.items()}


class Timeframe(Enum):  # noqa:H601
    """Enumerate list of public Timeframes published on histdata.com.

    Args:
        Enum (Enum): Parent Class

    Returns:
        (set): set of keys or values with:
                Timeframe.list_keys()
                Timeframe.list_values()
    """

    M1 = "1-minute-bar-quotes"
    T = "tick-data-quotes"  # noqa:WPS121
    T_LAST = "tick-last-quotes"
    T_BID = "tick-bid-quotes"
    T_ASK = "tick-ask-quotes"

    @classmethod
    def list_keys(cls) -> set:
        """Return a set containing the keys of the Timeframe enum.

        Returns:
            Pairs (set): set of str(keys)
        """
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls) -> set:
        """Return a set containing the values of the Timeframe enum.

        Returns:
            Format (set): set of str(values)
        """
        return {member.value for _, member in cls.__members__.items()}

    @classmethod
    def convert_to_values(cls, timeframe_set: set) -> set:
        """Convert a mixed set of timeframe keys and values to values only.

        Args:
            timeframe_set (set): mixed set of keys and values.

        Returns:
            set: timeframe values.
        """
        timeframes = timeframe_set
        if intersect := timeframe_set & Timeframe.list_keys():
            timeframes = timeframe_set | {
                Timeframe[timeframe].value
                for timeframe in intersect
                if timeframe in Timeframe.list_keys()
            }
        return timeframes & Timeframe.list_values()


class TimeFormat(Enum):  # noqa:H601
    """Enumerate list of public Timeformats for strptime histdata.com.

        from histdata.com csv sources.

    Args:
        Enum (Enum): Parent Class

    Returns:
        (set): set of keys or values with:
                TimeFormat.list_keys()
                TimeFormat.list_values()
    """

    MT_M1 = "%Y.%m.%d %H:%M"
    ASCII_M1 = "%Y%m%d %H%M%S"
    ASCII_T = "%Y%m%d %H%M%S%f"
    NT_M1 = "%Y%m%d %H%M%S"  # noqa:PIE796
    NT_T_LAST = "%Y%m%d %H%M%S"  # noqa:PIE796
    NT_T_BID = "%Y%m%d %H%M%S"  # noqa:PIE796
    NT_T_ASK = "%Y%m%d %H%M%S"  # noqa:PIE796
    MS_M1 = "%Y%m%d%H%M"

    @classmethod
    def list_keys(cls) -> set:
        """Return a set containing the keys of the TimeFormat enum.

        Returns:
            Pairs (set): set of str(keys)
        """
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls) -> set:
        """Return a set containing the values of the TimeFormat enum.

        Returns:
            Format (set): set of str(values)
        """
        return {member.value for _, member in cls.__members__.items()}


class TimePrecision(Enum):  # noqa:H601
    """Enumerate list of public InfluxDB write precisions.

    Args:
        Enum (Enum): Parent Class

    Returns:
        (set): set of keys or values with:
                TimePrecision.list_keys()
                TimePrecision.list_values()
    """

    MT_M1 = "s"
    ASCII_M1 = "s"
    ASCII_T = "ms"
    NT_M1 = "s"
    NT_T_LAST = "s"
    NT_T_BID = "s"
    NT_T_ASK = "s"
    MS_M1 = "s"

    @classmethod
    def list_keys(cls) -> set:
        """Return a set containing the keys of the TimePrecision enum.

        Returns:
            Pairs (set): set of str(keys)
        """
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls) -> set:
        """Return a set containing the values of the TimePrecision enum.

        Returns:
            Format (set): set of str(values)
        """
        return {member.value for _, member in cls.__members__.items()}


def get_valid_format_timeframes(file_format: str) -> list:
    """Return a list of format and timeframe combinations.

    Args:
        file_format (str): _description_

    Returns:
        list: _description_
    """
    timeframes = []

    match file_format:
        case "metatrader":
            timeframes.extend(["M1"])
        case "ninjatrader":
            timeframes.extend(["M1", "T_LAST", "T_BID", "T_ASK"])
        case "metastock":
            timeframes.extend(["M1"])
        case "ascii":
            timeframes.extend(["M1", "T"])
        case "excel":
            timeframes.extend(["M1"])

    return timeframes
