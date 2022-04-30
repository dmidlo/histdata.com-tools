from enum import Enum
from influxdb_client import WritePrecision


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

# metals/commodities
# xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp

# indices
# grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd
# nsxusd ukxgbp etxeur

class Pairs(Enum):
    eurusd = "EUR/USD"
    eurchf = "EUR/CHF"
    eurgbp = "EUR/GBP"
    eurjpy = "EUR/JPY"
    euraud = "EUR/AUD"
    usdcad = "USD/CAD"
    usdchf = "USD/CHF"
    usdjpy = "USD/JPY"
    usdmxn = "USD/MXN"
    gbpchf = "GBP/CHF"
    gbpjpy = "GBP/JPY"
    gbpusd = "GBP/USD"
    audjpy = "AUD/JPY"
    audusd = "AUD/USD"
    chfjpy = "CHF/JPY"
    nzdjpy = "NZD/JPY"
    nzdusd = "NZD/USD"
    xauusd = "XAU/USD"
    eurcad = "EUR/CAD"
    audcad = "AUD/CAD"
    cadjpy = "CAD/JPY"
    eurnzd = "EUR/NZD"
    grxeur = "GRX/EUR"
    nzdcad = "NZD/CAD"
    sgdjpy = "SGD/JPY"
    usdhkd = "USD/HKD"
    usdnok = "USD/NOK"
    usdtry = "USD/TRY"
    xauaud = "XAU/AUD"
    audchf = "AUD/CHF"
    auxaud = "AUX/AUD"
    eurhuf = "EUR/HUF"
    eurpln = "EUR/PLN"
    frxeur = "FRX/EUR"
    hkxhkd = "HKX/HKD"
    nzdchf = "NZD/CHF"
    spxusd = "SPX/USD"
    usdhuf = "USD/HUF"
    usdpln = "USD/PLN"
    usdzar = "USD/ZAR"
    xauchf = "XAU/CHF"
    zarjpy = "ZAR/JPY"
    bcousd = "BCO/USD"
    etxeur = "ETX/EUR"
    eurczk = "EUR/CZK"
    eursek = "EUR/SEK"
    gbpaud = "GBP/AUD"
    gbpnzd = "GBP/NZD"
    jpxjpy = "JPX/JPY"
    udxusd = "UDX/USD"
    usdczk = "USD/CZK"
    usdsek = "USD/SEK"
    wtiusd = "WTI/USD"
    xaueur = "XAU/EUR"
    audnzd = "AUD/NZD"
    cadchf = "CAD/CHF"
    eurdkk = "EUR/DKK"
    eurnok = "EUR/NOK"
    eurtry = "EUR/TRY"
    gbpcad = "GBP/CAD"
    nsxusd = "NSX/USD"
    ukxgbp = "UKX/GBP"
    usddkk = "USD/DKK"
    usdsgd = "USD/SGD"
    xagusd = "XAG/USD"
    xaugbp = "XAU/GBP"

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}


class Format(Enum):
    MT = "metatrader"
    NT = "ninjatrader"
    MS = "metastock"
    ASCII = "ascii"
    XLSX = "excel"

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}


class Timeframe(Enum):
    M1 = "1-minute-bar-quotes"
    T = "tick-data-quotes"
    T_LAST = "tick-last-quotes"
    T_BID = "tick-bid-quotes"
    T_ASK = "tick-ask-quotes"

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}


class TimeFormat(Enum):
    MT_M1 = "%Y.%m.%d %H:%M"
    ASCII_M1 = "%Y%m%d %H%M%S"
    ASCII_T = "%Y%m%d %H%M%S%f"
    NT_M1 = "%Y%m%d %H%M%S"
    NT_T_LAST = "%Y%m%d %H%M%S"
    NT_T_BID = "%Y%m%d %H%M%S"
    NT_T_ASK = "%Y%m%d %H%M%S"
    MS_M1 = "%Y%m%d%H%M"

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}


class TimePrecision(Enum):
    MT_M1 = WritePrecision.S
    ASCII_M1 = WritePrecision.S
    ASCII_T = WritePrecision.MS
    NT_M1 = WritePrecision.S
    NT_T_LAST = WritePrecision.S
    NT_T_BID = WritePrecision.S
    NT_T_ASK = WritePrecision.S
    MS_M1 = WritePrecision.S

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}


def get_valid_format_timeframes(csv_format):
    timeframes = []

    match csv_format:
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
