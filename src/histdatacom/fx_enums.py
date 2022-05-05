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

# metals-commodities
# xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp

# indices
# grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd
# nsxusd ukxgbp etxeur

class Pairs(Enum):
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
