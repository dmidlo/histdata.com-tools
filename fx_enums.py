from enum import Enum
from influxdb_client import WritePrecision

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
    uernzd = "EUR/NZD"
    grxeur = "GRX/EUR"
    nzdcad = "NZD/CAD"
    sgdspy = "SGD/JPY"
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
    nadchf = "NZD/CHF"
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

class Platform(Enum):
    MT = "metatrader"
    XLSX = "excel"
    NT = "ninjatrader"
    MS = "metastock"
    ASCII = "ascii"

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
    XLSX_M1 = "%Y%m%d %H%M"
    NT_M1 = "%Y%m%d %H%M%S"
    NT_TL = "%Y%m%d %H%M%S"
    NT_TB = "%Y%m%d %H%M%S"
    NT_TA = "%Y%m%d %H%M%S"
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
    XLSX_M1 = WritePrecision.S
    NT_M1 = WritePrecision.S
    NT_TL = WritePrecision.S
    NT_TB = WritePrecision.S
    NT_TA = WritePrecision.S
    MS_M1 = WritePrecision.S

    @classmethod
    def list_keys(cls):
        return set(cls.__members__.keys())

    @classmethod
    def list_values(cls):
        return {member.value for _, member in cls.__members__.items()}