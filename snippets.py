# import datatable as dt
# from datatable import f

# Try something like this
# DT = dt.Frame(["20220401 001612839"])
# print(DT)
#    | C0
#    | str32
# -- + ------------------
#  0 | 20220401 000012839

# year, month, day, hour

# DT = DT[:, dt.time.ymdt(f[:][0:4].as_type(int), \
#                         f[:][4:6].as_type(int), \
#                         f[:][6:8].as_type(int), \
#                         f[:][9:11].as_type(int), \
#                         f[:][11:13].as_type(int), \
#                         f[:][13:15].as_type(int), \
#                         10**6 * f[:][15:18].as_type(int))]

# # >    | C0
# # >    | time64
# # > -- + -----------------------
# # >  0 | 2022-04-01T00:00:12.839
# # > [1 row x 1 column]
# print(DT)
# DT = DT[:, (f[:].as_type(int)//10**6)]
# print(DT)
# DT = DT[:, f[:].as_type(dt.Type.time64)**6]
# print(DT)
# print(DT)
# >    |            C0
# >    |         int64
# > -- + -------------
# >  0 | 1648771212839
# > [1 row x 1 column]

# DT = dt.Frame(["20220401 000012839"])
# DT = DT[:, f[:][0:4]+"-"+f[:][4:6]+"-"+f[:][6:8]+" "+f[:][9:11]+":"+f[:][11:13]+":"+f[:][13:15]+"."+f[:][15:18]]
# DT[0] = dt.Type.time64
# print(DT[:, f[:].as_type(int)//10**6])
#    |            C0
#    |         int64
# -- + -------------
#  0 | 1648771212839
# [1 row x 1 column]

# histdatacom -I -p eurusd usdjpy gbpusd usdcad usdchf audusd nzdusd -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p eurgbp euraud gbpchf audnzd audcad audchf gbpaud usdmxn -f ascii -t tick-data-quotes -s start -e now -c low

# histdatacom -I -p eurchf eurcad eurnzd eurjpy gbpjpy chfjpy cadjpy -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p audjpy nzdjpy gbpcad nzdcad sgdjpy gbpnzd cadchf -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p eurtry usdtry usdsek usdnok usddkk usdzar usdhkd -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p usdsgd eurpln eurhuf nzdchf usdhuf usdpln eurczk -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p eursek usdczk zarjpy eurdkk eurnok usddkk-f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd -f ascii -t tick-data-quotes -s start -e now -c low
# histdatacom -I -p nsxusd ukxgbp etxeur -f ascii -t tick-data-quotes -s start -e now -c low

# histdata = {"eurusd",
# "eurchf",
# "eurgbp",
# "eurjpy",
# "euraud",
# "usdcad",
# "usdchf",
# "usdjpy",
# "usdmxn",
# "gbpchf",
# "gbpjpy",
# "gbpusd",
# "audjpy",
# "audusd",
# "chfjpy",
# "nzdjpy",
# "nzdusd",
# "xauusd",
# "eurcad",
# "audcad",
# "cadjpy",
# "eurnzd",
# "grxeur",
# "nzdcad",
# "sgdjpy",
# "usdhkd",
# "usdnok",
# "usdtry",
# "xauaud",
# "audchf",
# "auxaud",
# "eurhuf",
# "eurpln",
# "frxeur",
# "hkxhkd",
# "nzdchf",
# "spxusd",
# "usdhuf",
# "usdpln",
# "usdzar",
# "xauchf",
# "zarjpy",
# "bcousd",
# "etxeur",
# "eurczk",
# "eursek",
# "gbpaud",
# "gbpnzd",
# "jpxjpy",
# "udxusd",
# "usdczk",
# "usdsek",
# "wtiusd",
# "xaueur",
# "audnzd",
# "cadchf",
# "eurdkk",
# "eurnok",
# "eurtry",
# "gbpcad",
# "nsxusd",
# "ukxgbp",
# "usddkk",
# "usdsgd",
# "xagusd",
# "xaugbp"}





# histdata_and_oanda_intersect_symbs = histdata_symbs & oanda_symbs


import histdatacom
from histdatacom.options import Options
from histdatacom.fx_enums import Pairs

def import_pair_to_influx(pair, start, end):
    data_options = Options()

    data_options.import_to_influxdb = True  # implies validate, download, and extract
    data_options.delete_after_influx = True
    data_options.batch_size = "2000"
    data_options.cpu_utilization = "low"

    data_options.pairs = {f"{pair}"}# histdata_and_oanda_intersect_symbs
    data_options.start_yearmonth = f"{start}"
    data_options.end_yearmonth = f"{end}"
    data_options.formats = {"ascii"}  # Must be {"ascii"}
    data_options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or 1-minute-bar-quotes
    histdatacom(data_options)

def get_available_range_data(pairs):
    range_options = Options()
    range_options.pairs = pairs
    range_options.available_remote_data = True
    range_options.by = "start_dsc"
    range_data = histdatacom(range_options)  # (Jupyter)
    return range_data

def print_one_datatable_frame(pair, start=None, end=None):
    options = Options()
    options.api_return_type = "datatable"
    options.pairs = {f"{pair}"}
    options.start_yearmonth = "201501"
    options.formats = {"ascii"}
    options.timeframes = {"tick-data-quotes"}
    return histdatacom(options)

def main():
    histdata_symbs = Pairs.list_keys()
    
    # Oanda Symbols:
    oanda_symbs = {"audcad","audchf","audhkd","audjpy","audsgd","audusd","cadhkd","cadjpy","cadsgd",
    "chfhkd","chfjpy","euraud","eurcad","eurchf","eurgbp","eurhkd","eurjpy","eursgd","eurusd","gbpaud",
    "gbpcad","gbpchf","gbphkd","gbpjpy","gbpsgd","gbpusd","hkdjpy","sgdchf","sgdhkd","sgdjpy","usdcad",
    "usdchf","usdhkd","usdjpy","usdsgd","audnzd","cadchf","chfzar","eurczk","eurdkk","eurhuf","eurnok",
    "eurnzd","eurpln","eursek","eurtry","eurzar","gbpnzd","gbppln","gbpzar","nzdcad","nzdchf","nzdhkd",
    "nzdjpy","nzdsgd","nzdusd","tryjpy","usdcnh","usdczk","usddkk","usdhuf","usdmxn","usdnok","usdpln",
    "usdsar","usdsek","usdthb","usdtry","usdzar","zarjpy"}

    histdata_and_oanda_intersect_symbs = histdata_symbs & oanda_symbs

    pairs_data = get_available_range_data(histdata_and_oanda_intersect_symbs)
    for pair in pairs_data:
        start = pairs_data[pair]['start']
        end = pairs_data[pair]['end']
        
        print(pair, start, end)
        import_pair_to_influx(pair, start, end)

if __name__ == '__main__':
    main()
