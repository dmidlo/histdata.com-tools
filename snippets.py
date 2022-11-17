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

histdata_symbs = {"eurusd",
"eurchf",
"eurgbp",
"eurjpy",
"euraud",
"usdcad",
"usdchf",
"usdjpy",
"usdmxn",
"gbpchf",
"gbpjpy",
"gbpusd",
"audjpy",
"audusd",
"chfjpy",
"nzdjpy",
"nzdusd",
"xauusd",
"eurcad",
"audcad",
"cadjpy",
"eurnzd",
"grxeur",
"nzdcad",
"sgdjpy",
"usdhkd",
"usdnok",
"usdtry",
"xauaud",
"audchf",
"auxaud",
"eurhuf",
"eurpln",
"frxeur",
"hkxhkd",
"nzdchf",
"spxusd",
"usdhuf",
"usdpln",
"usdzar",
"xauchf",
"zarjpy",
"bcousd",
"etxeur",
"eurczk",
"eursek",
"gbpaud",
"gbpnzd",
"jpxjpy",
"udxusd",
"usdczk",
"usdsek",
"wtiusd",
"xaueur",
"audnzd",
"cadchf",
"eurdkk",
"eurnok",
"eurtry",
"gbpcad",
"nsxusd",
"ukxgbp",
"usddkk",
"usdsgd",
"xagusd",
"xaugbp"}



# Oanda Symbols:
oanda_symbs = {"audcad",
"audchf",
"audhkd",
"audjpy",
"audsgd",
"audusd",
"cadhkd",
"cadjpy",
"cadsgd",
"chfhkd",
"chfjpy",
"euraud",
"eurcad",
"eurchf",
"eurgbp",
"eurhkd",
"eurjpy",
"eursgd",
"eurusd",
"gbpaud",
"gbpcad",
"gbpchf",
"gbphkd",
"gbpjpy",
"gbpsgd",
"gbpusd",
"hkdjpy",
"sgdchf",
"sgdhkd",
"sgdjpy",
"usdcad",
"usdchf",
"usdhkd",
"usdjpy",
"usdsgd",
"audnzd",
"cadchf",
"chfzar",
"eurczk",
"eurdkk",
"eurhuf",
"eurnok",
"eurnzd",
"eurpln",
"eursek",
"eurtry",
"eurzar",
"gbpnzd",
"gbppln",
"gbpzar",
"nzdcad",
"nzdchf",
"nzdhkd",
"nzdjpy",
"nzdsgd",
"nzdusd",
"tryjpy",
"usdcnh",
"usdczk",
"usddkk",
"usdhuf",
"usdmxn",
"usdnok",
"usdpln",
"usdsar",
"usdsek",
"usdthb",
"usdtry",
"usdzar",
"zarjpy"}

histdata_and_oanda_intersect_symbs = histdata_symbs & oanda_symbs


import histdatacom
from histdatacom.options import Options
from rich import print
options = Options()


# options.validate_urls = True
# options.download_data_archives = True  # implies validate
# options.extract_csvs = True  # implies validate and download
options.import_to_influxdb = True  # implies validate, download, and extract

# options.api_return_type = "datatable"  # "datatable", "pandas", or "arrow"

options.formats = {"ascii"}  # Must be {"ascii"}
options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or 1-minute-bar-quotes
options.pairs = histdata_and_oanda_intersect_symbs # {"eurusd"}
options.start_yearmonth = "2001-01"
options.end_yearmonth = "2005-12"
# options.start_yearmonth = "2005-01"
# options.end_yearmonth = "2006-12"
options.cpu_utilization = "high"


def main():
    data = histdatacom(options)  # (Jupyter)


    print(data)

    # list
    # pandas.core.frame.DataFrame
    # datatable.Frame
    # pyarrow.lib.Table



if __name__ == '__main__':
    main()
