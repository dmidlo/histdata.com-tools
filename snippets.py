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

# histdatacom -I -p eurusd -f ascii -tick-data-quotes -s now -c 100
# histdatacom -I -p usdjpy gbpusd usdcad usdchf audusd nzdusd -f ascii -t tick-data-quotes -s start -e now

# histdatacom -I -p eurgbp euraud gbpchf audnzd -f ascii -t tick-data-quotes -s start -e now -c 80
# histdatacom -I -p audcad audchf gbpaud usdmxn -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p eurchf eurcad eurnzd eurjpy gbpjpy chfjpy cadjpy -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p audjpy nzdjpy gbpcad nzdcad sgdjpy gbpnzd cadchf -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p eurtry usdtry usdsek usdnok usddkk usdzar usdhkd -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p usdsgd eurpln eurhuf nzdchf usdhuf usdpln eurczk -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p eursek usdczk zarjpy eurdkk eurnok usddkk-f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd -f ascii -t tick-data-quotes -s start -e now -c medium
# histdatacom -I -p nsxusd ukxgbp etxeur -f ascii -t tick-data-quotes -s start -e now -c medium

