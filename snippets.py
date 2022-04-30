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

# histdatacom -I -p usdjpy gbpusd usdcad usdchf audusd nzdusd -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p eurgbp euraud gbpchf audnzd audcad audchf gbpaud usdmxn -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p eurchf eurcad eurnzd eurjpy gbpjpy chfjpy cadjpy -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p audjpy nzdjpy gbpcad nzdcad sgdjpy gbpnzd cadchf -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p eurtry usdtry usdsek usdnok usddkk usdzar usdhkd -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p usdsgd eurpln eurhuf nzdchf usdhuf usdpln eurczk -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p eursek usdczk zarjpy eurdkk eurnok usddkk-f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p xauusd xauaud xauchf bcousd wtiusd xaueur xagusd xaugbp -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p grxeur auxaud frxeur hkxhkd spxusd jpxjpy udxusd -f ascii -t tick-data-quotes -s start -e now
# histdatacom -I -p nsxusd ukxgbp etxeur -f ascii -t tick-data-quotes -s start -e now

from math import ceil
import multiprocessing
import sys

def get_pool_cpu_count(count=None):

    try:
        real_vcpu_count = multiprocessing.cpu_count()

        if count is None:
            count = real_vcpu_count
        else:
            err_text_cpu_level_err = \
            f"""
                    ERROR on -c {count}  ERROR
                        * Malformed command:
                            - -c cpu must be str: low, medium, or high. or integer percent 1-200
            """
            count = str(count)
            match count:
                case "low":
                    count = ceil(real_vcpu_count / 2.5)
                case "medium":
                    count = ceil(real_vcpu_count / 1.5)
                case "high":
                    count = real_vcpu_count
                case _:
                    if count.isnumeric() and 1 <= int(count) <= 200:
                        count =  ceil(real_vcpu_count * (int(count) / 100))
                    else:
                        raise ValueError(err_text_cpu_level_err)

        return count - 1 if count > 2 else ceil(count / 2)
    except ValueError as err:
        print(err)
        sys.exit(err)


print("  call:", get_pool_cpu_count())
print("   low:", get_pool_cpu_count("low"))
print("medium:", get_pool_cpu_count("medium"))
print("  high:", get_pool_cpu_count("high"))
print("  percent   1:", get_pool_cpu_count("1"))
print("  percent  50:", get_pool_cpu_count("50"))
print("  percent 100:", get_pool_cpu_count(100))
print("  percent 150:", get_pool_cpu_count("150"))
print("  percent 200:", get_pool_cpu_count(200))