# > Try something like this
# > ```python
# > >>> DT = dt.Frame(["20220401 000012839"])
# > >>> DT
# >    | C0                
# >    | str32             
# > -- + ------------------
# >  0 | 20220401 000012839
# > 
# > >>> DT = DT[:, dt.time.ymdt(f[:][0:4].as_type(int), f[:][4:6].as_type(int), f[:][6:8].as_type(int), f[:][9:11].as_type(int), f[:][11:13].as_type(int), f[:][13:15].as_type(int), 10**6 * f[:][15:18].as_type(int))]
# > >>> DT
# >    | C0                     
# >    | time64                 
# > -- + -----------------------
# >  0 | 2022-04-01T00:00:12.839
# > [1 row x 1 column]
# > 
# > >>> DT = DT[:, f[:].as_type(int)//10**6]
# > >>> DT
# >    |            C0
# >    |         int64
# > -- + -------------
# >  0 | 1648771212839
# > [1 row x 1 column]
# > ```

# >>> DT = dt.Frame(["20220401 000012839"])
# >>> DT = DT[:, f[:][0:4]+"-"+f[:][4:6]+"-"+f[:][6:8]+" "+f[:][9:11]+":"+f[:][11:13]+":"+f[:][13:15]+"."+f[:][15:18]]
# >>> DT[0] = dt.Type.time64
# >>> DT[:, f[:].as_type(int)//10**6]
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

def get_pool_cpu_count(count=None):

    real_vcpu_count = multiprocessing.cpu_count()

    if count is None:
        count = real_vcpu_count
    else:
        match count:
            case "low":
                count = ceil(real_vcpu_count / 2.5)
            case "medium":
                count = ceil(real_vcpu_count / 1.5)
            case "high":
                count = real_vcpu_count
            case _:
                raise ValueError("\n -c cpu must be str: low, medium, or high. \n")

    return count - 1 if count > 2 else ceil(count / 2)

print(get_pool_cpu_count())
print(get_pool_cpu_count("low"))
print(get_pool_cpu_count("medium"))
print(get_pool_cpu_count("high"))