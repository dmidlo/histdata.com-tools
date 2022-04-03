from fx_enums import Pairs

def test_line_protocol(row):
    
    return row["Instrument"] + \
        ",source=" + row["Source"] + \
        ",platform=" + row["Platform"] + \
        ",timeframe=" + row["Timeframe"] + " " + \
        "bidquote=" + str(row["bidQuote"]) + \
        ",askquote=" + str(row["askQuote"]) + \
        ",volume=" + str(row["Volume"]) + " " + \
        str(row["msSinceEpochUTC"])


# # Source,Platform,Timeframe,Instrument,msSinceEpochUTC,bidQuote,askQuote,Volume

# test_row = {
#  "Source": "histdata.com",
#  "Platform": "ASCII",
#  "Timeframe": "T",
#  "Instrument": "EURUSD",
#  "msSinceEpochUTC": "1080795604000",
#  "bidQuote": "1.228000",
#  "askQuote": "1.227600",
#  "Volume": "0"
# }

# print(test_line_protocol(test_row))

def test_count_pairs():
    print(len(Pairs))

test_count_pairs()

