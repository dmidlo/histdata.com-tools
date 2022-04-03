

def test(row):
    
    return row["Instrument"] + \
        ",source=" + row["Source"] + \
        ",platform=" + row["Platform"] + \
        ",timeframe=" + row["Timeframe"] + " " + \
        "bidquote=" + str(row["bidQuote"]) + \
        ",askquote=" + str(row["askQuote"]) + \
        ",volume=" + str(row["Volume"]) + " " + \
        str(row["msSinceEpochUTC"])


# Source,Platform,Timeframe,Instrument,msSinceEpochUTC,bidQuote,askQuote,Volume

test_row = {
 "Source": "histdata.com",
 "Platform": "ASCII",
 "Timeframe": "T",
 "Instrument": "EURUSD",
 "msSinceEpochUTC": "1080795604000",
 "bidQuote": "1.228000",
 "askQuote": "1.227600",
 "Volume": "0"
}

print(test(test_row))